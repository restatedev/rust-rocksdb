use std::path::Path;
use std::{env, fs, path::PathBuf, process::Command};

#[cfg(target_os = "linux")]
use libc::{AT_HWCAP, getauxval};

// On these platforms jemalloc-sys will use a prefixed jemalloc which cannot be linked together
// with RocksDB.
// See https://github.com/tikv/jemallocator/blob/f7adfca5aff272b43fd3ad896252b57fbbd9c72a/jemalloc-sys/src/env.rs#L24
const NO_JEMALLOC_TARGETS: &[&str] = &["android", "dragonfly", "darwin"];

fn link(name: &str, bundled: bool) {
    use std::env::var;
    let target = var("TARGET").unwrap();
    let target: Vec<_> = target.split('-').collect();
    if target.get(2) == Some(&"windows") {
        println!("cargo:rustc-link-lib=dylib={name}");
        if bundled && target.get(3) == Some(&"gnu") {
            let dir = var("CARGO_MANIFEST_DIR").unwrap();
            println!("cargo:rustc-link-search=native={}/{}", dir, target[0]);
        }
    }
}

fn fail_on_empty_directory(name: &str) {
    if fs::read_dir(name).unwrap().count() == 0 {
        println!("The `{name}` directory is empty, did you forget to pull the submodules?");
        println!("Try `git submodule update --init --recursive`");
        panic!();
    }
}

fn rocksdb_include_dir() -> String {
    match env::var("ROCKSDB_INCLUDE_DIR") {
        Ok(val) => val,
        Err(_) => "rocksdb/include".to_string(),
    }
}

fn bindgen_rocksdb() {
    let bindings = bindgen::Builder::default()
        .header(rocksdb_include_dir() + "/rocksdb/c.h")
        .header(rocksdb_include_dir() + "/rocksdb/restate.h")
        .clang_arg(format!("-I{}", rocksdb_include_dir()))
        .derive_debug(false)
        .blocklist_type("max_align_t") // https://github.com/rust-lang-nursery/rust-bindgen/issues/550
        .ctypes_prefix("libc")
        .size_t_is_usize(true)
        .generate()
        .expect("unable to generate rocksdb bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("unable to write rocksdb bindings");
}

#[cfg(target_os = "linux")]
fn check_getauxval_supported() -> bool {
    unsafe {
        let aux_value = getauxval(AT_HWCAP);
        if aux_value == 0 {
            return false;
        }

        true
    }
}

/// Splits `CARGO_ENCODED_RUSTFLAGS` into a Vec.
fn split_encoded_rustflags() -> Vec<String> {
    let flags = std::env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default();

    // extra flags that Cargo invokes rustc with, separated by a 0x1f character
    // https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-sets-for-crates
    flags.split("\x1f").map(|flag| flag.to_string()).collect()
}

/// Returns the argument to `-Ctarget-cpu=` if it exists.
fn get_target_cpu_flag() -> Option<String> {
    const TARGET_CPU_FLAG: &str = "-Ctarget-cpu=";
    let flags = split_encoded_rustflags();
    let complete_flag = flags.iter().find(|flag| flag.starts_with(TARGET_CPU_FLAG));
    complete_flag.map(|flag| flag[TARGET_CPU_FLAG.len()..].to_string())
}

/// If the Rust `-Ctarget-cpu=` option is set, this attempts to pass it through to the C/C++
/// compiler. It should print a Cargo build warning if the compiler does not support the flag,
/// or if the architecture is not supported.
fn pass_through_target_cpu(cfg: &mut cc::Build) {
    let Some(target_cpu_flag) = get_target_cpu_flag() else {
        return;
    };

    let arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    match arch.as_str() {
        "x86_64" => {
            cfg.flag_if_supported(format!("-march={target_cpu_flag}"));
        }
        "aarch64" => {
            cfg.flag_if_supported(format!("-mcpu={target_cpu_flag}"));
        }
        // TODO: add more architectures/compilers
        _ => {
            println!(
                "cargo::warning=unknown target architecture: {arch}; C/C++ target flags not passed through"
            );
        }
    }
}

#[cfg(feature = "folly")]
fn folly_dep_dir(install_root: &Path, prefix: &str) -> Option<PathBuf> {
    let pattern = format!("{}/{prefix}-*", install_root.display());
    glob::glob(&pattern)
        .ok()?
        .flatten()
        .find(|p| p.is_dir())
}

#[cfg(feature = "folly")]
fn getdeps_show_inst_dir() -> Option<PathBuf> {
    let folly_src = Path::new("rocksdb/third-party/folly");
    if !folly_src.join("build/fbcode_builder/getdeps.py").exists() {
        return None;
    }
    let python = env::var("PYTHON").unwrap_or_else(|_| "python3".to_string());
    let out = Command::new(&python)
        .current_dir(folly_src)
        .args(["build/fbcode_builder/getdeps.py", "show-inst-dir"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let path = String::from_utf8(out.stdout).ok()?.trim().to_string();
    if path.is_empty() {
        return None;
    }
    let p = PathBuf::from(path);
    if p.is_dir() { Some(p) } else { None }
}

#[cfg(feature = "folly")]
fn resolve_folly_path() -> Option<PathBuf> {
    if let Ok(p) = env::var("FOLLY_PATH") {
        let p = PathBuf::from(p);
        if p.is_dir() {
            return Some(p);
        }
        panic!("FOLLY_PATH={} does not exist or is not a directory", p.display());
    }
    // getdeps installs to a deterministic scratch dir derived from the
    // absolute path of the folly source checkout (typically under
    // /tmp/ or /private/var/folders/.../ on macOS). Ask it directly.
    if let Some(p) = getdeps_show_inst_dir() {
        return Some(p);
    }
    // Fallback: in-tree install (older getdeps versions, or if the scratch
    // dir was overridden via --scratch-path during build_folly).
    folly_dep_dir(Path::new("rocksdb/third-party/folly/_build/installed"), "folly")
}

#[cfg(feature = "folly")]
fn lib_subdir(dep_dir: &Path) -> PathBuf {
    let lib64 = dep_dir.join("lib64");
    if lib64.is_dir() { lib64 } else { dep_dir.join("lib") }
}

/// folly's boost manifest pulls `--with-python`, `--with-mpi`, and
/// `--with-graph_parallel`. libfolly.a doesn't link any of those, but the b2
/// build still compiles them, which can fail on hosts whose default python is
/// too new for boost 1.83's numpy support (e.g., Fedora 43 ships python 3.14;
/// boost 1.83's `libs/python/src/numpy/dtype.cpp` references the removed
/// `PyArray_Descr::elsize` field). Strip those args before invoking the build.
#[cfg(feature = "folly")]
fn patch_boost_manifest(rocksdb_dir: &Path) {
    let manifest = rocksdb_dir.join("third-party/folly/build/fbcode_builder/manifests/boost");
    let Ok(contents) = std::fs::read_to_string(&manifest) else {
        return;
    };
    let drop = ["--with-python", "--with-mpi", "--with-graph_parallel"];
    let filtered: String = contents
        .lines()
        .filter(|line| !drop.contains(&line.trim()))
        .map(|line| format!("{line}\n"))
        .collect();
    if filtered != contents {
        let _ = std::fs::write(&manifest, filtered);
    }
}

#[cfg(feature = "folly")]
fn auto_build_folly() -> Option<PathBuf> {
    let make_dir = Path::new("rocksdb");
    if !make_dir.join("Makefile").exists() {
        println!(
            "cargo:warning=cannot auto-build folly: rocksdb submodule not initialized at {}",
            make_dir.display()
        );
        return None;
    }

    println!(
        "cargo:warning=building folly + transitive deps via `make build_folly`; \
         first run can take 20-30 minutes and requires network access. \
         Subsequent builds reuse the install. Re-run cargo with `-vv` to stream the \
         underlying make output. Set FOLLY_PATH to skip and use a prebuilt install."
    );

    let checkout = Command::new("make")
        .current_dir(make_dir)
        .arg("checkout_folly")
        .status();
    match checkout {
        Ok(s) if s.success() => {}
        Ok(s) => {
            println!("cargo:warning=`make checkout_folly` exited with status {s}");
            return None;
        }
        Err(e) => {
            println!("cargo:warning=failed to invoke `make checkout_folly`: {e}");
            return None;
        }
    }

    patch_boost_manifest(make_dir);

    let build = Command::new("make")
        .current_dir(make_dir)
        .arg("build_folly")
        // Build folly in release mode unconditionally. The upstream Makefile
        // notes an ODR risk if folly and RocksDB disagree on debug/release,
        // but switching modes here would force a full ~30min rebuild on every
        // cargo profile change. Document the caveat instead.
        .env("DEBUG_LEVEL", "0")
        .status();
    match build {
        // The recipe's final step is a `patchelf` invocation which is only
        // present on Linux — on other platforms `make build_folly` may report
        // failure even though the install itself is complete. Fall through and
        // re-probe the install dir.
        Ok(_) => {}
        Err(e) => {
            println!("cargo:warning=failed to invoke `make build_folly`: {e}");
            return None;
        }
    }

    resolve_folly_path()
}

/// On macOS, getdeps builds glog and gflags as dylibs with install_names like
/// `@rpath/libglog.0.dylib`. cargo's `rustc-link-arg` doesn't propagate from a
/// sys crate to downstream binary links, so any LC_RPATH we emit here is
/// dropped — the final binary records `@rpath/...` but has no rpath to resolve
/// it. Rewrite the install_names to absolute paths so downstream linkers
/// record absolute paths directly and skip rpath resolution at runtime.
#[cfg(feature = "folly")]
fn rewrite_macos_install_names(dep_dir: &Path) {
    let lib_dir = lib_subdir(dep_dir);
    let Ok(entries) = std::fs::read_dir(&lib_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        // Only touch real files, not the symlinks getdeps creates for sonames.
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if !meta.file_type().is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if !name.contains(".dylib") {
            continue;
        }
        let Ok(abs) = path.canonicalize() else { continue };
        let Some(abs_str) = abs.to_str() else { continue };

        // Set the dylib's own install_name to its absolute path.
        let _ = Command::new("install_name_tool")
            .args(["-id", abs_str, abs_str])
            .status();

        // Rewrite any @rpath/<libname>.dylib references in the dylib's
        // dependencies to absolute paths under the parent install dir.
        let install_root = match abs.parent().and_then(|p| p.parent()).and_then(|p| p.parent()) {
            Some(p) => p.to_path_buf(),
            None => continue,
        };
        let Ok(out) = Command::new("otool").arg("-L").arg(abs_str).output() else {
            continue;
        };
        let listing = String::from_utf8_lossy(&out.stdout);
        for line in listing.lines() {
            let trimmed = line.trim();
            if let Some(suffix) = trimmed.strip_prefix("@rpath/") {
                let lib_name = match suffix.split_whitespace().next() {
                    Some(s) => s,
                    None => continue,
                };
                if let Some(target_path) = find_rpath_target(&install_root, lib_name) {
                    let _ = Command::new("install_name_tool")
                        .args(["-change", &format!("@rpath/{lib_name}"), &target_path])
                        .arg(abs_str)
                        .status();
                }
            }
        }
    }
}

#[cfg(feature = "folly")]
fn find_rpath_target(install_root: &Path, lib_name: &str) -> Option<String> {
    let entries = std::fs::read_dir(install_root).ok()?;
    for entry in entries.flatten() {
        let dep_lib_dir = lib_subdir(&entry.path());
        let candidate = dep_lib_dir.join(lib_name);
        if candidate.exists() {
            return candidate.canonicalize().ok()?.to_str().map(str::to_string);
        }
    }
    None
}

#[cfg(feature = "folly")]
fn setup_folly(config: &mut cc::Build, target: &str) {
    println!("cargo:rerun-if-env-changed=FOLLY_PATH");

    let folly_path = resolve_folly_path()
        .or_else(auto_build_folly)
        .unwrap_or_else(|| {
            panic!(
                "the `folly` feature is enabled but no folly install was found \
                 and auto-build failed. Either set `FOLLY_PATH` to a prebuilt \
                 folly install directory, or fix the build environment (needs \
                 make, python3, cmake, and network access for the initial fetch)."
            )
        });

    println!("cargo:rerun-if-changed={}", folly_path.display());

    // getdeps may resolve some folly deps to source-built sibling dirs
    // (`<dep>-<hash>/` next to the folly install) and others to system
    // packages (via `--allow-system-packages`). Per-dep fallback: if the
    // sibling exists, use it (header/include + static link); otherwise
    // assume the distro provides it under /usr/include and /usr/lib.
    let install_root = folly_path
        .parent()
        .expect("folly install dir has a parent")
        .to_path_buf();
    let dep_names = [
        "boost",
        "double-conversion",
        "gflags",
        "glog",
        "libevent",
        "libsodium",
        "fmt",
    ];
    let mut deps: std::collections::HashMap<&str, PathBuf> = std::collections::HashMap::new();
    for name in dep_names {
        if let Some(dir) = folly_dep_dir(&install_root, name) {
            deps.insert(name, dir);
        }
    }

    if target.contains("darwin") {
        if let Some(d) = deps.get("glog") {
            rewrite_macos_install_names(d);
        }
        if let Some(d) = deps.get("gflags") {
            rewrite_macos_install_names(d);
        }
    }

    // Folly's own headers (always under FOLLY_PATH/include).
    let folly_inc = folly_path.join("include");
    if folly_inc.is_dir() {
        config.flag("-isystem").flag(folly_inc.to_str().unwrap());
    }
    // Add -isystem for source-built deps; system deps live on the compiler's
    // default include path.
    for name in dep_names {
        if let Some(dir) = deps.get(name) {
            let inc = dir.join("include");
            if inc.is_dir() {
                config.flag("-isystem").flag(inc.to_str().unwrap());
            }
        }
    }

    config.define("USE_FOLLY", None);
    config.define("FOLLY_NO_CONFIG", None);
    config.define("HAVE_CXX11_ATOMIC", None);
    // libfolly.a was compiled with weak-symbol support enabled (gcc/clang
    // on linux both support it). `FOLLY_NO_CONFIG` bypasses folly-config.h
    // so we have to mirror the build-time value here, otherwise folly's
    // `MallocImpl.h` emits neither the weak-function declarations nor the
    // function-pointer fallback (the latter is gated on `!USE_JEMALLOC`),
    // leaving callers like `Malloc.h::sizedAlignedFree` referencing an
    // undeclared `free_aligned_sized`.
    config.define("FOLLY_HAVE_WEAK_SYMBOLS", "1");

    if target.contains("darwin") {
        // On macOS, folly's portability/Time.h tries to ship a `clockid_t`
        // typedef shim unless __CLOCK_AVAILABILITY is visible at parse time.
        // Recent macOS SDKs always have clock_gettime + a real clockid_t enum,
        // so force-disable the shim to avoid a typedef redefinition error.
        config.define("FOLLY_HAVE_CLOCK_GETTIME", "1");
    }

    #[cfg(feature = "folly-coroutines")]
    {
        config.define("USE_COROUTINES", None);
        // USE_COROUTINES forces RTTI on in the upstream Makefile.
        config.define("ROCKSDB_USE_RTTI", None);
        let compiler = config.get_compiler();
        if !compiler.is_like_clang() {
            config.flag("-fcoroutines");
        }
        config.flag_if_supported("-Wno-deprecated");
        config.flag_if_supported("-Wno-redundant-move");
        config.flag_if_supported("-Wno-invalid-memory-model");
        config.flag_if_supported("-Wno-maybe-uninitialized");
    }

    // Link search paths — folly itself always; per-dep search paths only for
    // source-built siblings (system deps live on the default linker path).
    println!(
        "cargo:rustc-link-search=native={}",
        folly_path.join("lib").display()
    );
    for name in ["boost", "double-conversion", "libevent", "libsodium"] {
        if let Some(dir) = deps.get(name) {
            let lib = dir.join("lib");
            if lib.is_dir() {
                println!("cargo:rustc-link-search=native={}", lib.display());
            }
        }
    }
    for name in ["fmt", "glog", "gflags"] {
        if let Some(dir) = deps.get(name) {
            println!(
                "cargo:rustc-link-search=native={}",
                lib_subdir(dir).display()
            );
        }
    }

    // Folly linked statically. Each transitive dep: static when getdeps
    // source-built it (consistent ABI), dylib when it falls back to the
    // distro-shipped shared lib.
    println!("cargo:rustc-link-lib=static=folly");
    let kind_for = |name: &str| if deps.contains_key(name) { "static" } else { "dylib" };
    let boost_kind = kind_for("boost");
    for boost_lib in [
        "boost_context",
        "boost_filesystem",
        "boost_atomic",
        "boost_program_options",
        "boost_regex",
        "boost_system",
        "boost_thread",
    ] {
        println!("cargo:rustc-link-lib={boost_kind}={boost_lib}");
    }
    println!("cargo:rustc-link-lib={}=double-conversion", kind_for("double-conversion"));
    println!("cargo:rustc-link-lib={}=event", kind_for("libevent"));
    println!("cargo:rustc-link-lib={}=sodium", kind_for("libsodium"));
    println!("cargo:rustc-link-lib={}=fmt", kind_for("fmt"));

    // glog and gflags are typically dylibs in both flavours: getdeps doesn't
    // build static variants, and distros ship .so files.
    println!("cargo:rustc-link-lib=dylib=glog");
    println!("cargo:rustc-link-lib=dylib=gflags");

    // Source-built glog/gflags need rpath so the binary loader can find them
    // outside default search paths.
    if let Some(d) = deps.get("glog") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_subdir(d).display());
    }
    if let Some(d) = deps.get("gflags") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_subdir(d).display());
    }

    if target.contains("linux") {
        // GNU ld only: pull in indirect symbol deps (e.g. glog → gflags).
        println!("cargo:rustc-link-arg=-Wl,--copy-dt-needed-entries");
        println!("cargo:rustc-link-lib=dylib=dl");
    }
}

fn build_rocksdb() {
    // https://doc.rust-lang.org/cargo/reference/environment-variables.html
    let target = env::var("TARGET").unwrap();
    // https://doc.rust-lang.org/reference/conditional-compilation.html#target_arch
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    let target_features_env = env::var("CARGO_CFG_TARGET_FEATURE").unwrap_or_default();
    let target_features: Vec<_> = target_features_env.split(',').collect();

    let mut config = cc::Build::new();
    config.include("rocksdb/include/");
    config.include("rocksdb/");
    config.include("rocksdb/third-party/gtest-1.8.1/fused-src/");

    if cfg!(feature = "snappy") {
        config.define("SNAPPY", Some("1"));
        config.include("snappy/");
    }

    if cfg!(feature = "lz4") {
        config.define("LZ4", Some("1"));
        if let Some(path) = env::var_os("DEP_LZ4_INCLUDE") {
            config.include(path);
        }
    }

    if cfg!(feature = "zstd") {
        config.define("ZSTD", Some("1"));
        if let Some(path) = env::var_os("DEP_ZSTD_INCLUDE") {
            config.include(path);
        }

        if cfg!(feature = "zstd-static-linking-only") {
            config.define("ZSTD_STATIC_LINKING_ONLY", Some("1"));
        }
    }

    if cfg!(feature = "zlib") {
        config.define("ZLIB", Some("1"));
        if let Some(path) = env::var_os("DEP_Z_INCLUDE") {
            config.include(path);
        }
    }

    if cfg!(feature = "bzip2") {
        config.define("BZIP2", Some("1"));
        if let Some(path) = env::var_os("DEP_BZIP2_INCLUDE") {
            config.include(path);
        }
    }

    if cfg!(feature = "rtti") {
        config.define("USE_RTTI", Some("1"));
    }

    #[cfg(feature = "malloc-usable-size")]
    if target.contains("linux") {
        config.define("ROCKSDB_MALLOC_USABLE_SIZE", Some("1"));
    }

    // https://github.com/facebook/rocksdb/blob/be7703b27d9b3ac458641aaadf27042d86f6869c/Makefile#L195
    if cfg!(feature = "lto") {
        config.flag("-flto");
        if !config.get_compiler().is_like_clang() {
            panic!(
                "LTO is only supported with clang. Either disable the `lto` feature\
             or set `CC=/usr/bin/clang CXX=/usr/bin/clang++` environment variables."
            );
        }
    }

    config.include(".");
    config.define("NDEBUG", Some("1"));

    // true for C++ >= 17; we set -std=c++20 below
    config.define("HAVE_ALIGNED_NEW", None);

    // __uint128_t is supported by GCC and Clang; Don't use it for MSVC
    // TODO: implement a detection script?
    if !target.contains("msvc") {
        config.define("HAVE_UINT128_EXTENSION", None);
    }

    let mut lib_sources = include_str!("rocksdb_lib_sources.txt")
        .trim()
        .split('\n')
        .map(str::trim)
        // We have a pre-generated a version of build_version.cc in the local directory
        .filter(|file| !matches!(*file, "util/build_version.cc"))
        .collect::<Vec<&'static str>>();

    // attempt to pass through the RUSTFLAGS -Ctarget-cpu to allow the same optimizations for C/C++
    pass_through_target_cpu(&mut config);

    // CPU-specific build configuration
    if target_arch == "x86_64" {
        // This is needed to enable hardware CRC32C. Technically, SSE 4.2 is
        // only available since Intel Nehalem (about 2010) and AMD Bulldozer
        // (about 2011).
        if target_features.contains(&"sse2") {
            config.flag_if_supported("-msse2");
        }
        if target_features.contains(&"sse4.1") {
            config.flag_if_supported("-msse4.1");
        }
        if target_features.contains(&"sse4.2") {
            config.flag_if_supported("-msse4.2");
        } else {
            println!(
                r#"cargo::warning=compiling without SSE4.2: CRC will be slow (set RUSTFLAGS="-Ctarget-cpu=..." to optimize RocksDB e.g. -Ctarget-cpu=broadwell)"#
            );
        }
        // Pass along additional target features as defined in
        // build_tools/build_detect_platform.
        if target_features.contains(&"avx2") {
            config.flag_if_supported("-mavx2");
        }
        if target_features.contains(&"bmi1") {
            config.flag_if_supported("-mbmi");
        }
        if target_features.contains(&"lzcnt") {
            config.flag_if_supported("-mlzcnt");
        }

        if !target.contains("android") && target_features.contains(&"pclmulqdq") {
            config.flag_if_supported("-mpclmul");
        }

        if target_features.contains(&"avx") && !target_features.contains(&"pclmulqdq") {
            // RocksDB BUG (<= 10.11.0/2026-01-23): assumes AVX implies -mpclmul
            // x86-64-v3/-v4 does not include PCLMUL
            println!(
                r#"cargo:warning=RocksDB BUG: target arch missing -mpclmul; compile may fail: pass named architecture e.g. -Ctarget-cpu=broadwell"#
            );
        }
    } else if target_arch == "aarch64" {
        if target_features.contains(&"crc") && target_features.contains(&"aes") {
            // the target supports the instructions RocksDB needs: if we don't have a target-cpu,
            // use -march=armv8-a+crc+aes+crypto, like the RocksDB Makefile.
            // If we DO have a target-cpu, assume pass_through_target_cpu() has set it above
            if get_target_cpu_flag().is_none() {
                // TODO: Should just be +crc+aes but RocksDB checks for __ARM_FEATURE_CRYPTO
                // https://github.com/facebook/rocksdb/pull/14217
                config.flag_if_supported("-march=armv8-a+crc+aes+crypto");
            }
        } else {
            println!(
                r#"cargo:warning=building for aarch64 WITHOUT CRC instruction: build with RUSTFLAGS="-Ctarget-cpu=..." to optimize RocksDB e.g. -Ctarget-cpu=neoverse-n1"#
            );
        }
    }

    if target.contains("apple-ios") {
        config.define("OS_MACOSX", None);

        config.define("IOS_CROSS_COMPILE", None);
        config.define("PLATFORM", "IOS");
        config.define("NIOSTATS_CONTEXT", None);
        config.define("NPERF_CONTEXT", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);

        // SAFETY: This is the build script, which is single-threaded and runs
        // before any other code. Setting environment variables here is safe.
        unsafe { env::set_var("IPHONEOS_DEPLOYMENT_TARGET", "12.0") };
    } else if target.contains("darwin") {
        config.define("OS_MACOSX", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);
    } else if target.contains("android") {
        config.define("OS_ANDROID", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);

        if &target == "armv7-linux-androideabi" {
            config.define("_FILE_OFFSET_BITS", Some("32"));
        }
    } else if target.contains("aix") {
        config.define("OS_AIX", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);
    } else if target.contains("linux") {
        config.define("OS_LINUX", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);
        config.define("ROCKSDB_SCHED_GETCPU_PRESENT", None);

        #[cfg(target_os = "linux")]
        if check_getauxval_supported() {
            config.define("ROCKSDB_AUXV_GETAUXVAL_PRESENT", None);
        }
        config.define("ROCKSDB_FALLOCATE_PRESENT", None);
        config.define("ROCKSDB_RANGESYNC_PRESENT", None);
    } else if target.contains("dragonfly") {
        config.define("OS_DRAGONFLYBSD", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);
    } else if target.contains("freebsd") {
        config.define("OS_FREEBSD", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);
    } else if target.contains("netbsd") {
        config.define("OS_NETBSD", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);
    } else if target.contains("openbsd") {
        config.define("OS_OPENBSD", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);
    } else if target.contains("windows") {
        link("rpcrt4", false);
        link("shlwapi", false);
        config.define("DWIN32", None);
        config.define("OS_WIN", None);
        config.define("_MBCS", None);
        config.define("WIN64", None);
        config.define("NOMINMAX", None);
        config.define("ROCKSDB_WINDOWS_UTF8_FILENAMES", None);

        if &target == "x86_64-pc-windows-gnu" {
            // Tell MinGW to create localtime_r wrapper of localtime_s function.
            config.define("_POSIX_C_SOURCE", Some("1"));
            // Tell MinGW to use at least Windows Vista headers instead of the ones of Windows XP.
            // (This is minimum supported version of rocksdb)
            config.define("_WIN32_WINNT", Some("_WIN32_WINNT_VISTA"));
        }

        // Remove POSIX-specific sources
        lib_sources = lib_sources
            .iter()
            .cloned()
            .filter(|file| {
                !matches!(
                    *file,
                    "port/port_posix.cc"
                        | "env/env_posix.cc"
                        | "env/fs_posix.cc"
                        | "env/io_posix.cc"
                )
            })
            .collect::<Vec<&'static str>>();

        // Add Windows-specific sources
        lib_sources.extend([
            "port/win/env_default.cc",
            "port/win/port_win.cc",
            "port/win/xpress_win.cc",
            "port/win/io_win.cc",
            "port/win/win_thread.cc",
            "port/win/env_win.cc",
            "port/win/win_logger.cc",
        ]);

        if cfg!(feature = "jemalloc") {
            lib_sources.push("port/win/win_jemalloc.cc");
        }
    }

    if cfg!(feature = "jemalloc") && NO_JEMALLOC_TARGETS.iter().all(|i| !target.contains(i)) {
        config.define("ROCKSDB_JEMALLOC", Some("1"));
        config.define("JEMALLOC_NO_DEMANGLE", Some("1"));
        // When folly is also enabled, both `folly/memory/Malloc.h` and
        // jemalloc's own `jemalloc.h` end up in the same translation unit.
        // Folly's header redefines `MALLOCX_*` macros and declares `mallocx`
        // etc. as function pointers; jemalloc.h declares them as real
        // functions. `USE_JEMALLOC` makes folly's `Malloc.h` defer to
        // jemalloc.h instead of providing its own shims.
        if cfg!(feature = "folly") {
            config.define("USE_JEMALLOC", None);
        }
        if let Some(jemalloc_root) = env::var_os("DEP_JEMALLOC_ROOT") {
            config.include(Path::new(&jemalloc_root).join("include"));
        }
    }

    #[cfg(feature = "io-uring")]
    if target.contains("linux") {
        pkg_config::probe_library("liburing")
            .expect("The io-uring feature was requested but the library is not available");
        config.define("ROCKSDB_IOURING_PRESENT", Some("1"));
    }

    #[cfg(feature = "folly")]
    setup_folly(&mut config, &target);

    if &target != "armv7-linux-androideabi"
        && env::var("CARGO_CFG_TARGET_POINTER_WIDTH").unwrap() != "64"
    {
        config.define("_FILE_OFFSET_BITS", Some("64"));
        config.define("_LARGEFILE64_SOURCE", Some("1"));
    }

    if target.contains("msvc") {
        if cfg!(feature = "mt_static") {
            config.static_crt(true);
        }
        config.flag("-EHsc");
        // Don't use cxx_standard: Uses : instead of =
        config.flag("-std:c++20");
    } else {
        config.flag(cxx_standard());
        // matches the flags in CMakeLists.txt from rocksdb
        config.flag("-Wsign-compare");
        config.flag("-Wshadow");
        config.flag("-Wno-unused-parameter");
        config.flag("-Wno-unused-variable");
        config.flag("-Woverloaded-virtual");
        config.flag("-Wnon-virtual-dtor");
        config.flag("-Wno-missing-field-initializers");
        config.flag("-Wno-strict-aliasing");
        config.flag("-Wno-invalid-offsetof");
    }
    if target.contains("riscv64gc") {
        // link libatomic required to build for riscv64gc
        println!("cargo:rustc-link-lib=atomic");
    }
    for file in lib_sources {
        config.file(format!("rocksdb/{file}"));
    }

    config.file("build_version.cc");

    config.cpp(true);

    if !target.contains("windows") {
        config.flag("-include").flag("cstdint");
    }

    // By default `cc` will link C++ standard library automatically,
    // see https://docs.rs/cc/latest/cc/index.html#c-support.
    // There is no need to manually set `cpp_link_stdlib`.

    config.compile("librocksdb.a");
}

fn build_snappy() {
    let target = env::var("TARGET").unwrap();
    let endianness = env::var("CARGO_CFG_TARGET_ENDIAN").unwrap();
    let mut config = cc::Build::new();

    config.include("snappy/");
    config.include(".");
    config.define("NDEBUG", Some("1"));
    config.extra_warnings(false);

    if target.contains("msvc") {
        config.flag("-EHsc");
        if cfg!(feature = "mt_static") {
            config.static_crt(true);
        }
        config.flag("-std:c++20");
    } else {
        config.flag("-std=c++20");
    }

    if endianness == "big" {
        config.define("SNAPPY_IS_BIG_ENDIAN", Some("1"));
    }

    config.file("snappy/snappy.cc");
    config.file("snappy/snappy-sinksource.cc");
    config.file("snappy/snappy-c.cc");
    config.cpp(true);
    config.compile("libsnappy.a");
}

fn try_to_find_and_link_lib(lib_name: &str) -> bool {
    println!("cargo:rerun-if-env-changed={lib_name}_COMPILE");
    if let Ok(v) = env::var(format!("{lib_name}_COMPILE"))
        && (v.to_lowercase() == "true" || v == "1")
    {
        return false;
    }

    println!("cargo:rerun-if-env-changed={lib_name}_LIB_DIR");
    println!("cargo:rerun-if-env-changed={lib_name}_STATIC");

    if let Ok(lib_dir) = env::var(format!("{lib_name}_LIB_DIR")) {
        println!("cargo:rustc-link-search=native={lib_dir}");
        let mode = match env::var_os(format!("{lib_name}_STATIC")) {
            Some(_) => "static",
            None => "dylib",
        };
        println!("cargo:rustc-link-lib={}={}", mode, lib_name.to_lowercase());
        return true;
    }
    false
}

/// Returns the value of the `ROCKSDB_CXX_STD` env var, or the default `-std=c++{version}` flag for
/// building RocksDB.
fn cxx_standard() -> String {
    env::var("ROCKSDB_CXX_STD").map_or("-std=c++20".to_owned(), |cxx_std| {
        if !cxx_std.starts_with("-std=") {
            format!("-std={cxx_std}")
        } else {
            cxx_std
        }
    })
}

fn update_submodules() {
    let program = "git";
    let dir = "../";
    let args = ["submodule", "update", "--init"];
    println!(
        "Running command: \"{} {}\" in dir: {}",
        program,
        args.join(" "),
        dir
    );
    let ret = Command::new(program).current_dir(dir).args(args).status();

    match ret.map(|status| (status.success(), status.code())) {
        Ok((true, _)) => (),
        Ok((false, Some(c))) => panic!("Command failed with error code {c}"),
        Ok((false, None)) => panic!("Command got killed"),
        Err(e) => panic!("Command failed with error: {e}"),
    }
}

fn cpp_link_stdlib(target: &str) {
    // according to https://github.com/alexcrichton/cc-rs/blob/master/src/lib.rs#L2189
    if let Ok(stdlib) = env::var("CXXSTDLIB") {
        println!("cargo:rustc-link-lib=dylib={stdlib}");
    } else if target.contains("apple") || target.contains("freebsd") || target.contains("openbsd") {
        println!("cargo:rustc-link-lib=dylib=c++");
    } else if target.contains("linux") {
        println!("cargo:rustc-link-lib=dylib=stdc++");
    } else if target.contains("aix") {
        println!("cargo:rustc-link-lib=dylib=c++");
        println!("cargo:rustc-link-lib=dylib=c++abi");
    }
}

fn main() {
    if !Path::new("rocksdb/AUTHORS").exists() {
        update_submodules();
    }
    bindgen_rocksdb();
    let target = env::var("TARGET").unwrap();

    if !try_to_find_and_link_lib("ROCKSDB") {
        // rocksdb only works with the prebuilt rocksdb system lib on freebsd.
        // we dont need to rebuild rocksdb
        if target.contains("freebsd") {
            println!("cargo:rustc-link-search=native=/usr/local/lib");
            let mode = match env::var_os("ROCKSDB_STATIC") {
                Some(_) => "static",
                None => "dylib",
            };
            println!("cargo:rustc-link-lib={mode}=rocksdb");

            return;
        }

        println!("cargo:rerun-if-changed=rocksdb/");
        fail_on_empty_directory("rocksdb");
        build_rocksdb();
    } else {
        cpp_link_stdlib(&target);
    }
    if cfg!(feature = "snappy") && !try_to_find_and_link_lib("SNAPPY") {
        println!("cargo:rerun-if-changed=snappy/");
        fail_on_empty_directory("snappy");
        build_snappy();
    }

    // Allow dependent crates to locate the sources and output directory of
    // this crate. Notably, this allows a dependent crate to locate the RocksDB
    // sources and built archive artifacts provided by this crate.
    println!(
        "cargo:cargo_manifest_dir={}",
        env::var("CARGO_MANIFEST_DIR").unwrap()
    );
    println!("cargo:out_dir={}", env::var("OUT_DIR").unwrap());
}
