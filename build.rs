fn main() {
    let lib = pkg_config::Config::new()
        .atleast_version("239")
        .probe("libsystemd")
        .expect("libsystemd not found via pkg-config");

    for path in &lib.include_paths {
        let candidate = path.join("systemd/sd-event.h");
        if candidate.exists() {
            println!("cargo:rustc-env=SD_EVENT_HEADER={}", candidate.display());
            println!("cargo:rerun-if-changed={}", candidate.display());
            return;
        }
    }
    panic!("sd-event.h not found in libsystemd include paths");
}
