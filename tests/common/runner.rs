//! Test runners for test targets with `harness = false`.

use std::process::ExitCode;

use sd_event_futures::os;

/// `[(name, test)]` for [`run_single_threaded`] and [`run_example_or`].
#[allow(unused_macros)]
macro_rules! tests {
    ($($test:ident),* $(,)?) => {
        &[$((stringify!($test), $test as fn())),*]
    };
}
pub(crate) use tests;

/// Set to make a test binary that includes an example run that example
/// instead of its tests; see [`run_example_or`].
pub const RUN_EXAMPLE: (&str, &str) = ("LEARN_FUTURES_RUN_EXAMPLE", "1");

/// The `main()` of a test binary that includes an example program.
/// Integration tests can't depend on an example's binary, so the test
/// binary doubles as the example: started with [`RUN_EXAMPLE`] set (see
/// [`this_program`]), it runs `example`, the example's `main()`; otherwise
/// it runs `tests` one after another.
pub fn run_example_or(example: fn() -> ExitCode, tests: &[(&str, fn())]) -> ExitCode {
    if std::env::var_os(RUN_EXAMPLE.0).is_some() {
        return example();
    }
    run(tests, |_| {})
}

/// The running test binary, to start as its example with [`RUN_EXAMPLE`].
pub fn this_program() -> String {
    std::env::current_exe()
        .unwrap()
        .into_os_string()
        .into_string()
        .unwrap()
}

/// Run `tests` one after another on the main thread. For tests that fork:
/// like PID 1, the process then has a single thread, so forked children
/// may do anything.
pub fn run_single_threaded(tests: &[(&str, fn())]) -> ExitCode {
    run(tests, |name| {
        assert_eq!(
            os::thread_count(),
            1,
            "{name} would fork with other threads running"
        );
    })
}

/// Run `tests` and print results the way libtest does. Arguments that
/// don't start with `-` filter tests by name.
fn run(tests: &[(&str, fn())], before_each: impl Fn(&str)) -> ExitCode {
    let filters: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| !a.starts_with('-'))
        .collect();
    let (mut passed, mut failed) = (0, Vec::new());
    for &(name, test) in tests {
        if !filters.is_empty() && !filters.iter().any(|f| name.contains(f.as_str())) {
            continue;
        }
        before_each(name);
        if std::panic::catch_unwind(test).is_ok() {
            println!("test {name} ... ok");
            passed += 1;
        } else {
            println!("test {name} ... FAILED");
            failed.push(name);
        }
    }
    let verdict = if failed.is_empty() { "ok" } else { "FAILED" };
    println!(
        "\ntest result: {verdict}. {passed} passed; {} failed",
        failed.len()
    );
    if failed.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
