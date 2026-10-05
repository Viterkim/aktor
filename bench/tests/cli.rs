use std::process::{Command, Output};

fn run(settings: &[(&str, &str)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_aktor-bench"));

    for name in [
        "BACKEND", "WORK", "CLIENTS", "CALLS", "REPEATS", "RUNTIME", "CAPACITY",
    ] {
        command.env_remove(name);
    }

    command.envs([
        ("CALLS", "32"),
        ("REPEATS", "1"),
        ("WORK", "count"),
        ("CLIENTS", "1"),
    ]);

    command.envs(settings.iter().copied()).output().unwrap()
}

#[test]
fn selection() {
    for settings in [
        vec![("BACKEND", "typo")],
        vec![("BACKEND", "direct-aktor"), ("CLIENTS", "16")],
        vec![("BACKEND", "actify"), ("CAPACITY", "1")],
        vec![("BACKEND", "aktor-task"), ("CAPACITY", "0")],
    ] {
        let output = run(&settings);
        assert!(!output.status.success(), "{settings:?}");
        assert!(output.stdout.is_empty(), "an invalid run printed CSV");
    }

    #[cfg(feature = "allocations")]
    assert!(!run(&[("BACKEND", "aktor-thread")]).status.success());

    let output = run(&[("BACKEND", "aktor-task")]);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let csv = String::from_utf8(output.stdout).unwrap();

    assert_eq!(csv.lines().count(), 2);
    assert!(csv.lines().nth(1).unwrap().starts_with("aktor-task,count,"));
}

#[cfg(not(feature = "allocations"))]
#[test]
fn pressure() {
    let output = run(&[
        ("BACKEND", "aktor-task"),
        ("CLIENTS", "16"),
        ("CAPACITY", "1"),
    ]);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("one blocked admission, all drained"));
    assert_eq!(String::from_utf8(output.stdout).unwrap().lines().count(), 2);
}
