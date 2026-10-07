use std::{
    boxed::Box,
    process::{Command, Output, Stdio},
    string::String,
    thread,
    time::{Duration, Instant},
    vec::Vec,
};

pub fn child_command(test: &str) -> Command {
    let executable = std::env::current_exe().unwrap();
    let listed = Command::new(&executable)
        .args(["--list", "--exact", test])
        .output()
        .unwrap();
    let names = String::from_utf8_lossy(&listed.stdout);
    let expected = std::format!("{test}: test");

    assert!(
        listed.status.success() && names.lines().any(|name| name == expected),
        "unknown child test: {test}\n{names}\n{}",
        String::from_utf8_lossy(&listed.stderr)
    );

    let mut command = Command::new(executable);
    command.args([
        "--exact",
        test,
        "--nocapture",
        "--test-threads=1",
        "--format=pretty",
        "--color=never",
    ]);
    command
}

pub fn child(test: &str, case: &str) -> Output {
    use std::io::Read;

    let mut child = child_command(test)
        .env("AKTOR_CHILD", case)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let output = thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).unwrap();
        bytes
    });

    let errors = thread::spawn(move || {
        let mut bytes = Vec::new();
        stderr.read_to_end(&mut bytes).unwrap();
        bytes
    });

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut timed_out = false;
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }

        if Instant::now() >= deadline {
            child.kill().unwrap();
            timed_out = true;
            break child.wait().unwrap();
        }

        thread::sleep(Duration::from_millis(10));
    };

    let output = Output {
        status,
        stdout: output.join().unwrap(),
        stderr: errors.join().unwrap(),
    };

    assert!(
        !timed_out,
        "child timed out: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    if output.status.success() {
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .any(|line| line.starts_with("test result: ok. 1 passed; 0 failed;")),
            "child test did not finish: {test} ({case})\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    output
}

pub async fn panics<F: std::future::Future>(future: F) -> bool {
    let mut future = Box::pin(future);

    std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(std::task::Poll::Ready(_)) => std::task::Poll::Ready(false),
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
            Err(_) => std::task::Poll::Ready(true),
        }
    })
    .await
}

pub fn poll<F: std::future::Future + Unpin>(mut future: F) -> std::task::Poll<F::Output> {
    std::pin::Pin::new(&mut future)
        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
}

#[derive(Default)]
pub struct CountWake(pub std::sync::atomic::AtomicUsize);
impl std::task::Wake for CountWake {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}
