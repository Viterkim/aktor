use std::{
    process::{Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

pub fn child(test: &str, case: &str) -> Output {
    use std::io::Read;

    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--nocapture"])
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
