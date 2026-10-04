use aktor::{aktor, listener::channel};
use std::{io, thread};
use tokio::runtime::Builder;

#[aktor]
async fn add(count: &mut u32, amount: u32) -> u32 {
    *count += amount;
    *count
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn core::error::Error>> {
    let (handle, mut listener) = channel::<u32>(8)?;

    let worker = thread::spawn(move || -> io::Result<u32> {
        let runtime = Builder::new_current_thread().build()?;

        runtime.block_on(async {
            let mut count = 0;

            while let Some(message) = listener.recv().await {
                let name = message.operation.name;
                println!("starting {name}");

                message.run(&mut count).await;

                println!("finished {name}");
            }

            Ok(count)
        })
    });

    println!("reply: {}", add(&handle, 2).await);
    println!("reply: {}", add(&handle, 3).await);

    drop(handle);
    let count = worker
        .join()
        .map_err(|_| io::Error::other("counter thread panicked"))??;

    println!("final count: {count}");
    Ok(())
}
