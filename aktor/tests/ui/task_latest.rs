use aktor::*;
use std::rc::Rc;

#[aktor(crate = aktor)]
async fn read(_: &mut u32) -> usize {
    let value = Rc::new(7);
    std::future::ready(()).await;
    *value
}

fn latest(handle: &AktorTask<u32>) {
    let (sender, _) = read::latest(handle);
    sender.send();
}
