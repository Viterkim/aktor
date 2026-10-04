use aktor::{aktor, listener::Handle};

struct Database;
struct Audio;

#[aktor(crate = aktor, actor = Database)]
async fn read(state: &usize) -> usize {
    *state
}

fn queued(handle: &Handle<usize, Audio>) {
    let _request = read(handle);
}
