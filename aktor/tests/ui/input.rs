use aktor::{aktor, listener::Handle};

#[aktor(crate = aktor)]
async fn write(state: &mut String, input: &str) {
    state.push_str(input);
}

fn queued(handle: &Handle<String>, input: &str) {
    let _request = write(handle, input);
}
