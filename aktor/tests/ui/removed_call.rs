use aktor::*;

#[aktor(crate = aktor)]
async fn clear(state: &mut u32) {
    *state = 0;
}

fn removed(handle: &listener::Handle<u32>) {
    let _ = clear(handle).try_send();
    let _ = clear(handle).try_send_recover();
    let _ = clear(handle).try_cast();
    let _ = clear(handle).try_cast_recover();
}
