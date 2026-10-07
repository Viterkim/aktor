use aktor::*;

#[aktor(crate = aktor)]
async fn clear(state: &mut u32) {
    *state = 0;
}

fn removed(handle: &listener::Handle<u32>) {
    let _ = clear(handle).into_request().try_send();
    let _ = clear(handle).into_request().try_send_recover();
    let _ = clear(handle).into_request().try_cast();
    let _ = clear(handle).into_request().try_cast_recover();
}
