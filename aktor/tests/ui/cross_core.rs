use aktor::*;
use std::rc::Rc;

#[aktor(crate = aktor)]
async fn input(counter: &mut u32, value: Rc<u32>) {
    *counter += *value;
}

#[aktor(crate = aktor)]
async fn output(counter: &u32) -> Rc<u32> {
    Rc::new(*counter)
}

pub async fn transferred(counter: &cross_core::Handle<u32>) {
    input(counter, Rc::new(1)).await;
    output(counter).await;
}
