use aktor::{AktorData, data};

#[derive(AktorData)]
#[aktor(crate = aktor)]
pub struct Message<T> {
    pub value: u64,
    #[aktor(skip)]
    pub cache: T,
}

pub struct Cache;

pub fn encode() {
    let message = Message {
        value: 7,
        cache: Cache,
    };
    let _ = data::encode(&message);
}
