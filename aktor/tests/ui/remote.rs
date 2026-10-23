use aktor::aktor;

struct Value;

#[aktor(crate = aktor, remote)]
async fn read(_: &u32) -> Value {
    Value
}
