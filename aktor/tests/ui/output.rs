use aktor::aktor;

#[aktor(crate = aktor)]
async fn read(state: &String) -> &str {
    state
}
