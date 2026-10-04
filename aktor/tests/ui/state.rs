use aktor::aktor;

#[aktor(crate = aktor)]
async fn read(state: String) -> usize {
    state.len()
}
