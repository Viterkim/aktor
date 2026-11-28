use aktor::AktorData;

#[derive(AktorData)]
#[aktor(crate = aktor)]
pub enum Message {
    #[aktor(skip)]
    Secret,
}
