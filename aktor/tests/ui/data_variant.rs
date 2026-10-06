use aktor::AktorData;

#[derive(AktorData, aktor::data::__serde::Serialize)]
#[serde(crate = "aktor::data::__serde")]
#[aktor(crate = aktor)]
pub enum Message {
    #[serde(skip)]
    Secret,
}
