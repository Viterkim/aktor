use aktor::AktorData;

#[derive(AktorData, aktor::data::__serde::Serialize)]
#[serde(crate = "aktor::data::__serde")]
#[aktor(crate = aktor)]
pub struct Message {
    #[serde(skip_serializing)]
    pub secret: String,
}
