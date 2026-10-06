use aktor::AktorData;

#[derive(AktorData, aktor::data::__serde::Serialize)]
#[serde(crate = "aktor::data::__serde")]
#[aktor(crate = aktor)]
pub struct Message {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub secret: String,
}
