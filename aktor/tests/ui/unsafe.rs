use aktor::aktor;

#[aktor]
async unsafe fn read(_: &u32) -> u32 {
    7
}
