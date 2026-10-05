use aktor::*;

fn duplicate() {
    aktor_setups! {
        counter: (),
        counter: (),
    };
}
