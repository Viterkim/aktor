use wasm_bindgen::prelude::*;

#[wasm_bindgen(inline_js = "export function report_panic(message) { console.error(message); }")]
extern "C" {
    fn report_panic(message: &str);
}

#[wasm_bindgen(start)]
pub fn install_hook() {
    std::panic::set_hook(Box::new(|info| report_panic(&info.to_string())));
}
