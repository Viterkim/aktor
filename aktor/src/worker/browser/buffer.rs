use js_sys::{ArrayBuffer, Uint8Array};
use std::cell::RefCell;
use wasm_bindgen::prelude::*;

pub const MIN_REUSE: usize = 4096;

#[wasm_bindgen(inline_js = "
export function transfer_buffer(length, capacity) {
    if (capacity > 0 && typeof ArrayBuffer.prototype.resize === 'function') {
        try {
            return new ArrayBuffer(length, { maxByteLength: capacity });
        } catch (_) {}
    }

    return new ArrayBuffer(length);
}

export function resize_transfer_buffer(buffer, length) {
    if (buffer.byteLength === length) return true;

    if (buffer.resizable && length <= buffer.maxByteLength) {
        buffer.resize(length);
        return true;
    }

    return false;
}

export function transfer_capacity(buffer) {
    return buffer.resizable ? buffer.maxByteLength : buffer.byteLength;
}
")]
extern "C" {
    #[wasm_bindgen(catch)]
    fn transfer_buffer(length: usize, capacity: usize) -> Result<ArrayBuffer, JsValue>;
    #[wasm_bindgen(catch)]
    fn resize_transfer_buffer(buffer: &ArrayBuffer, length: usize) -> Result<bool, JsValue>;
    fn transfer_capacity(buffer: &ArrayBuffer) -> f64;
}

// Calls pass the buffer back and forth. Control messages leave it alone.
pub struct Buffers {
    cached: RefCell<Option<(ArrayBuffer, f64)>>,
    limit: usize,
}
impl Buffers {
    pub fn new(byte_budget: usize) -> Self {
        Self {
            cached: RefCell::new(None),
            limit: byte_budget.min(8 * 1024 * 1024),
        }
    }

    pub fn write(&self, bytes: &[u8]) -> Result<ArrayBuffer, JsValue> {
        let cached = self.cached.borrow_mut().take();

        if cached.is_none() && bytes.len() < MIN_REUSE {
            return Ok(Uint8Array::from(bytes).buffer());
        }

        let buffer = if let Some((buffer, _)) = cached
            && resize_transfer_buffer(&buffer, bytes.len())?
        {
            buffer
        } else {
            let capacity = if bytes.len() <= self.limit {
                bytes.len().next_power_of_two().min(self.limit)
            } else {
                0
            };

            transfer_buffer(bytes.len(), capacity)?
        };

        Uint8Array::new(&buffer).copy_from(bytes);
        Ok(buffer)
    }

    pub fn retain(&self, buffer: ArrayBuffer) {
        let capacity = transfer_capacity(&buffer);

        if capacity < MIN_REUSE as f64 || capacity > self.limit as f64 {
            return;
        }

        let previous = {
            let mut cached = self.cached.borrow_mut();

            if cached
                .as_ref()
                .is_some_and(|(_, previous)| *previous > capacity)
            {
                return;
            }

            cached.replace((buffer, capacity))
        };

        drop(previous);
    }

    pub fn clear(&self) {
        let cached = self.cached.borrow_mut().take();
        drop(cached);
    }
}
