use wasm_bindgen::prelude::*;

#[wasm_bindgen(inline_js = r#"
const actorWorker = `
let bindings;
let exports;
let job;
let stopping = false;
let initializing = false;
let retired = false;
let destroy;

self.addEventListener('error', event => {
    if (retired) event.preventDefault();
    self.close();
});
self.addEventListener('unhandledrejection', event => {
    if (retired) event.preventDefault();
    else self.postMessage({ error: String(event.reason) });
    self.close();
});

self.onmessage = async ({ data }) => {
    if (data.cancel) {
        stopping = true;

        if (exports) {
            bindings.__aktor_thread_cancel(job);
        } else if (!initializing) {
            self.postMessage({ error: 'actor worker cancelled during startup' });
            self.close();
        }

        return;
    }

    job = data.job;

    let outcome;

    try {
        bindings = await import(data.program);

        if (stopping) throw new Error('actor worker cancelled during startup');
        initializing = true;
        const instantiate = WebAssembly.instantiate;

        // wasm-bindgen keeps these exports in its callback closures too.
        WebAssembly.instantiate = async (module, imports) => {
            const instance = await instantiate(module, imports);
            if (module !== data.module) return instance;

            const guarded = { ...instance.exports };
            destroy = guarded.__wbindgen_thread_destroy;

            for (const [name, value] of Object.entries(guarded)) {
                if (typeof value !== 'function') continue;

                guarded[name] = (...args) => {
                    if (retired) throw new Error('actor worker already stopped');
                    return value(...args);
                };
            }

            Object.defineProperty(instance, 'exports', { value: guarded });
            return { instance, module };
        };

        try {
            exports = await bindings.default({ module_or_path: data.module, memory: data.memory });
        } finally {
            WebAssembly.instantiate = instantiate;
        }

        if (exports.memory !== data.memory || !destroy) {
            throw new Error('actor worker did not import the shared memory');
        }

        if (stopping) throw new Error('actor worker cancelled during startup');
        await bindings.__aktor_thread_start(job);
        outcome = { done: true };
    } catch (error) {
        outcome = { error: String(error?.stack || error) };
    } finally {
        try {
            // Pending Promise callbacks must never enter Wasm after this last call.
            retired = true;
            if (destroy) destroy();
        } catch (error) {
            outcome = { error: String(error?.stack || error) };
        }

        self.postMessage(outcome);
        self.close();
    }
};
`;

export function openThread(program, module, memory, job) {
    if (!(memory.buffer instanceof SharedArrayBuffer)) {
        throw new Error('BrowserThread requires a shared-memory Wasm build and cross-origin isolation');
    }

    program = new URL(program, globalThis.location.href).href;
    const url = URL.createObjectURL(new Blob([actorWorker], { type: 'text/javascript' }));
    let worker;

    try {
        worker = new Worker(url, { type: 'module' });
    } finally {
        URL.revokeObjectURL(url);
    }

    const finished = new Promise((resolve, reject) => {
        worker.onmessage = ({ data }) => {
            if (data.done) resolve();
            else reject(new Error(data.error));
        };
        worker.onerror = event => {
            event.preventDefault();
            reject(new Error(event.message || 'shared actor worker failed'));
        };
    });

    try {
        worker.postMessage({ program, module, memory, job });
    } catch (error) {
        worker.terminate();
        throw error;
    }
    return { worker, finished, job };
}

export function finishThread(thread) {
    return thread.finished;
}

export function cancelThread(thread) {
    // Let Rust drop its work on its own thread. terminate() can strand shared locks.
    thread.worker.postMessage({ cancel: true });
}
"#)]
extern "C" {
    pub type Thread;

    #[wasm_bindgen(catch, js_name = openThread)]
    pub fn open(
        program: &str,
        module: JsValue,
        memory: JsValue,
        job: u32,
    ) -> Result<Thread, JsValue>;

    #[wasm_bindgen(js_name = finishThread)]
    pub fn finished(thread: &Thread) -> js_sys::Promise;

    #[wasm_bindgen(js_name = cancelThread)]
    pub fn cancel(thread: &Thread);
}

impl Thread {
    pub fn start(program: &str, job: u32) -> Result<Self, JsValue> {
        open(program, wasm_bindgen::module(), wasm_bindgen::memory(), job)
    }

    pub async fn wait(&self) -> Result<(), JsValue> {
        wasm_bindgen_futures::JsFuture::from(finished(self))
            .await
            .map(|_| ())
    }

    pub fn cancel(&self) {
        cancel(self);
    }
}
