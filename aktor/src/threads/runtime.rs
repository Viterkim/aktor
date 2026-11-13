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

function finish(outcome, orderly = true) {
    if (retired) return;

    // Pending Promise callbacks must never enter Wasm after this last call.
    retired = true;

    try {
        if (orderly && destroy) destroy();
        outcome.retired = orderly && (!initializing || !!destroy);
    } catch (error) {
        const message = String(error?.stack || error);
        outcome.error = outcome.error ? outcome.error + '\\nThread cleanup failed: ' + message : message;
        delete outcome.done;
    }

    self.postMessage(outcome);
    self.close();
}

self.addEventListener('error', event => {
    event.preventDefault();
    finish({ error: String(event.error?.stack || event.message || 'shared actor worker failed') }, false);
});
self.addEventListener('unhandledrejection', event => {
    event.preventDefault();
    finish({ error: String(event.reason?.stack || event.reason) }, false);
});

self.onmessage = async ({ data }) => {
    if (data.cancel) {
        stopping = true;

        if (exports) {
            bindings.__aktor_thread_cancel(job);
        } else if (!initializing) {
            finish({ error: 'actor worker cancelled during startup' });
        }

        return;
    }

    job = data.job;

    let outcome;
    let orderly = true;

    try {
        bindings = await import(data.program);

        if (stopping) throw new Error('actor worker cancelled during startup');
        initializing = true;
        const instantiate = WebAssembly.instantiate;
        let threadDestroy;

        // wasm-bindgen keeps these exports in its callback closures too.
        WebAssembly.instantiate = async (module, imports) => {
            const instance = await instantiate(module, imports);
            if (module !== data.module) return instance;

            const guarded = { ...instance.exports };
            threadDestroy = guarded.__wbindgen_thread_destroy;

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

        if (exports.memory !== data.memory || !threadDestroy) {
            throw new Error('actor worker did not import the shared memory');
        }

        destroy = threadDestroy;
        if (stopping) throw new Error('actor worker cancelled during startup');
        await bindings.__aktor_thread_start(job);
        outcome = { done: true };
    } catch (error) {
        orderly = !(error instanceof WebAssembly.RuntimeError);
        outcome = { error: String(error?.stack || error) };
    } finally {
        finish(outcome, orderly);
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

    const thread = { worker, job, retired: false };
    const finished = new Promise((resolve, reject) => {
        worker.onmessage = ({ data }) => {
            thread.retired = data.retired === true;
            if (data.done) resolve();
            else reject(new Error(data.error));
        };
        worker.onerror = event => {
            event.preventDefault();
            let source = event.filename || program;

            if (event.lineno) {
                source += `:${event.lineno}`;
                if (event.colno) source += `:${event.colno}`;
            } else if (event.colno) {
                source += ` (column ${event.colno})`;
            }

            reject(new Error(`${source}: ${event.message || 'shared actor worker failed'}`));
        };
    });

    try {
        worker.postMessage({ program, module, memory, job });
    } catch (error) {
        worker.terminate();
        throw error;
    }
    thread.finished = finished;
    return thread;
}

export function threadRetired(thread) {
    return thread.retired;
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

    #[wasm_bindgen(js_name = threadRetired)]
    pub fn retired(thread: &Thread) -> bool;

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

    pub fn retirement_received(&self) -> bool {
        retired(self)
    }

    pub fn cancel(&self) {
        cancel(self);
    }
}
