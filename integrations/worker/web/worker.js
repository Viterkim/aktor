import init, { start_worker, startup_failed } from './pkg/aktor_worker_proof.js';
await init();
try {
    await start_worker();
} catch (error) {
    startup_failed(String(error));
}
