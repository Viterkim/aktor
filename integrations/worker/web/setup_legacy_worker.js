import init, { start_legacy_setup_worker, startup_failed } from './pkg/aktor_worker_proof.js';

await init();

try {
    await start_legacy_setup_worker();
} catch (error) {
    startup_failed(String(error));
}
