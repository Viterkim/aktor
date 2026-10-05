import init, { start_setup_worker, startup_failed } from './pkg/aktor_worker_proof.js';

await init();

try {
    await start_setup_worker(new URL(location.href).searchParams.get('build') || '');
} catch (error) {
    startup_failed(String(error));
}
