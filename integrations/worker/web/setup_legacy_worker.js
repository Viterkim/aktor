import init, { start_legacy_setup_worker } from './pkg/aktor_worker_proof.js';

await init();

await start_legacy_setup_worker();
