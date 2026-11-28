import init, { start_worker } from './pkg/aktor_worker_proof.js';

await init();

await start_worker();
