import init, { start_role_worker } from './pkg/aktor_worker_proof.js';

await init();
start_role_worker();
