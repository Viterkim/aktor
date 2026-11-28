import init, { start_setup_worker } from './pkg/aktor_worker_proof.js';

await init();

await start_setup_worker(new URL(location.href).searchParams.get('build') || '');
