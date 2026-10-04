const { default: init, start_registration_worker } = await import('./pkg/aktor_worker_proof.js');

await init();
start_registration_worker(new URL(location.href).searchParams.has('signature'));
