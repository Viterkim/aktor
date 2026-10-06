const parameters = new URL(location.href).searchParams;

globalThis.aktorCleanupGate = parameters.has('cleanup_gate');
globalThis.aktorGate = new Promise(resolve => {
    globalThis.addEventListener('message', event => {
        if (event.data === 'release') {
            event.stopImmediatePropagation();
            resolve();
        }
    });
});

if (parameters.has('startup_gate')) {
    const { default: init, start_unready_worker } = await import('./pkg/aktor_worker_proof.js');

    await init();
    await start_unready_worker();
} else {
    await import('./group-worker.js');
}
