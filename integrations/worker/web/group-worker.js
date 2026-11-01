import init, {
    start_group_worker,
    start_role_worker,
    start_group_setup_worker,
    start_rejected_worker,
    start_unconfirmed_worker,
    typed_setup_failed
} from './pkg/aktor_worker_proof.js';

await init();

const parameters = new URL(location.href).searchParams;

if (parameters.has('unconfirmed')) {
    await start_unconfirmed_worker(Number(parameters.get('unconfirmed')));
} else if (parameters.has('rollback')) {
    await start_rejected_worker();
} else if (parameters.has('configured')) {
    await start_group_setup_worker();
} else if (parameters.has('fail_setup')) {
    typed_setup_failed();
} else if (parameters.has('other')) {
    start_role_worker();
} else {
    start_group_worker(
        parameters.get('fail_cleanup') === 'true',
        parameters.get('wrong_data') === 'true',
        parameters.get('fail_data') === 'true'
    );
}
