import init, {
    start_group_worker,
    start_role_worker,
    typed_setup_failed
} from './pkg/aktor_worker_proof.js';

await init();

const parameters = new URL(location.href).searchParams;

if (parameters.has('fail_setup')) {
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
