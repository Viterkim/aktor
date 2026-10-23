await new Promise(resolve => setTimeout(resolve, 1000));

export { default, __aktor_thread_start, __aktor_thread_cancel } from './pkg/aktor_thread_proof.js';
