postMessage(
    JSON.stringify({
        Ready: {
            version: 0,
            options: {
                build: 'old bundle',
                timeout_ms: 5000,
                capacity: 32,
                max_payload_bytes: 1048576,
                max_outstanding_bytes: 4194304
            }
        }
    })
);
