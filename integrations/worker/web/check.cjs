const assert = require('node:assert/strict');
const playwright = require('playwright');
const engine = process.env.AKTOR_BROWSER || 'chromium';

(async () => {
    const browser = await playwright[engine].launch({ headless: true });

    try {
        const context = await browser.newContext();
        const page = await context.newPage();
        const frontendErrors = [];

        page.on('pageerror', error => {
            frontendErrors.push(error.message);
            console.error(error);
        });

        await page.goto(process.env.AKTOR_PROOF_URL || 'http://127.0.0.1:8765');
        await page.waitForFunction(() => window.ready);

        const registration = await page.evaluate(async () => {
            const missing = JSON.parse(
                await window.registrationCheck(
                    new URL('./missing-operation.js', location.href).href,
                    false
                )
            );
            const signature = JSON.parse(
                await window.registrationCheck(
                    new URL('./missing-operation.js?signature', location.href).href,
                    false
                )
            );
            const role = JSON.parse(
                await window.registrationCheck(
                    new URL('./role-worker.js', location.href).href,
                    true
                )
            );

            return { missing, signature, role };
        });

        assert.equal(registration.missing.Err.outcome, 'NotAdmitted');
        assert.equal(registration.missing.Err.cause.Operations.expected.length, 3);
        assert.deepEqual(registration.missing.Err.cause.Operations.actual, []);
        assert.equal(registration.signature.Err.outcome, 'NotAdmitted');
        assert.match(registration.signature.Err.cause.Operations.actual[0], /u64/);
        assert.equal(registration.role, 17);
        assert.deepEqual(await page.evaluate(() => window.setup), { Ok: null });

        for (const mode of [0, 1, 2, 3, 4, 5, 6, 7, 8]) {
            const result = await page.evaluate(async mode => {
                let timer;

                try {
                    const timedOut = new Promise((_, reject) => {
                        timer = setTimeout(
                            () => reject(new Error(`group ${mode} did not finish`)),
                            5000
                        );
                    });
                    const completed = window.groupCheck(
                        new URL('./group-worker.js', location.href).href,
                        mode
                    );

                    return JSON.parse(await Promise.race([completed, timedOut]));
                } finally {
                    clearTimeout(timer);
                }
            }, mode);

            assert.equal(result.hook_count, 1);
            assert.equal(result.typed_cleanup, true);
            assert.equal(result.deferred_reply_safe, true);
            assert.equal(result.actors, 2);

            if (mode === 0) {
                assert.equal(result.ordinary_error, true);
                assert.equal(result.failure, null);
            }

            if (mode === 1 || mode === 2 || mode === 3) {
                assert.equal(result.failure, 'storage');
            }

            if (mode === 2) {
                assert.equal(result.timed_out, true);
                assert.deepEqual(result.timed_out_actors, ['storage']);
            }

            if (mode === 4 || mode === 6 || mode === 7) {
                assert.ok(
                    result.diagnostics.some(([, reports]) =>
                        reports.some(report => report.includes('backup folder is read only'))
                    )
                );
            }

            if (mode === 5) {
                assert.equal(result.failure, 'storage');
                assert.match(result.failure_message, /input refused/);
            }

            if (mode === 8) {
                assert.equal(result.failure, 'storage');
                assert.match(result.failure_message, /output refused/);
                assert.equal(result.timed_out, false);
            }

            console.log('group', mode, result);
        }

        for (const mode of [0, 1, 2]) {
            assert.equal(
                await page.evaluate(
                    mode =>
                        window.groupStartupCheck(
                            new URL('./group-worker.js', location.href).href,
                            mode
                        ),
                    mode
                ),
                true
            );
        }

        assert.equal(
            await page.evaluate(() =>
                window.groupListenerCheck(new URL('./group-worker.js', location.href).href)
            ),
            true
        );
        assert.equal(
            await page.evaluate(() =>
                window.typedSetupCheck(new URL('./group-worker.js?fail_setup', location.href).href)
            ),
            true
        );
        assert.deepEqual(frontendErrors, []);

        assert.deepEqual(
            await page.evaluate(async () => JSON.parse(await client.write('volume', '0.7'))),
            { Ok: '0.7' }
        );
        assert.deepEqual(await page.evaluate(async () => JSON.parse(await client.read('absent'))), {
            Err: { Missing: 'absent' }
        });

        await page.reload();
        await page.waitForFunction(() => window.ready);
        assert.deepEqual(await page.evaluate(async () => JSON.parse(await client.read('volume'))), {
            Ok: '0.7'
        });

        const result = await page.evaluate(async () => {
            const a = new RTCPeerConnection({ iceServers: [] });
            const b = new RTCPeerConnection({ iceServers: [] });
            a.onicecandidate = event => {
                if (event.candidate) {
                    b.addIceCandidate(event.candidate);
                }
            };
            b.onicecandidate = event => {
                if (event.candidate) {
                    a.addIceCandidate(event.candidate);
                }
            };

            const received = [];
            const remote = new Promise(resolve => {
                b.ondatachannel = event => {
                    event.channel.onmessage = message => {
                        const sent = JSON.parse(message.data);
                        received.push({ ...sent, received: performance.now() });
                    };
                    resolve(event.channel);
                };
            });

            const channel = a.createDataChannel('chat');
            const opened = new Promise(resolve => {
                channel.onopen = resolve;
            });

            await a.setLocalDescription(await a.createOffer());
            await b.setRemoteDescription(a.localDescription);
            await b.setLocalDescription(await b.createAnswer());
            await a.setRemoteDescription(b.localDescription);
            await Promise.all([opened, remote]);

            const occupied = client.occupy(700);
            while (!client.executing()) {
                await new Promise(resolve => setTimeout(resolve, 1));
            }

            const start = performance.now();
            let ticks = 0;
            const timer = setInterval(() => {
                ticks += 1;
                channel.send(JSON.stringify({ id: ticks, sent: performance.now() }));
            }, 20);

            const output = await occupied;
            const end = performance.now();
            const elapsed = end - start;

            clearInterval(timer);
            await new Promise(resolve => setTimeout(resolve, 40));
            channel.close();
            a.close();
            b.close();

            return {
                ticks,
                received: received.length,
                during: received.filter(message => message.received <= end).length,
                worstLatency: Math.max(...received.map(message => message.received - message.sent)),
                elapsed,
                occupied: JSON.parse(output)
            };
        });

        assert.deepEqual(result.occupied, 700);
        assert.ok(result.ticks >= 15, JSON.stringify(result));
        assert.equal(result.during, result.ticks, JSON.stringify(result));
        assert.ok(result.worstLatency < 200, JSON.stringify(result));
        console.log('OPFS reload and DataChannel during blocking SQLite owner:', result);

        const order = await page.evaluate(async () => {
            const result = [];
            const first = client
                .pause(150)
                .then(value => result.push(['pause', JSON.parse(value)]));
            await new Promise(resolve => setTimeout(resolve, 20));
            const second = client
                .read('volume')
                .then(value => result.push(['read', JSON.parse(value)]));
            await Promise.all([first, second]);

            return result;
        });
        assert.deepEqual(order, [
            ['pause', 150],
            ['read', { Ok: '0.7' }]
        ]);

        const pressure = await page.evaluate(async () => {
            const occupied = client.occupy(200);
            while (!client.executing()) {
                await new Promise(resolve => setTimeout(resolve, 1));
            }

            let admitted = 0;
            for (let i = 0; i < 200; i++) {
                if (client.abandon('value ' + i)) {
                    admitted++;
                }
            }

            const outstanding = JSON.parse(client.outstanding());

            let finished = false;
            const waiting = client.write('after pressure', 'done').then(value => {
                finished = true;
                return value;
            });
            await new Promise(resolve => setTimeout(resolve, 20));
            const waited = !finished;
            await occupied;
            await waiting;

            return { admitted, outstanding, waited };
        });
        assert.equal(pressure.admitted, 31);
        assert.equal(pressure.outstanding[0], 32);
        assert.equal(pressure.waited, true);

        const clientReady = await page.evaluate(async () => {
            client.begin_shutdown();
            return JSON.parse(await client.finished());
        });
        assert.deepEqual(clientReady, { Ok: null });

        const latest = await page.evaluate(async () => {
            window.client = Client.limited(new URL('./worker.js', location.href).href, 4, 1024);
            await client.ready();
            return JSON.parse(await client.latest_sequence());
        });
        assert.deepEqual(latest, [
            { Ok: '0.9' },
            { Err: { Missing: 'absent' } },
            { Ok: '0.9' },
            true,
            4
        ]);

        const reservation = await page.evaluate(async () => JSON.parse(await client.reservation()));
        assert.equal(reservation[0][0], 1);
        assert.equal(reservation[0][1], 1024);
        assert.equal(reservation[1], true);
        assert.deepEqual(await page.evaluate(async () => JSON.parse(await client.rejection())), [
            { outcome: 'NotAdmitted', cause: { Codec: 'invalid input' }, data: null },
            { Ok: '0.9' }
        ]);
        assert.equal(await page.evaluate(() => client.large()), 8 * 1024 * 1024);

        assert.deepEqual(
            await page.evaluate(async () => {
                const result = JSON.parse(await client.submitted());
                window.client = Client.limited(new URL('./worker.js', location.href).href, 4, 1024);
                await client.ready();
                return result;
            }),
            [true, 0, true]
        );

        const secondTab = await context.newPage();
        await secondTab.goto(process.env.AKTOR_PROOF_URL || 'http://127.0.0.1:8765');
        await secondTab.waitForFunction(() => window.ready);
        const locked = await secondTab.evaluate(() => window.setup);
        assert.equal(locked.Err.outcome, 'NotAdmitted');
        assert.match(locked.Err.cause.Setup, /already open in another tab/);
        await secondTab.close();

        const shutdown = await page.evaluate(async () => {
            const active = client.pause(150);
            while (!client.executing()) {
                await new Promise(resolve => setTimeout(resolve, 1));
            }

            const admitted = client.abandon('shutdown write');
            client.begin_shutdown();
            const result = JSON.parse(await client.finished());
            await active;

            window.client = new Client(new URL('./worker.js', location.href).href);
            await client.ready();
            return [admitted, result, JSON.parse(await client.read('abandoned'))];
        });
        assert.deepEqual(shutdown, [true, { Ok: null }, { Ok: 'shutdown write' }]);

        await page.evaluate(async () => {
            client.begin_shutdown();
            await client.finished();
        });
        const bytes = await page.evaluate(async () => {
            window.client = Client.limited(new URL('./worker.js', location.href).href, 32, 1024);
            await client.ready();
            const occupied = client.occupy(150);
            while (!client.executing()) {
                await new Promise(resolve => setTimeout(resolve, 1));
            }

            let admitted = 0;
            for (let i = 0; i < 100; i++) {
                if (client.abandon('x'.repeat(500))) {
                    admitted++;
                }
            }

            const count = JSON.parse(client.outstanding());
            await occupied;
            client.begin_shutdown();
            await client.finished();

            window.client = new Client(new URL('./worker.js', location.href).href);
            await client.ready();
            const timed = JSON.parse(await client.timed_pause(800, 100));
            const retained = JSON.parse(client.outstanding());
            client.begin_shutdown();
            await client.finished();

            window.client = new Client(new URL('./worker.js', location.href).href);
            await client.ready();
            return { admitted, count, timed, retained };
        });
        assert.equal(bytes.admitted, 1);
        assert.equal(bytes.count[0], 2);
        assert.ok(bytes.count[1] <= 1024);
        assert.deepEqual(bytes.timed, { Err: true });
        assert.equal(bytes.retained[0], 1);

        const dropped = await page.evaluate(async () => {
            const observer = client.observer();
            await client.queue_pause(150);
            const admitted = client.abandon('last owner write');
            client.free();
            const finished = JSON.parse(await observer.finished());
            observer.free();

            window.client = new Client(new URL('./worker.js', location.href).href);
            await client.ready();
            return [admitted, finished, JSON.parse(await client.read('abandoned'))];
        });
        assert.deepEqual(dropped, [true, { Ok: null }, { Ok: 'last owner write' }]);

        // Group checks above exercise owner loss without panicking in the frontend.
        await page.evaluate(async () => {
            client.begin_shutdown();
            await client.finished();
        });
        assert.equal(
            await page.evaluate(() =>
                window.sqliteListenerCheck(new URL('./worker.js', location.href).href)
            ),
            true
        );
        const mismatch = await page.evaluate(async () => {
            const cached = new Client(new URL('./mismatch.js', location.href).href);
            const first = JSON.parse(await cached.ready());
            const retained = JSON.parse(await cached.ready());
            return [first, retained];
        });
        assert.deepEqual(
            mismatch,
            Array(2).fill({ Err: { outcome: 'NotAdmitted', cause: 'Protocol', data: null } })
        );
        console.log(
            engine +
                ': admission, latest, exclusive startup, graceful cleanup, group failure and explicit timeout passed.'
        );
    } finally {
        await browser.close();
    }
})().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
