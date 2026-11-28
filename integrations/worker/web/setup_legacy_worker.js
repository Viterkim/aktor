// An old bootstrap announces Ready without waiting for the page's config.
postMessage(new Uint8Array([1, 2, 40, 41, 0, 0, 0, 32, 64, 0]).buffer);
