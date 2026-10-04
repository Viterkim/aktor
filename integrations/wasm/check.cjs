const assert = require('node:assert/strict');
const fs = require('node:fs');

const module_ = new WebAssembly.Module(fs.readFileSync(process.argv[2]));
assert.deepEqual(WebAssembly.Module.imports(module_), []);

for (let i = 0; i < 4; i++) {
    const instance = new WebAssembly.Instance(module_);
    for (let call = 0; call < 8; call++) {
        assert.equal(instance.exports.aktor_check(), 0);
    }
}

console.log('Node: local actor calls and cleanup passed, no host imports.');
