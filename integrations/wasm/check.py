import sys
import wasmtime

engine = wasmtime.Engine()
module = wasmtime.Module.from_file(engine, sys.argv[1])
assert not module.imports, module.imports

for _ in range(4):
    store = wasmtime.Store(engine)
    instance = wasmtime.Instance(store, module, [])
    for _ in range(8):
        assert instance.exports(store)["aktor_check"](store) == 0

print("Wasmtime: local actor calls and cleanup passed, no host imports.")
