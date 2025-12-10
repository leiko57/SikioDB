let wasm = null;
let db = null;
self.onmessage = async function (e) {
    const { id, method, args } = e.data;
    try {
        const result = await handleMethod(method, args);
        self.postMessage({ id, result });
    } catch (error) {
        self.postMessage({ id, error: error.message || String(error) });
    }
};
async function handleMethod(method, args) {
    switch (method) {
        case 'init':
            return await initWasm(args.wasmUrl);
        case 'open':
            if (!wasm) {
                throw new Error('WASM not initialized. Call init first.');
            }
            db = await wasm.SikioDB.open(args.name);
            return true;
        case 'put':
            if (!db) throw new Error('Database not opened');
            const keyBytes = new Uint8Array(args.key);
            const valueBytes = new Uint8Array(args.value);
            db.put(keyBytes, valueBytes);
            return true;
        case 'get':
            if (!db) throw new Error('Database not opened');
            const getKeyBytes = new Uint8Array(args.key);
            const result = db.get(getKeyBytes);
            return result ? Array.from(result) : null;
        case 'delete':
            if (!db) throw new Error('Database not opened');
            const delKeyBytes = new Uint8Array(args.key);
            return db.delete(delKeyBytes);
        case 'flush':
            if (!db) throw new Error('Database not opened');
            db.flush();
            return true;
        case 'close':
            if (db) {
                db.close();
                db = null;
            }
            return true;
        case 'putBatch':
            if (!db) throw new Error('Database not opened');
            return db.put_batch(args.data);
        case 'putNoSync':
            if (!db) throw new Error('Database not opened');
            db.putNoSync(new Uint8Array(args.key), new Uint8Array(args.value));
            return true;
        case 'scanPrefix':
            if (!db) throw new Error('Database not opened');
            const prefix = new Uint8Array(args.prefix);
            const results = db.scan_prefix(prefix);
            const pairs = [];
            for (let i = 0; i < results.length; i += 2) {
                pairs.push({
                    key: Array.from(results[i]),
                    value: Array.from(results[i + 1])
                });
            }
            return pairs;
        case 'putWithTTL':
            if (!db) throw new Error('Database not opened');
            db.putWithTTL(new Uint8Array(args.key), new Uint8Array(args.value), BigInt(args.ttl));
            return true;
        case 'verifyIntegrity':
            if (!db) throw new Error('Database not opened');
            return db.verify_integrity();
        case 'scanRange':
            if (!db) throw new Error('Database not opened');
            const startKey = new Uint8Array(args.startKey);
            const endKey = new Uint8Array(args.endKey);
            const limit = args.limit || 1000;
            const scanResults = db.scanRange(startKey, endKey, limit);
            const scanPairs = [];
            for (let i = 0; i < scanResults.length; i++) {
                const entry = scanResults[i];
                scanPairs.push({
                    key: Array.from(entry.key),
                    value: Array.from(entry.value)
                });
            }
            return scanPairs;
        case 'commitTransaction':
            if (!db) throw new Error('Database not opened');
            const txn = db.beginWriteTxn();
            try {
                for (const op of args.ops) {
                    if (op.type === 'put') {
                        txn.put(new Uint8Array(op.key), new Uint8Array(op.value));
                    } else if (op.type === 'delete') {
                        txn.delete(new Uint8Array(op.key));
                    }
                }
                db.commitTxn(txn);
                return true;
            } catch (e) {
                txn.abort();
                throw e;
            }
        default:
            throw new Error(`Unknown method: ${method}`);
    }
}
async function initWasm(wasmUrl) {
    const wasmModule = await import(wasmUrl.replace('.wasm', '.js'));
    await wasmModule.default();
    wasm = wasmModule;
    return true;
}
