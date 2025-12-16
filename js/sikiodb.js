import { TabCoordinator } from './tab-coordinator.js';
import { IndexedDBFallback, isOPFSAvailable } from './fallback-storage.js';
import { QueryBuilder } from './query-builder.js';
import { SubscriptionManager } from './subscriptions.js';

export class SikioDB {
    constructor() {
        this.worker = null;
        this.pendingCalls = new Map();
        this.callId = 0;
        this._coordinator = null;
        this._fallback = null;
        this._subscriptions = null;
        this._isLeader = false;
        this._dbName = null;
    }
    static async open(name, options = {}) {
        const instance = new SikioDB();
        instance._dbName = name;

        instance._coordinator = new TabCoordinator(name);

        instance._coordinator.onBecomeLeader = async () => {
            instance._isLeader = true;

            if (isOPFSAvailable()) {
                try {
                    await instance._initWorker(options.wasmUrl || '../pkg/sikiodb.js');
                    await instance._call('open', {
                        name,
                        compression: options.compression || false,
                        encryptionKey: options.encryption?.key || null
                    });
                } catch (e) {
                    console.warn('OPFS unavailable, falling back to IndexedDB:', e);
                    if (instance.worker) {
                        instance.worker.terminate();
                        instance.worker = null;
                    }
                    instance.pendingCalls.clear();
                    instance._fallback = new IndexedDBFallback(name);
                    await instance._fallback.open();
                }
            } else {
                instance._fallback = new IndexedDBFallback(name);
                await instance._fallback.open();
            }
        };

        instance._coordinator.onBecomeFollower = () => {
            instance._isLeader = false;
        };

        instance._coordinator.onLeaderRequest = async (method, args) => {
            return instance._executeMethod(method, args);
        };

        await instance._coordinator.initialize();

        if (!instance._isLeader) {
            await new Promise(resolve => setTimeout(resolve, 100));
        }

        instance._subscriptions = new SubscriptionManager(instance);

        return instance;
    }
    async _initWorker(wasmUrl) {
        const workerUrl = new URL('./worker.js?v=' + Date.now(), import.meta.url);
        this.worker = new Worker(workerUrl, { type: 'module' });
        this.worker.onmessage = (e) => {
            const { id, result, error } = e.data;
            const pending = this.pendingCalls.get(id);
            if (pending) {
                this.pendingCalls.delete(id);
                if (error) {
                    pending.reject(new Error(error));
                } else {
                    pending.resolve(result);
                }
            }
        };
        this.worker.onerror = (e) => {
            console.error('SikioDB Worker error:', e);
            const errorMsg = e.message || 'Worker crashed';
            for (const [id, pending] of this.pendingCalls) {
                pending.reject(new Error(errorMsg));
            }
            this.pendingCalls.clear();
        };
        await this._call('init', { wasmUrl });
    }
    _call(method, args = {}, transfer = []) {
        return new Promise((resolve, reject) => {
            const id = ++this.callId;
            this.pendingCalls.set(id, { resolve, reject });
            this.worker.postMessage({ id, method, args }, transfer);
        });
    }

    _toFallbackKey(data) {
        const bytes = this._toArray(data);
        return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
    }

    _wrapRawValue(valueBytes) {
        const wrapped = new Uint8Array(1 + valueBytes.length);
        wrapped[0] = 0;
        wrapped.set(valueBytes, 1);
        return wrapped;
    }

    _wrapTtlValue(valueBytes, ttlMs) {
        const expiry = BigInt(Date.now()) + BigInt(ttlMs);
        const wrapped = new Uint8Array(1 + 8 + valueBytes.length);
        wrapped[0] = 1;
        new DataView(wrapped.buffer, wrapped.byteOffset, wrapped.byteLength).setBigUint64(1, expiry, true);
        wrapped.set(valueBytes, 9);
        return wrapped;
    }

    _unwrapStoredValue(storedBytes) {
        if (!storedBytes || storedBytes.length === 0) {
            return null;
        }

        const valueType = storedBytes[0];
        if (valueType === 0) {
            return storedBytes.slice(1);
        }

        if (valueType === 1) {
            if (storedBytes.length < 9) {
                return null;
            }

            const view = new DataView(storedBytes.buffer, storedBytes.byteOffset, storedBytes.byteLength);
            const expiry = view.getBigUint64(1, true);
            if (BigInt(Date.now()) > expiry) {
                return null;
            }

            return storedBytes.slice(9);
        }

        return storedBytes.slice(1);
    }

    async put(key, value) {
        if (!this._isLeader && !this._fallback) {
            return this._coordinator.proxyRequest('put', { key, value });
        }

        if (this._fallback) {
            const keyIdb = this._toFallbackKey(key);
            const userBytes = this._toArray(value);
            const wrapped = this._wrapRawValue(userBytes);
            await this._fallback.put(keyIdb, wrapped);
            return;
        }

        const keyArray = this._toArray(key);
        const valueArray = this._toArray(value);
        return this._call('put', { key: keyArray, value: valueArray });
    }
    async putNoSync(key, value) {
        if (!this._isLeader && !this._fallback) {
            return this._coordinator.proxyRequest('putNoSync', { key, value });
        }

        if (this._fallback) {
            return this.put(key, value);
        }

        const keyArray = this._toArray(key);
        const valueArray = this._toArray(value);
        return this._call('putNoSync', { key: keyArray, value: valueArray });
    }
    async get(key) {
        if (!this._isLeader && !this._fallback) {
            const result = await this._coordinator.proxyRequest('get', { key });
            return result ? new Uint8Array(result) : null;
        }

        if (this._fallback) {
            const keyIdb = this._toFallbackKey(key);
            const stored = await this._fallback.get(keyIdb);
            if (!stored) {
                return null;
            }
            const storedBytes = stored instanceof Uint8Array ? stored : new Uint8Array(stored);
            return this._unwrapStoredValue(storedBytes);
        }

        const keyArray = this._toArray(key);
        const result = await this._call('get', { key: keyArray });
        return result ? new Uint8Array(result) : null;
    }
    async delete(key) {
        if (!this._isLeader && !this._fallback) {
            return this._coordinator.proxyRequest('delete', { key });
        }

        if (this._fallback) {
            const keyIdb = this._toFallbackKey(key);
            const existing = await this._fallback.get(keyIdb);
            if (!existing) {
                return false;
            }
            const storedBytes = existing instanceof Uint8Array ? existing : new Uint8Array(existing);
            const userValue = this._unwrapStoredValue(storedBytes);
            await this._fallback.delete(keyIdb);
            return userValue !== null;
        }

        const keyArray = this._toArray(key);
        return this._call('delete', { key: keyArray });
    }
    async putWithTTL(key, value, ttlMs) {
        if (!this._isLeader && !this._fallback) {
            return this._coordinator.proxyRequest('putWithTTL', { key, value, ttlMs });
        }

        if (this._fallback) {
            const keyIdb = this._toFallbackKey(key);
            const userBytes = this._toArray(value);
            const wrapped = this._wrapTtlValue(userBytes, ttlMs);
            await this._fallback.put(keyIdb, wrapped);
            return;
        }

        const keyArray = this._toArray(key);
        const valArray = this._toArray(value);
        return this._call('putWithTTL', { key: keyArray, value: valArray, ttl: ttlMs });
    }
    async flush() {
        if (!this._isLeader && !this._fallback) {
            return this._coordinator.proxyRequest('flush', {});
        }

        if (this._fallback) {
            return;
        }

        return this._call('flush');
    }
    async putBatch(entries) {
        if (!this._isLeader && !this._fallback) {
            return this._coordinator.proxyRequest('putBatch', { entries });
        }
        if (!Array.isArray(entries)) {
            throw new Error('putBatch expects an array of entries');
        }
        if (entries.length === 0) {
            return 0;
        }

        if (this._fallback) {
            const encoder = new TextEncoder();
            const converted = entries.map((e) => {
                if (!e || typeof e.key !== 'string' || typeof e.value !== 'string') {
                    throw new Error('Each entry must have string key and value properties');
                }
                const keyBytes = encoder.encode(e.key);
                const valueBytes = encoder.encode(e.value);
                return {
                    key: this._toFallbackKey(keyBytes),
                    value: this._wrapRawValue(valueBytes)
                };
            });
            return this._fallback.putBatch(converted);
        }

        const encoder = new TextEncoder();
        const estimatedSize = entries.reduce((acc, e) => {
            const keyLen = typeof e.key === 'string' ? e.key.length * 3 : 0;
            const valLen = typeof e.value === 'string' ? e.value.length * 3 : 0;
            return acc + keyLen + valLen + 8;
        }, 0);
        const initialSize = Math.max(1024, estimatedSize);
        let buffer = new Uint8Array(initialSize);
        let view = new DataView(buffer.buffer);
        let offset = 0;
        for (const e of entries) {
            if (!e || typeof e.key !== 'string' || typeof e.value !== 'string') {
                throw new Error('Each entry must have string key and value properties');
            }
            const keyStr = e.key;
            const valStr = e.value;
            if (offset + keyStr.length * 3 + valStr.length * 3 + 8 > buffer.length) {
                const newBuffer = new Uint8Array(buffer.length * 2);
                newBuffer.set(buffer);
                buffer = newBuffer;
                view = new DataView(buffer.buffer);
            }
            const keyLenOffset = offset;
            offset += 4;
            const keyResult = encoder.encodeInto(keyStr, buffer.subarray(offset));
            const keyWritten = keyResult.written;
            view.setUint32(keyLenOffset, keyWritten, true);
            offset += keyWritten;
            const valLenOffset = offset;
            offset += 4;
            const valResult = encoder.encodeInto(valStr, buffer.subarray(offset));
            const valWritten = valResult.written;
            view.setUint32(valLenOffset, valWritten, true);
            offset += valWritten;
        }
        const exactData = buffer.slice(0, offset);
        return this._call('putBatch', { data: exactData }, [exactData.buffer]);
    }
    async scanRange(startKey, endKey, limit = 1000) {
        if (!this._isLeader && !this._fallback) {
            const result = await this._coordinator.proxyRequest('scanRange', { startKey, endKey, limit });
            return result.map(entry => {
                const keyBytes = entry.key instanceof Uint8Array ? entry.key : new Uint8Array(Object.values(entry.key));
                const valueBytes = entry.value instanceof Uint8Array ? entry.value : new Uint8Array(Object.values(entry.value));
                return { key: keyBytes, value: valueBytes };
            });
        }

        if (this._fallback) {
            const startKeyIdb = this._toFallbackKey(startKey);
            const endKeyIdb = this._toFallbackKey(endKey);
            const rawLimit = Math.min(10000, Math.max(1, limit) * 10);
            const rawResults = await this._fallback.scanRange(startKeyIdb, endKeyIdb, rawLimit);

            const results = [];
            for (const { key, value } of rawResults) {
                const keyBytes = typeof key === 'string' ? new TextEncoder().encode(key) : new Uint8Array(key);
                const storedBytes = value instanceof Uint8Array ? value : new Uint8Array(value);
                const userValue = this._unwrapStoredValue(storedBytes);
                if (userValue) {
                    results.push({ key: keyBytes, value: userValue });
                    if (results.length >= limit) {
                        break;
                    }
                }
            }
            return results;
        }

        const startArray = this._toArray(startKey);
        const endArray = this._toArray(endKey);
        const results = await this._call('scanRange', {
            startKey: startArray,
            endKey: endArray,
            limit
        });
        return results.map(entry => ({
            key: new Uint8Array(entry.key),
            value: new Uint8Array(entry.value)
        }));
    }
    async *scanRangeStream(startKey, endKey, batchSize = 100) {
        if (!this._isLeader && !this._fallback) {
            const all = await this.scanRange(startKey, endKey, 10000);
            for (const item of all) yield item;
            return;
        }

        if (this._fallback) {
            const endArray = this._toArray(endKey);
            let currentStart = this._toArray(startKey);
            while (true) {
                const batch = await this.scanRange(currentStart, endArray, batchSize + 1);
                if (batch.length === 0) {
                    break;
                }
                const hasMore = batch.length > batchSize;
                const toYield = hasMore ? batch.slice(0, batchSize) : batch;
                for (const entry of toYield) {
                    yield entry;
                }
                if (!hasMore) {
                    break;
                }
                const lastKey = batch[batchSize].key;
                currentStart = new Uint8Array([...lastKey, 0]);
            }
            return;
        }

        const startArray = this._toArray(startKey);
        let currentStart = startArray;
        const endArray = this._toArray(endKey);
        while (true) {
            const batch = await this._call('scanRange', {
                startKey: currentStart,
                endKey: endArray,
                limit: batchSize + 1
            });
            if (batch.length === 0) {
                break;
            }
            const hasMore = batch.length > batchSize;
            const toYield = hasMore ? batch.slice(0, batchSize) : batch;
            for (const entry of toYield) {
                yield {
                    key: new Uint8Array(entry.key),
                    value: new Uint8Array(entry.value)
                };
            }
            if (!hasMore) {
                break;
            }
            const lastKey = batch[batchSize].key;
            currentStart = new Uint8Array([...lastKey, 0]);
        }
    }
    async verifyIntegrity() {
        if (!this._isLeader && !this._fallback) {
            return this._coordinator.proxyRequest('verifyIntegrity', {});
        }

        if (this._fallback) {
            return [];
        }

        return this._call('verifyIntegrity');
    }
    async close() {
        if (this._isLeader && !this._fallback) {
            await this._call('close');
        }
        if (this.worker) {
            this.worker.terminate();
            this.worker = null;
        }
        if (this._coordinator) {
            this._coordinator.destroy();
            this._coordinator = null;
        }
        if (this._fallback) {
            this._fallback.close();
            this._fallback = null;
        }
        this._isLeader = false;
    }
    _toArray(data) {
        if (data instanceof Uint8Array) {
            return data;
        }
        if (typeof data === 'string') {
            return new TextEncoder().encode(data);
        }
        if (Array.isArray(data)) {
            return new Uint8Array(data);
        }
        throw new Error('Invalid data type. Expected Uint8Array, string, or number array.');
    }

    async _executeMethod(method, args) {
        switch (method) {
            case 'put':
                return this.put(args.key, args.value);
            case 'putNoSync':
                return this.putNoSync(args.key, args.value);
            case 'get':
                return this.get(args.key);
            case 'delete':
                return this.delete(args.key);
            case 'putWithTTL':
                return this.putWithTTL(args.key, args.value, args.ttlMs ?? args.ttl);
            case 'flush':
                return this.flush();
            case 'putBatch':
                return this.putBatch(args.entries);
            case 'scanRange':
                return this.scanRange(args.startKey, args.endKey, args.limit);
            case 'verifyIntegrity':
                return this.verifyIntegrity();
            case 'commitTransaction':
                return this._commitTransaction(args.ops);
            case 'setMany':
                return this.setMany(args.entries);
            default:
                if (this._fallback) {
                    return this._executeFallbackMethod(method, args);
                }
                return this._call(method, args);
        }
    }

    async _executeFallbackMethod(method, args) {
        switch (method) {
            default:
                throw new Error(`Fallback does not support method: ${method}`);
        }
    }

    query(store) {
        return new QueryBuilder(this, store);
    }

    subscribe(store, callback, options = {}) {
        if (!this._subscriptions) {
            throw new Error('Database not initialized');
        }
        return this._subscriptions.subscribe(store, callback, options);
    }

    async setMany(entries) {
        if (!Array.isArray(entries)) {
            throw new Error('setMany expects an array of { key, value } objects');
        }

        if (this._isLeader || this._fallback) {
            const encoder = new TextEncoder();
            for (const { key, value } of entries) {
                const keyBytes = typeof key === 'string' ? encoder.encode(key) : key;
                const valueBytes = typeof value === 'string' ? encoder.encode(value) : value;
                await this.put(keyBytes, valueBytes);
            }
            return entries.length;
        }

        return this._coordinator.proxyRequest('setMany', { entries });
    }

    async getMany(keys) {
        if (!Array.isArray(keys)) {
            throw new Error('getMany expects an array of keys');
        }

        const results = await Promise.all(keys.map(key => this.get(key)));
        return results;
    }

    async deleteMany(keys) {
        if (!Array.isArray(keys)) {
            throw new Error('deleteMany expects an array of keys');
        }

        let count = 0;
        for (const key of keys) {
            const deleted = await this.delete(key);
            if (deleted) count++;
        }
        return count;
    }

    async transaction(fn) {
        const encoder = new TextEncoder();
        const ops = [];

        const tx = {
            put: (key, value) => {
                ops.push({
                    type: 'put',
                    key: typeof key === 'string' ? Array.from(encoder.encode(key)) : Array.from(key),
                    value: typeof value === 'string' ? Array.from(encoder.encode(value)) : Array.from(value)
                });
            },
            delete: (key) => {
                ops.push({
                    type: 'delete',
                    key: typeof key === 'string' ? Array.from(encoder.encode(key)) : Array.from(key)
                });
            }
        };

        await fn(tx);

        if (ops.length === 0) return true;

        return this._commitTransaction(ops);
    }

    async _commitTransaction(ops) {
        if (!this._isLeader && !this._fallback) {
            await this._coordinator.proxyRequest('commitTransaction', { ops });
            return true;
        }

        if (this._fallback) {
            const idbOps = ops.map((op) => {
                const keyBytes = new Uint8Array(op.key);
                const key = this._toFallbackKey(keyBytes);

                if (op.type === 'put') {
                    const valueBytes = new Uint8Array(op.value);
                    return { type: 'put', key, value: this._wrapRawValue(valueBytes) };
                }

                return { type: 'delete', key };
            });
            await this._fallback.transaction(idbOps);
            return true;
        }

        await this._call('commitTransaction', { ops });
        return true;
    }



    async export() {
        const allData = {};
        const decoder = new TextDecoder();
        const prefix = new Uint8Array([0]);
        const end = new Uint8Array([255, 255, 255, 255]);

        const results = await this.scanRange(prefix, end, 100000);
        for (const { key, value } of results) {
            const keyStr = decoder.decode(key);
            try {
                allData[keyStr] = JSON.parse(decoder.decode(value));
            } catch {
                allData[keyStr] = Array.from(value);
            }
        }

        return JSON.stringify(allData);
    }

    async import(data) {
        const parsed = typeof data === 'string' ? JSON.parse(data) : data;
        const encoder = new TextEncoder();

        for (const [key, value] of Object.entries(parsed)) {
            const keyBytes = encoder.encode(key);
            const valueBytes = Array.isArray(value)
                ? new Uint8Array(value)
                : encoder.encode(JSON.stringify(value));
            await this.put(keyBytes, valueBytes);
        }

        return Object.keys(parsed).length;
    }

    get isLeader() {
        return this._isLeader;
    }
}
export function stringToBytes(str) {
    return new TextEncoder().encode(str);
}
export function bytesToString(bytes) {
    return new TextDecoder().decode(bytes);
}
