const DB_VERSION = 1;
const STORE_NAME = 'keyvalue';

export class IndexedDBFallback {
    constructor(dbName) {
        this.dbName = `sikiodb-fallback-${dbName}`;
        this.db = null;
    }

    async open() {
        return new Promise((resolve, reject) => {
            const request = indexedDB.open(this.dbName, DB_VERSION);

            request.onerror = () => reject(request.error);

            request.onupgradeneeded = (event) => {
                const db = event.target.result;
                if (!db.objectStoreNames.contains(STORE_NAME)) {
                    db.createObjectStore(STORE_NAME);
                }
            };

            request.onsuccess = () => {
                this.db = request.result;
                resolve();
            };
        });
    }

    async put(key, value) {
        return this._transaction('readwrite', (store) => {
            return store.put(value, key);
        });
    }

    async get(key) {
        return this._transaction('readonly', (store) => {
            return store.get(key);
        });
    }

    async delete(key) {
        return this._transaction('readwrite', (store) => {
            return store.delete(key);
        });
    }

    async putBatch(entries) {
        return new Promise((resolve, reject) => {
            const tx = this.db.transaction(STORE_NAME, 'readwrite');
            const store = tx.objectStore(STORE_NAME);
            let count = 0;

            tx.oncomplete = () => resolve(count);
            tx.onerror = () => reject(tx.error);

            for (const { key, value } of entries) {
                store.put(value, key);
                count++;
            }
        });
    }

    async getMany(keys) {
        return new Promise((resolve, reject) => {
            const tx = this.db.transaction(STORE_NAME, 'readonly');
            const store = tx.objectStore(STORE_NAME);
            const results = new Array(keys.length).fill(null);
            let completed = 0;

            tx.onerror = () => reject(tx.error);

            keys.forEach((key, index) => {
                const request = store.get(key);
                request.onsuccess = () => {
                    results[index] = request.result ?? null;
                    completed++;
                    if (completed === keys.length) {
                        resolve(results);
                    }
                };
            });

            if (keys.length === 0) {
                resolve([]);
            }
        });
    }

    async deleteMany(keys) {
        return new Promise((resolve, reject) => {
            const tx = this.db.transaction(STORE_NAME, 'readwrite');
            const store = tx.objectStore(STORE_NAME);
            let count = 0;

            tx.oncomplete = () => resolve(count);
            tx.onerror = () => reject(tx.error);

            for (const key of keys) {
                store.delete(key);
                count++;
            }
        });
    }

    async scanRange(startKey, endKey, limit = 1000) {
        return new Promise((resolve, reject) => {
            const tx = this.db.transaction(STORE_NAME, 'readonly');
            const store = tx.objectStore(STORE_NAME);
            const range = IDBKeyRange.bound(startKey, endKey);
            const results = [];

            const request = store.openCursor(range);

            request.onerror = () => reject(request.error);
            request.onsuccess = (event) => {
                const cursor = event.target.result;
                if (cursor && results.length < limit) {
                    results.push({
                        key: cursor.key,
                        value: cursor.value
                    });
                    cursor.continue();
                } else {
                    resolve(results);
                }
            };
        });
    }

    async clear() {
        return this._transaction('readwrite', (store) => {
            return store.clear();
        });
    }

    async getAllKeys() {
        return this._transaction('readonly', (store) => {
            return store.getAllKeys();
        });
    }

    async export() {
        return new Promise((resolve, reject) => {
            const tx = this.db.transaction(STORE_NAME, 'readonly');
            const store = tx.objectStore(STORE_NAME);
            const data = {};

            const request = store.openCursor();

            request.onerror = () => reject(request.error);
            request.onsuccess = (event) => {
                const cursor = event.target.result;
                if (cursor) {
                    data[cursor.key] = cursor.value;
                    cursor.continue();
                } else {
                    resolve(data);
                }
            };
        });
    }

    async import(data) {
        return new Promise((resolve, reject) => {
            const tx = this.db.transaction(STORE_NAME, 'readwrite');
            const store = tx.objectStore(STORE_NAME);

            tx.oncomplete = () => resolve();
            tx.onerror = () => reject(tx.error);

            for (const [key, value] of Object.entries(data)) {
                store.put(value, key);
            }
        });
    }

    _transaction(mode, operation) {
        return new Promise((resolve, reject) => {
            const tx = this.db.transaction(STORE_NAME, mode);
            const store = tx.objectStore(STORE_NAME);
            const request = operation(store);

            request.onerror = () => reject(request.error);
            request.onsuccess = () => resolve(request.result);
        });
    }

    close() {
        if (this.db) {
            this.db.close();
            this.db = null;
        }
    }
}

export function isOPFSAvailable() {
    return typeof navigator !== 'undefined' &&
        'storage' in navigator &&
        'getDirectory' in navigator.storage;
}

export function isIndexedDBAvailable() {
    return typeof indexedDB !== 'undefined';
}
