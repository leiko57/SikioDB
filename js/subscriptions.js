const POLL_INTERVAL = 100;

export class Subscription {
    constructor(db, store, options = {}) {
        this._db = db;
        this._store = store;
        this._listeners = new Set();
        this._lastSnapshot = null;
        this._pollTimer = null;
        this._pollInterval = options.pollInterval || POLL_INTERVAL;
        this._channel = null;
        this._isActive = false;
    }

    subscribe(callback) {
        this._listeners.add(callback);

        if (this._listeners.size === 1) {
            this._start();
        }

        if (this._lastSnapshot) {
            callback({ type: 'initial', data: this._lastSnapshot });
        }

        return () => this.unsubscribe(callback);
    }

    unsubscribe(callback) {
        this._listeners.delete(callback);

        if (this._listeners.size === 0) {
            this._stop();
        }
    }

    _start() {
        if (this._isActive) return;
        this._isActive = true;

        this._channel = new BroadcastChannel(`sikiodb-changes-${this._store}`);
        this._channel.onmessage = (e) => this._handleExternalChange(e.data);

        this._startPolling();
    }

    _stop() {
        this._isActive = false;

        if (this._pollTimer) {
            clearInterval(this._pollTimer);
            this._pollTimer = null;
        }

        if (this._channel) {
            this._channel.close();
            this._channel = null;
        }
    }

    _startPolling() {
        this._checkForChanges();

        this._pollTimer = setInterval(() => {
            this._checkForChanges();
        }, this._pollInterval);
    }

    async _checkForChanges() {
        try {
            const currentData = await this._fetchStoreData();
            const changes = this._computeChanges(this._lastSnapshot, currentData);

            if (changes.length > 0) {
                this._lastSnapshot = currentData;
                this._notifyListeners({ type: 'change', changes, data: currentData });
            } else if (!this._lastSnapshot) {
                this._lastSnapshot = currentData;
                this._notifyListeners({ type: 'initial', data: currentData });
            }
        } catch (error) {
            console.error('Subscription poll error:', error);
        }
    }

    async _fetchStoreData() {
        const encoder = new TextEncoder();
        const decoder = new TextDecoder();

        const prefix = `${this._store}:`;
        const prefixBytes = encoder.encode(prefix);
        const endPrefix = new Uint8Array([...prefixBytes.slice(0, -1), prefixBytes[prefixBytes.length - 1] + 1]);

        const rawResults = await this._db.scanRange(prefixBytes, endPrefix, 10000);

        const dataMap = new Map();
        for (const { key, value } of rawResults) {
            const keyStr = decoder.decode(key);
            try {
                dataMap.set(keyStr, JSON.parse(decoder.decode(value)));
            } catch {
                dataMap.set(keyStr, decoder.decode(value));
            }
        }

        return dataMap;
    }

    _computeChanges(oldSnapshot, newSnapshot) {
        const changes = [];
        const oldMap = oldSnapshot || new Map();
        const newMap = newSnapshot || new Map();

        for (const [key, newValue] of newMap) {
            if (!oldMap.has(key)) {
                changes.push({ type: 'add', key, value: newValue });
            } else {
                const oldValue = oldMap.get(key);
                if (JSON.stringify(oldValue) !== JSON.stringify(newValue)) {
                    changes.push({ type: 'update', key, oldValue, value: newValue });
                }
            }
        }

        for (const [key, oldValue] of oldMap) {
            if (!newMap.has(key)) {
                changes.push({ type: 'delete', key, oldValue });
            }
        }

        return changes;
    }

    _handleExternalChange(data) {
        if (data.store === this._store) {
            this._checkForChanges();
        }
    }

    _notifyListeners(event) {
        for (const listener of this._listeners) {
            try {
                listener(event);
            } catch (error) {
                console.error('Subscription listener error:', error);
            }
        }
    }

    notifyChange(changeType, key, value) {
        if (this._channel) {
            this._channel.postMessage({
                store: this._store,
                type: changeType,
                key,
                value
            });
        }
    }
}

export class SubscriptionManager {
    constructor(db) {
        this._db = db;
        this._subscriptions = new Map();
    }

    subscribe(store, callback, options = {}) {
        if (!this._subscriptions.has(store)) {
            this._subscriptions.set(store, new Subscription(this._db, store, options));
        }

        const subscription = this._subscriptions.get(store);
        return subscription.subscribe(callback);
    }

    notifyChange(store, changeType, key, value) {
        const subscription = this._subscriptions.get(store);
        if (subscription) {
            subscription.notifyChange(changeType, key, value);
        }
    }

    destroy() {
        for (const subscription of this._subscriptions.values()) {
            subscription._stop();
        }
        this._subscriptions.clear();
    }
}
