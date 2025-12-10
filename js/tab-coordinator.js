const LOCK_NAME = 'sikiodb-leader';
const CHANNEL_NAME = 'sikiodb-sync';
const HEARTBEAT_INTERVAL = 1000;
const LEADER_TIMEOUT = 3000;

export class TabCoordinator {
    constructor(dbName) {
        this.dbName = dbName;
        this.isLeader = false;
        this.channel = null;
        this.pendingRequests = new Map();
        this.requestId = 0;
        this.heartbeatTimer = null;
        this.lastLeaderHeartbeat = 0;
        this.onBecomeLeader = null;
        this.onBecomeFollower = null;
        this.lockController = null;
    }

    async initialize() {
        this.channel = new BroadcastChannel(`${CHANNEL_NAME}-${this.dbName}`);
        this.channel.onmessage = (e) => this._handleMessage(e.data);

        if (!navigator.locks) {
            console.warn('Web Locks API not available, falling back to leader mode');
            await this._becomeLeader();
            return;
        }

        this._tryAcquireLock();
    }

    async _tryAcquireLock() {
        const lockName = `${LOCK_NAME}-${this.dbName}`;

        try {
            await navigator.locks.request(
                lockName,
                { mode: 'exclusive', ifAvailable: true },
                async (lock) => {
                    if (lock) {
                        await this._becomeLeader();
                        return new Promise((resolve) => {
                            this._releaseLock = resolve;
                        });
                    }
                }
            );
        } catch (e) {
            console.warn('Failed to acquire lock:', e);
        }

        if (!this.isLeader) {
            this._becomeFollower();
            this._waitForLeadership();
        }
    }

    async _waitForLeadership() {
        const lockName = `${LOCK_NAME}-${this.dbName}`;

        navigator.locks.request(lockName, { mode: 'exclusive' }, async (lock) => {
            if (lock) {
                await this._becomeLeader();
                return new Promise((resolve) => {
                    this._releaseLock = resolve;
                });
            }
        });
    }

    async _becomeLeader() {
        this.isLeader = true;
        this._startHeartbeat();

        if (this.onBecomeLeader) {
            await this.onBecomeLeader();
        }

        this.channel.postMessage({
            type: 'leader-announce',
            tabId: this._getTabId()
        });
    }

    _becomeFollower() {
        this.isLeader = false;
        this._stopHeartbeat();
        this.lastLeaderHeartbeat = Date.now();

        if (this.onBecomeFollower) {
            this.onBecomeFollower();
        }
    }

    _startHeartbeat() {
        this._stopHeartbeat();
        this.heartbeatTimer = setInterval(() => {
            this.channel.postMessage({
                type: 'heartbeat',
                tabId: this._getTabId(),
                timestamp: Date.now()
            });
        }, HEARTBEAT_INTERVAL);
    }

    _stopHeartbeat() {
        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = null;
        }
    }

    _handleMessage(data) {
        switch (data.type) {
            case 'heartbeat':
            case 'leader-announce':
                if (!this.isLeader) {
                    this.lastLeaderHeartbeat = Date.now();
                }
                break;

            case 'request':
                if (this.isLeader && this.onLeaderRequest) {
                    this._handleLeaderRequest(data);
                }
                break;

            case 'response':
                this._handleResponse(data);
                break;
        }
    }

    async _handleLeaderRequest(data) {
        try {
            const result = await this.onLeaderRequest(data.method, data.args);
            this.channel.postMessage({
                type: 'response',
                requestId: data.requestId,
                tabId: data.tabId,
                result
            });
        } catch (error) {
            this.channel.postMessage({
                type: 'response',
                requestId: data.requestId,
                tabId: data.tabId,
                error: error.message
            });
        }
    }

    _handleResponse(data) {
        if (data.tabId !== this._getTabId()) return;

        const pending = this.pendingRequests.get(data.requestId);
        if (!pending) return;

        this.pendingRequests.delete(data.requestId);

        if (data.error) {
            pending.reject(new Error(data.error));
        } else {
            pending.resolve(data.result);
        }
    }

    proxyRequest(method, args) {
        return new Promise((resolve, reject) => {
            const requestId = ++this.requestId;
            const timeout = setTimeout(() => {
                this.pendingRequests.delete(requestId);
                reject(new Error('Request timeout - leader may be unavailable'));
            }, 10000);

            this.pendingRequests.set(requestId, {
                resolve: (result) => {
                    clearTimeout(timeout);
                    resolve(result);
                },
                reject: (error) => {
                    clearTimeout(timeout);
                    reject(error);
                }
            });

            this.channel.postMessage({
                type: 'request',
                requestId,
                tabId: this._getTabId(),
                method,
                args
            });
        });
    }

    _getTabId() {
        if (!this._tabId) {
            this._tabId = `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
        }
        return this._tabId;
    }

    destroy() {
        this._stopHeartbeat();
        if (this._releaseLock) {
            this._releaseLock();
            this._releaseLock = null;
        }
        if (this.channel) {
            this.channel.close();
            this.channel = null;
        }
        this.pendingRequests.clear();
    }
}
