const OPERATORS = {
    '==': (a, b) => a === b,
    '!=': (a, b) => a !== b,
    '>': (a, b) => a > b,
    '>=': (a, b) => a >= b,
    '<': (a, b) => a < b,
    '<=': (a, b) => a <= b,
    'in': (a, b) => Array.isArray(b) && b.includes(a),
    'not-in': (a, b) => Array.isArray(b) && !b.includes(a),
    'contains': (a, b) => typeof a === 'string' && a.includes(b),
    'starts-with': (a, b) => typeof a === 'string' && a.startsWith(b),
    'ends-with': (a, b) => typeof a === 'string' && a.endsWith(b)
};

export class QueryBuilder {
    constructor(db, storeName) {
        this._db = db;
        this._store = storeName;
        this._conditions = [];
        this._orderByField = null;
        this._orderDirection = 'asc';
        this._limitCount = null;
        this._offsetCount = 0;
        this._selectFields = null;
    }

    where(field, operator, value) {
        if (!(operator in OPERATORS)) {
            throw new Error(`Unknown operator: ${operator}. Valid: ${Object.keys(OPERATORS).join(', ')}`);
        }
        this._conditions.push({ field, operator, value, logic: 'and' });
        return this;
    }

    orWhere(field, operator, value) {
        if (!(operator in OPERATORS)) {
            throw new Error(`Unknown operator: ${operator}`);
        }
        this._conditions.push({ field, operator, value, logic: 'or' });
        return this;
    }

    orderBy(field, direction = 'asc') {
        this._orderByField = field;
        this._orderDirection = direction.toLowerCase() === 'desc' ? 'desc' : 'asc';
        return this;
    }

    limit(count) {
        this._limitCount = count;
        return this;
    }

    offset(count) {
        this._offsetCount = count;
        return this;
    }

    select(...fields) {
        this._selectFields = fields.length > 0 ? fields : null;
        return this;
    }

    _canUseFastPath() {
        return this._conditions.length === 0 &&
            this._orderByField === null &&
            this._selectFields === null &&
            this._offsetCount === 0;
    }

    async exec() {
        const prefix = this._store ? `${this._store}:` : '';

        if (this._canUseFastPath()) {
            const limit = this._limitCount || 10000;
            const allData = await this._fetchWithLimit(prefix, limit);
            return allData;
        }

        const needsFullScan = this._orderByField !== null || this._offsetCount > 0;
        const scanLimit = needsFullScan ? 10000 :
            (this._limitCount !== null ? this._limitCount * 3 : 10000);

        const allData = await this._fetchWithLimit(prefix, scanLimit);

        let results = [];
        for (const item of allData) {
            if (this._matchesConditions(item)) {
                results.push(item);
                if (!needsFullScan && this._limitCount !== null && results.length >= this._limitCount) {
                    break;
                }
            }
        }

        if (this._orderByField) {
            results = this._sortResults(results);
        }

        if (this._offsetCount > 0) {
            results = results.slice(this._offsetCount);
        }

        if (this._limitCount !== null) {
            results = results.slice(0, this._limitCount);
        }

        if (this._selectFields) {
            results = results.map(item => this._projectFields(item));
        }

        return results;
    }


    async first() {
        this._limitCount = 1;
        const results = await this.exec();
        return results[0] || null;
    }

    async count() {
        const prefix = this._store ? `${this._store}:` : '';
        const allData = await this._fetchAllFromStore(prefix);
        return allData.filter(item => this._matchesConditions(item)).length;
    }

    async exists() {
        return (await this.count()) > 0;
    }

    async delete() {
        const results = await this.exec();
        let deletedCount = 0;

        for (const item of results) {
            if (item._key) {
                await this._db.delete(item._key);
                deletedCount++;
            }
        }

        return deletedCount;
    }

    async update(updates) {
        const results = await this.exec();
        let updatedCount = 0;

        for (const item of results) {
            if (item._key) {
                const updated = { ...item, ...updates };
                delete updated._key;
                await this._db.put(item._key, JSON.stringify(updated));
                updatedCount++;
            }
        }

        return updatedCount;
    }

    _matchesConditions(item) {
        if (this._conditions.length === 0) return true;

        let result = true;

        for (let i = 0; i < this._conditions.length; i++) {
            const cond = this._conditions[i];
            const fieldValue = this._getNestedValue(item, cond.field);
            const matches = OPERATORS[cond.operator](fieldValue, cond.value);

            if (i === 0) {
                result = matches;
            } else if (cond.logic === 'or') {
                result = result || matches;
            } else {
                result = result && matches;
            }
        }

        return result;
    }

    _getNestedValue(obj, path) {
        const parts = path.split('.');
        let current = obj;

        for (const part of parts) {
            if (current == null) return undefined;
            current = current[part];
        }

        return current;
    }

    _sortResults(results) {
        const field = this._orderByField;
        const dir = this._orderDirection === 'desc' ? -1 : 1;

        return [...results].sort((a, b) => {
            const aVal = this._getNestedValue(a, field);
            const bVal = this._getNestedValue(b, field);

            if (aVal == null && bVal == null) return 0;
            if (aVal == null) return 1;
            if (bVal == null) return -1;

            if (typeof aVal === 'string' && typeof bVal === 'string') {
                return dir * aVal.localeCompare(bVal);
            }

            if (aVal < bVal) return -1 * dir;
            if (aVal > bVal) return 1 * dir;
            return 0;
        });
    }

    _projectFields(item) {
        const projected = {};
        for (const field of this._selectFields) {
            projected[field] = this._getNestedValue(item, field);
        }
        return projected;
    }

    async _fetchAllFromStore(prefix) {
        return this._fetchWithLimit(prefix, 10000);
    }

    async _fetchWithLimit(prefix, limit) {
        const encoder = new TextEncoder();
        const decoder = new TextDecoder();

        const prefixBytes = encoder.encode(prefix);

        const endPrefix = new Uint8Array(prefixBytes);
        let i = endPrefix.length - 1;
        while (i >= 0) {
            if (endPrefix[i] < 255) {
                endPrefix[i]++;
                break;
            }
            endPrefix[i] = 0;
            i--;
        }

        let actualEnd = endPrefix;
        if (i < 0) {
            actualEnd = new Uint8Array([255, 255, 255, 255]);
        }

        const rawResults = await this._db.scanRange(prefixBytes, actualEnd, limit);

        return rawResults.map(({ key, value }) => {
            try {
                const keyStr = decoder.decode(key);
                const valueStr = decoder.decode(value);
                const parsed = JSON.parse(valueStr);
                parsed._key = keyStr;
                return parsed;
            } catch {
                return { _key: decoder.decode(key), _raw: value };
            }
        });
    }
}


export function query(db, store) {
    return new QueryBuilder(db, store);
}
