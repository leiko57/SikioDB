export type DataInput = Uint8Array | string | number[];

export interface KeyValuePair {
    key: Uint8Array;
    value: Uint8Array;
}

export interface OpenOptions {
    wasmUrl?: string;
    compression?: boolean;
    encryption?: {
        key: string;
    };
}

export interface SubscriptionEvent<T = any> {
    type: 'initial' | 'change';
    data: Map<string, T>;
    changes?: ChangeRecord<T>[];
}

export interface ChangeRecord<T = any> {
    type: 'add' | 'update' | 'delete';
    key: string;
    value?: T;
    oldValue?: T;
}

export interface TransactionContext {
    put(key: DataInput, value: DataInput): void;
    delete(key: DataInput): void;
}

export type Operator =
    | '==' | '!='
    | '>' | '>=' | '<' | '<='
    | 'in' | 'not-in'
    | 'contains' | 'starts-with' | 'ends-with';

export class QueryBuilder<T = any> {
    where(field: string, operator: Operator, value: any): QueryBuilder<T>;
    orWhere(field: string, operator: Operator, value: any): QueryBuilder<T>;
    orderBy(field: string, direction?: 'asc' | 'desc'): QueryBuilder<T>;
    limit(count: number): QueryBuilder<T>;
    offset(count: number): QueryBuilder<T>;
    select(...fields: string[]): QueryBuilder<Partial<T>>;
    exec(): Promise<T[]>;
    first(): Promise<T | null>;
    count(): Promise<number>;
    exists(): Promise<boolean>;
    delete(): Promise<number>;
    update(updates: Partial<T>): Promise<number>;
}

export class SikioDB {
    constructor();

    static open(name: string, options?: OpenOptions): Promise<SikioDB>;

    put(key: DataInput, value: DataInput): Promise<void>;
    putNoSync(key: DataInput, value: DataInput): Promise<void>;
    get(key: DataInput): Promise<Uint8Array | null>;
    delete(key: DataInput): Promise<boolean>;
    putWithTTL(key: DataInput, value: DataInput, ttlMs: number): Promise<void>;

    flush(): Promise<void>;
    putBatch(entries: Array<{ key: string; value: string }>): Promise<number>;

    setMany(entries: Array<{ key: DataInput; value: DataInput }>): Promise<number>;
    getMany(keys: DataInput[]): Promise<(Uint8Array | null)[]>;
    deleteMany(keys: DataInput[]): Promise<number>;

    query<T = any>(store: string): QueryBuilder<T>;

    subscribe<T = any>(
        store: string,
        callback: (event: SubscriptionEvent<T>) => void,
        options?: { pollInterval?: number }
    ): () => void;

    transaction(fn: (tx: TransactionContext) => Promise<void> | void): Promise<boolean>;

    export(): Promise<string>;
    import(data: string | Record<string, any>): Promise<number>;

    verifyIntegrity(): Promise<number[]>;
    scanRange(startKey: DataInput, endKey: DataInput, limit?: number): Promise<KeyValuePair[]>;
    scanRangeStream(startKey: DataInput, endKey: DataInput, batchSize?: number): AsyncIterableIterator<KeyValuePair>;

    close(): Promise<void>;

    readonly isLeader: boolean;
}

export function stringToBytes(str: string): Uint8Array;
export function bytesToString(bytes: Uint8Array): string;

export { QueryBuilder as Query };
export { IndexedDBFallback } from './fallback-storage.js';
export { TabCoordinator } from './tab-coordinator.js';
