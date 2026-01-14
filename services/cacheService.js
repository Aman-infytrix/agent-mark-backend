const crypto = require('crypto');

class CacheService {
    constructor() {
        this.cache = new Map();
        this.maxSize = 1000;
        this.defaultTTL = 5 * 60 * 1000; // 5 minutes
        this.stats = { hits: 0, misses: 0 };
    }

    generateKey(sql) {
        return crypto.createHash('md5').update(sql.trim().toLowerCase()).digest('hex');
    }

    get(sql) {
        const key = this.generateKey(sql);
        const item = this.cache.get(key);

        if (!item) {
            this.stats.misses++;
            return null;
        }

        if (Date.now() > item.expiresAt) {
            this.cache.delete(key);
            this.stats.misses++;
            return null;
        }

        this.stats.hits++;
        console.log(`Cache HIT for query (${key.slice(0, 8)}...)`);
        return item.value;
    }

    set(sql, value, ttl = this.defaultTTL) {
        const key = this.generateKey(sql);

        if (this.cache.size >= this.maxSize) {
            const firstKey = this.cache.keys().next().value;
            this.cache.delete(firstKey);
        }

        this.cache.set(key, {
            value,
            expiresAt: Date.now() + ttl,
            createdAt: Date.now()
        });

        console.log(`Cached query result (${key.slice(0, 8)}...), TTL: ${ttl / 1000}s`);
    }

    has(sql) {
        const key = this.generateKey(sql);
        const item = this.cache.get(key);
        if (!item) return false;
        if (Date.now() > item.expiresAt) {
            this.cache.delete(key);
            return false;
        }
        return true;
    }

    clear() {
        this.cache.clear();
        console.log('Cache cleared');
    }

    getStats() {
        const total = this.stats.hits + this.stats.misses;
        const hitRate = total > 0 ? ((this.stats.hits / total) * 100).toFixed(2) : 0;
        return {
            size: this.cache.size,
            maxSize: this.maxSize,
            hits: this.stats.hits,
            misses: this.stats.misses,
            hitRate: `${hitRate}%`
        };
    }
}

module.exports = new CacheService();
