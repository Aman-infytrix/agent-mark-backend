const { Trino, BasicAuth } = require("trino-client");
const cacheService = require("./cacheService");

class TrinoService {
    constructor() {
        this.serverUrl = process.env.TRINO_SERVER;
        this.user = process.env.TRINO_USER;
        this.schemaCache = null;
        this.clientPool = new Map();
        this.maxPoolSize = 10;
        this.catalogSchemas = this.parseCatalogs();
    }

    parseCatalogs() {
        const catalogsEnv = process.env.TRINO_CATALOGS || 'lakehouse.ap_south1_gold';
        return catalogsEnv.split(',').map(cs => {
            const [catalog, schema] = cs.trim().split('.');
            return { catalog, schema };
        });
    }

    async getClient(catalog, schema) {
        const poolKey = `${catalog}.${schema}`;
        if (this.clientPool.has(poolKey)) {
            console.log(`Reusing pooled connection for ${poolKey}`);
            return this.clientPool.get(poolKey);
        }

        console.log(`Creating new connection for ${poolKey}`);
        const client = Trino.create({
            server: this.serverUrl,
            catalog: catalog,
            schema: schema,
            auth: new BasicAuth(this.user),
        });

        if (this.clientPool.size < this.maxPoolSize) {
            this.clientPool.set(poolKey, client);
        }
        return client;
    }

    validateReadOnly(sql) {
        const forbiddenPatterns = [
            /^\s*INSERT\s/i, /^\s*UPDATE\s/i, /^\s*DELETE\s/i,
            /^\s*DROP\s/i, /^\s*CREATE\s/i, /^\s*ALTER\s/i,
            /^\s*TRUNCATE\s/i, /^\s*GRANT\s/i, /^\s*REVOKE\s/i, /^\s*MERGE\s/i,
        ];

        for (const pattern of forbiddenPatterns) {
            if (pattern.test(sql)) {
                throw new Error("Only SELECT queries are allowed. Write operations are forbidden.");
            }
        }

        const allowedPatterns = [
            /^\s*SELECT\s/i, /^\s*WITH\s/i, /^\s*SHOW\s/i,
            /^\s*DESCRIBE\s/i, /^\s*EXPLAIN\s/i,
        ];

        if (!allowedPatterns.some(pattern => pattern.test(sql))) {
            throw new Error("Only SELECT, SHOW, DESCRIBE, and EXPLAIN queries are allowed.");
        }
        return true;
    }

    async executeQuery(sql, useCache = true) {
        this.validateReadOnly(sql);

        if (useCache) {
            const cached = cacheService.get(sql);
            if (cached) {
                return { ...cached, fromCache: true };
            }
        }

        const { catalog, schema } = this.catalogSchemas[0];
        const client = await this.getClient(catalog, schema);
        const startTime = Date.now();
        const iter = await client.query(sql);

        const columns = [];
        const rows = [];

        for await (const chunk of iter) {
            if (columns.length === 0 && chunk.columns) {
                chunk.columns.forEach((col) => columns.push(col.name));
            }
            if (chunk.data) {
                rows.push(...chunk.data);
            }
        }

        const result = { columns, rows };
        const duration = Date.now() - startTime;
        console.log(`Query executed in ${duration}ms, ${rows.length} rows`);

        if (useCache) {
            cacheService.set(sql, result);
        }

        return { ...result, fromCache: false };
    }

    async getTablesFromCatalog(catalog, schema) {
        const sql = `SHOW TABLES FROM ${catalog}.${schema}`;
        const result = await this.executeQuery(sql);
        return result.rows.map((row) => ({
            table: row[0], catalog, schema,
            fullName: `${catalog}.${schema}.${row[0]}`
        }));
    }

    async getTables() {
        const allTables = [];
        for (const { catalog, schema } of this.catalogSchemas) {
            try {
                const tables = await this.getTablesFromCatalog(catalog, schema);
                allTables.push(...tables);
            } catch (error) {
                console.error(`Error getting tables from ${catalog}.${schema}:`, error.message);
            }
        }
        return allTables;
    }

    async getTableSchema(catalog, schema, tableName) {
        const sql = `DESCRIBE ${catalog}.${schema}.${tableName}`;
        const result = await this.executeQuery(sql);
        return result.rows.map((row) => ({
            column: row[0], type: row[1],
            extra: row[2] || "", comment: row[3] || "",
        }));
    }

    async getFullSchema() {
        if (this.schemaCache) return this.schemaCache;

        const schema = {};
        const tables = await this.getTables();

        for (const tableInfo of tables) {
            try {
                const columns = await this.getTableSchema(tableInfo.catalog, tableInfo.schema, tableInfo.table);
                schema[tableInfo.fullName] = { ...tableInfo, columns };
            } catch (error) {
                console.error(`Error getting schema for ${tableInfo.fullName}:`, error.message);
                schema[tableInfo.fullName] = { ...tableInfo, columns: [] };
            }
        }

        this.schemaCache = schema;
        return schema;
    }

    formatSchemaForPrompt() {
        if (!this.schemaCache) return "Schema not loaded yet.";

        let prompt = `Available Catalogs and Tables:\n`;
        const byCatalog = {};

        for (const [fullName, info] of Object.entries(this.schemaCache)) {
            const key = `${info.catalog}.${info.schema}`;
            if (!byCatalog[key]) byCatalog[key] = [];
            byCatalog[key].push({ fullName, ...info });
        }

        for (const [catalogSchema, tables] of Object.entries(byCatalog)) {
            prompt += `\n## Catalog: ${catalogSchema}\n`;
            for (const tableInfo of tables) {
                prompt += `\n### ${tableInfo.fullName}\n`;
                if (tableInfo.columns.length === 0) {
                    prompt += "  (unable to retrieve columns)\n";
                } else {
                    tableInfo.columns.forEach((col) => {
                        prompt += `  - ${col.column}: ${col.type}`;
                        if (col.comment) prompt += ` -- ${col.comment}`;
                        prompt += "\n";
                    });
                }
            }
        }

        prompt += `\nIMPORTANT: Always use fully qualified table names (catalog.schema.table) in queries.`;
        prompt += `\nTables from different catalogs can be JOINed together in the same query.`;
        return prompt;
    }

    async refreshSchema() {
        this.schemaCache = null;
        return await this.getFullSchema();
    }

    getCatalogsInfo() { return this.catalogSchemas; }
    getCacheStats() { return cacheService.getStats(); }
    clearCache() { cacheService.clear(); }
}

module.exports = new TrinoService();
