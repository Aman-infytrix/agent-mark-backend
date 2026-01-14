/**
 * Script to gather all schema information from Trino and update the knowledgebase.json
 * Run with: node scripts/generateKnowledgeBase.js
 */

require("dotenv").config();
const { Trino, BasicAuth } = require("trino-client");
const fs = require("fs");
const path = require("path");

const TRINO_SERVER = process.env.TRINO_SERVER;
const TRINO_USER = process.env.TRINO_USER;
const TRINO_CATALOGS = process.env.TRINO_CATALOGS || "lakehouse.ap_south1_gold";

async function getClient(catalog, schema) {
    return Trino.create({
        server: TRINO_SERVER,
        catalog: catalog,
        schema: schema,
        auth: new BasicAuth(TRINO_USER),
    });
}

async function executeQuery(client, sql) {
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

    return { columns, rows };
}

async function getTables(client, catalog, schema) {
    const result = await executeQuery(client, `SHOW TABLES FROM ${catalog}.${schema}`);
    return result.rows.map((row) => row[0]);
}

async function getTableSchema(client, catalog, schema, table) {
    const result = await executeQuery(client, `DESCRIBE ${catalog}.${schema}.${table}`);
    return result.rows.map((row) => ({
        column: row[0],
        type: row[1],
        extra: row[2] || "",
        comment: row[3] || "",
    }));
}

async function getSampleValues(client, catalog, schema, table, column, limit = 10) {
    try {
        const sql = `SELECT DISTINCT ${column} FROM ${catalog}.${schema}.${table} WHERE ${column} IS NOT NULL LIMIT ${limit}`;
        const result = await executeQuery(client, sql);
        return result.rows.map((row) => row[0]).filter(v => v !== null);
    } catch (error) {
        console.warn(`  Could not get sample values for ${column}: ${error.message}`);
        return [];
    }
}

async function generateKnowledgeBase() {
    console.log("🚀 Starting knowledge base generation from Trino...\n");

    const catalogSchemas = TRINO_CATALOGS.split(",").map((cs) => {
        const [catalog, schema] = cs.trim().split(".");
        return { catalog, schema };
    });

    const knowledgeBase = {
        version: "1.0",
        lastUpdated: new Date().toISOString().split("T")[0],
        description: "Auto-generated knowledge base from Trino database schema",
        catalogs: {},
        commonPatterns: {
            brandFilter: {
                description: "How to filter by brand name",
                pattern: "JOIN database.global.t_master_brand b ON CAST(a.master_brand_id AS uuid) = b.master_brand_id WHERE LOWER(b.brand_name) = 'brandname'"
            },
            platformFilter: {
                description: "How to filter by platform/marketplace",
                pattern: "JOIN database.global.t_master_platform p ON CAST(a.master_platform_id AS uuid) = p.master_platform_id WHERE LOWER(p.platform_name) = 'amazon'"
            },
            monthComparison: {
                description: "Compare same month across years",
                pattern: "WHERE EXTRACT(MONTH FROM date) = 1 AND EXTRACT(YEAR FROM date) IN (2025, 2026) GROUP BY EXTRACT(YEAR FROM date)"
            },
            categoryFilter: {
                description: "Filter by category (case-insensitive)",
                pattern: "WHERE LOWER(category) = 'men'"
            }
        },
        entityRecognition: {
            brands: {
                description: "Company/brand names - stored in t_master_brand",
                examples: [],
                identifiers: ["Usually company names without codes or numbers"]
            },
            products: {
                description: "Product SKUs/names - stored in product_name column",
                examples: [],
                identifiers: ["Often contain codes, numbers, or technical identifiers"]
            },
            categories: {
                description: "Product categories",
                values: []
            }
        },
        typeCasting: {
            critical: true,
            description: "master_brand_id and master_platform_id are VARCHAR in lakehouse but UUID in database tables",
            solution: "Always use CAST(column AS uuid) when joining cross-catalog"
        }
    };

    for (const { catalog, schema } of catalogSchemas) {
        console.log(`📂 Processing catalog: ${catalog}.${schema}`);

        try {
            const client = await getClient(catalog, schema);
            const tables = await getTables(client, catalog, schema);

            console.log(`  Found ${tables.length} tables`);

            const catalogKey = `${catalog}.${schema}`;
            knowledgeBase.catalogs[catalogKey] = {
                description: `Tables from ${catalogKey}`,
                tables: {}
            };

            for (const table of tables) {
                console.log(`  📊 Processing table: ${table}`);

                try {
                    const columns = await getTableSchema(client, catalog, schema, table);

                    const tableInfo = {
                        description: `Table ${table}`,
                        columns: {}
                    };

                    for (const col of columns) {
                        tableInfo.columns[col.column] = {
                            type: col.type,
                            description: col.comment || `Column ${col.column}`
                        };

                        // Get sample values for important columns
                        if (["category", "sub_category", "brand_name", "platform_name"].includes(col.column.toLowerCase())) {
                            const samples = await getSampleValues(client, catalog, schema, table, col.column, 20);
                            if (samples.length > 0) {
                                tableInfo.columns[col.column].knownValues = samples;

                                // Populate entity recognition
                                if (col.column.toLowerCase() === "category") {
                                    knowledgeBase.entityRecognition.categories.values = [...new Set([...knowledgeBase.entityRecognition.categories.values, ...samples])];
                                }
                                if (col.column.toLowerCase() === "brand_name") {
                                    knowledgeBase.entityRecognition.brands.examples = [...new Set([...knowledgeBase.entityRecognition.brands.examples, ...samples])].slice(0, 10);
                                }
                            }
                        }

                        // Get sample product names
                        if (col.column.toLowerCase() === "product_name") {
                            const samples = await getSampleValues(client, catalog, schema, table, col.column, 10);
                            if (samples.length > 0) {
                                tableInfo.columns[col.column].examples = samples;
                                knowledgeBase.entityRecognition.products.examples = [...new Set([...knowledgeBase.entityRecognition.products.examples, ...samples])].slice(0, 10);
                            }
                        }
                    }

                    knowledgeBase.catalogs[catalogKey].tables[table] = tableInfo;
                } catch (tableError) {
                    console.error(`  ❌ Error processing table ${table}: ${tableError.message}`);
                }
            }
        } catch (catalogError) {
            console.error(`❌ Error processing catalog ${catalog}.${schema}: ${catalogError.message}`);
        }
    }

    // Write the knowledge base to file
    const outputPath = path.join(__dirname, "..", "knowledgebase.json");
    fs.writeFileSync(outputPath, JSON.stringify(knowledgeBase, null, 2));

    console.log(`\n✅ Knowledge base generated successfully!`);
    console.log(`📁 Output: ${outputPath}`);
    console.log(`📊 Total catalogs: ${Object.keys(knowledgeBase.catalogs).length}`);

    let totalTables = 0;
    for (const catalog of Object.values(knowledgeBase.catalogs)) {
        totalTables += Object.keys(catalog.tables).length;
    }
    console.log(`📊 Total tables: ${totalTables}`);
}

generateKnowledgeBase().catch(console.error);
