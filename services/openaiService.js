const OpenAI = require("openai");
const fs = require("fs");
const path = require("path");

class OpenAIService {
    constructor() {
        this.client = null;
        this.trinoService = null;
        this.knowledgeBase = null;
        this.tableAccess = null;
    }

    initialize(trinoService) {
        this.client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
        this.trinoService = trinoService;
        this.loadKnowledgeBase();
        this.loadTableAccess();
    }

    loadKnowledgeBase() {
        try {
            const kbPath = path.join(__dirname, "..", "knowledgebase.json");
            const kbContent = fs.readFileSync(kbPath, "utf-8");
            this.knowledgeBase = JSON.parse(kbContent);
            console.log("Knowledge base loaded successfully");
        } catch (error) {
            console.warn("Could not load knowledge base:", error.message);
            this.knowledgeBase = null;
        }
    }

    loadTableAccess() {
        try {
            const taPath = path.join(__dirname, "..", "tableAccess.json");
            const taContent = fs.readFileSync(taPath, "utf-8");
            this.tableAccess = JSON.parse(taContent);
            console.log("Table access config loaded successfully");
        } catch (error) {
            console.warn("Could not load table access config:", error.message);
            this.tableAccess = null;
        }
    }

    formatKnowledgeBaseForPrompt() {
        if (!this.knowledgeBase) return "";

        // Get table access configuration from separate file
        const tableAccess = this.tableAccess || {};

        let kb = "\n\n=== KNOWLEDGE BASE (FULL SCHEMA WITH ACCESS STATUS) ===\n";

        // Collect all tables with their access status
        const allTables = [];
        let accessibleCount = 0;
        let inaccessibleCount = 0;

        if (this.knowledgeBase.catalogs) {
            for (const [catalogName, catalogInfo] of Object.entries(this.knowledgeBase.catalogs)) {
                const catalogAccess = tableAccess[catalogName] || {};

                for (const [tableName, tableInfo] of Object.entries(catalogInfo.tables || {})) {
                    const tableConfig = catalogAccess[tableName];
                    const isEnabled = tableConfig ? tableConfig.enabled !== false : true;

                    if (isEnabled) accessibleCount++;
                    else inaccessibleCount++;

                    allTables.push({
                        catalogName,
                        tableName,
                        tableInfo,
                        tableConfig,
                        isEnabled,
                        fullName: `${catalogName}.${tableName}`
                    });
                }
            }
        }

        // Summary of access
        kb += `\n## ACCESS SUMMARY:\n`;
        kb += `- ACCESSIBLE tables: ${accessibleCount} (you CAN query these)\n`;
        kb += `- INACCESSIBLE tables: ${inaccessibleCount} (you CANNOT query these - tell user to enable)\n\n`;

        // Group by catalog and show all tables with access markers
        const byCatalog = {};
        for (const item of allTables) {
            if (!byCatalog[item.catalogName]) {
                byCatalog[item.catalogName] = [];
            }
            byCatalog[item.catalogName].push(item);
        }

        kb += "## ALL TABLES (WITH ACCESS STATUS):\n";
        for (const [catalogName, tables] of Object.entries(byCatalog)) {
            kb += `\n### Catalog: ${catalogName}\n`;
            for (const { tableName, tableInfo, tableConfig, isEnabled, fullName } of tables) {
                const accessMarker = isEnabled ? "[ACCESSIBLE]" : "[NO ACCESS]";
                const tableDesc = tableConfig?.description || tableInfo.description || "";
                kb += `\n${accessMarker} **${fullName}** - ${tableDesc}\n`;

                // Only show column details for accessible tables (to keep prompt shorter)
                if (isEnabled) {
                    const columns = tableInfo.columns || {};
                    const columnList = Object.entries(columns).map(([colName, colInfo]) => {
                        let colDesc = `  - ${colName} (${colInfo.type})`;
                        if (colInfo.knownValues && colInfo.knownValues.length > 0) {
                            colDesc += ` [Values: ${colInfo.knownValues.slice(0, 5).join(", ")}...]`;
                        }
                        return colDesc;
                    }).join("\n");
                    kb += columnList + "\n";
                }
            }
        }

        kb += `\n## IMPORTANT BEHAVIOR:\n`;
        kb += `- ONLY query tables marked [ACCESSIBLE]\n`;
        kb += `- For [NO ACCESS] tables: Tell user "To get this data, please enable the [table_name] table in tableAccess.json"\n`;
        kb += `- NEVER invent table names - only use tables listed above\n`;

        // Add fuzzy matching guide for category values
        kb += "\n## CATEGORY/SUBCATEGORY MATCHING GUIDE:\n";
        kb += "When user mentions categories, use LOWER() for case-insensitive matching:\n";
        if (this.knowledgeBase.catalogs) {
            const allCategories = new Set();
            const allSubCategories = new Set();
            for (const catalogInfo of Object.values(this.knowledgeBase.catalogs)) {
                for (const tableInfo of Object.values(catalogInfo.tables || {})) {
                    const columns = tableInfo.columns || {};
                    if (columns.category?.knownValues) {
                        columns.category.knownValues.forEach(v => allCategories.add(v));
                    }
                    if (columns.sub_category?.knownValues) {
                        columns.sub_category.knownValues.forEach(v => allSubCategories.add(v));
                    }
                }
            }
            if (allCategories.size > 0) {
                kb += `- CATEGORIES: ${Array.from(allCategories).slice(0, 15).join(", ")}\n`;
                kb += `  Usage: WHERE LOWER(category) = 'men' (note: use 'men' not 'mens')\n`;
            }
            if (allSubCategories.size > 0) {
                kb += `- SUB-CATEGORIES: ${Array.from(allSubCategories).slice(0, 15).join(", ")}\n`;
                kb += `  Usage: WHERE LOWER(sub_category) = 'running'\n`;
            }
        }

        // Add entity recognition with examples
        if (this.knowledgeBase.entityRecognition) {
            kb += "\n## ENTITY RECOGNITION (DISTINGUISH BETWEEN):\n";
            const er = this.knowledgeBase.entityRecognition;
            if (er.brands?.examples?.length > 0) {
                kb += `- BRAND NAMES (join with t_master_brand): ${er.brands.examples.join(", ")}\n`;
                kb += `  Pattern: JOIN database.global.t_master_brand b ON CAST(a.master_brand_id AS uuid) = b.master_brand_id WHERE LOWER(b.brand_name) = 'brandname'\n`;
            }
            if (er.products?.examples?.length > 0) {
                kb += `- PRODUCT NAMES (use product_name column): ${er.products.examples.join(", ")}\n`;
                kb += `  Pattern: WHERE LOWER(product_name) LIKE '%productname%'\n`;
            }
        }

        // Add table selection guide (only for accessible tables)
        kb += "\n## TABLE SELECTION GUIDE:\n";
        kb += "(Only use tables that are listed above in ACCESSIBLE TABLES)\n";

        const tableGuides = [
            { table: "mv_ads_sales_analysis", catalog: "lakehouse.ap_south1_gold", hint: "For SALES data (net_sale, gross_sale, units)" },
            { table: "mv_ads_ad_analysis", catalog: "lakehouse.ap_south1_gold", hint: "For AD PERFORMANCE by product (ads_impression, ads_click, ads_spend)" },
            { table: "mv_ads_cst_analysis", catalog: "lakehouse.ap_south1_gold", hint: "For AD PERFORMANCE by campaign/targeting" },
            { table: "mv_ads_placement_analysis", catalog: "lakehouse.ap_south1_gold", hint: "For PLACEMENT analysis" },
            { table: "mv_ads_keyword_analysis", catalog: "lakehouse.ap_south1_gold", hint: "For KEYWORD analysis" },
            { table: "t_master_brand_campaign", catalog: "database.global", hint: "For CAMPAIGN list/details" },
            { table: "v_master_campaign", catalog: "database.global", hint: "For CAMPAIGN view" },
            { table: "t_master_brand", catalog: "database.global", hint: "For BRAND lookup" },
            { table: "t_master_platform", catalog: "database.global", hint: "For PLATFORM lookup" },
            { table: "t_master_brand_product", catalog: "database.global", hint: "For PRODUCT mapping" },
            { table: "v_master_product", catalog: "database.global", hint: "For PRODUCT view" },
            { table: "t_master_brand_targeting", catalog: "database.global", hint: "For TARGETING data" },
            { table: "t_master_brand_category", catalog: "database.global", hint: "For CATEGORY mapping" },
            { table: "t_master_brand_sub_category", catalog: "database.global", hint: "For SUB-CATEGORY mapping" }
        ];

        for (const guide of tableGuides) {
            const catalogAccess = tableAccess[guide.catalog] || {};
            const tableConfig = catalogAccess[guide.table];
            const isEnabled = tableConfig ? tableConfig.enabled !== false : true;

            if (isEnabled) {
                kb += `- ${guide.hint}: Use ${guide.catalog}.${guide.table}\n`;
            }
        }

        // Add common patterns
        if (this.knowledgeBase.commonPatterns) {
            kb += "\n## COMMON QUERY PATTERNS:\n";
            for (const [key, value] of Object.entries(this.knowledgeBase.commonPatterns)) {
                kb += `- ${value.description}:\n  ${value.pattern}\n`;
            }
        }

        // Add type casting reminder
        if (this.knowledgeBase.typeCasting) {
            kb += `\n## CRITICAL TYPE CASTING:\n${this.knowledgeBase.typeCasting.description}\nSOLUTION: ${this.knowledgeBase.typeCasting.solution}\n`;
        }

        kb += "\n=== END KNOWLEDGE BASE ===\n";
        return kb;
    }

    async generateSQL(userMessage, conversationHistory = [], selectedBrand = null) {
        // RELOAD TABLE ACCESS CONFIGURATION HOT
        this.loadTableAccess();

        const knowledgeBaseContext = this.formatKnowledgeBaseForPrompt();
        const catalogsInfo = this.trinoService.getCatalogsInfo();
        const catalogsList = catalogsInfo.map(c => `${c.catalog}.${c.schema}`).join(', ');
        const currentDate = new Date().toISOString().split('T')[0];

        let brandContext = "";
        if (selectedBrand) {
            brandContext = `\n\nCRITICAL BRAND FILTERING ENFORCED:\nThe user has SELECTED the brand: "${selectedBrand}".\n\nRULES YOU MUST FOLLOW:\n1. You MUST limit ALL data to "${selectedBrand}".\n2. For every query, you MUST add a WHERE clause to filter by this brand.\n3. If querying a dimension table (like category, product, campaign), you MUST JOIN it with 'database.global.t_master_brand' (or equivalent) to filter by brand_name = '${selectedBrand}'.\n4. NEVER list all categories/products/campaigns globally - only list those associated with "${selectedBrand}".\n5. If the user asks for "all categories", they mean "all categories for ${selectedBrand}".`;
        }

        const systemPrompt = `You are a SQL expert assistant that helps users query a Trino database. 
You have READ-ONLY access to MULTIPLE catalogs: ${catalogsList}

CURRENT DATE: ${currentDate}${brandContext}

CRITICAL BEHAVIOR (YOU MUST FOLLOW THESE):
- You MUST generate and execute SQL queries for every user request about data.
- NEVER just describe the schema - ALWAYS run an actual query.
- ONLY USE TABLES FROM THE ACCESSIBLE TABLES SECTION IN THE KNOWLEDGE BASE BELOW.
- NEVER INVENT OR GUESS TABLE NAMES. If a table is not in the ACCESSIBLE TABLES list, you CANNOT use it.
- If a user asks for analysis: Try to answer using ONLY the tables marked [ACCESSIBLE].
- ONLY if NO relevant table is accessible, respond with: {"type": "text", "message": "I don't have access to the required data table. Please check tableAccess.json to enable the necessary tables."}
- USE THE KNOWLEDGE BASE BELOW as your PRIMARY REFERENCE for:
  * Which table to use for different types of queries (sales, ads, keywords, etc.)
  * Known valid values for category, sub_category columns (use these exact values)
  * Entity recognition: distinguish between brand names vs product names
  * Query patterns for joins, filters, and aggregations
- When user asks "show tables" or "list tables":
  * For lakehouse tables: SHOW TABLES FROM lakehouse.ap_south1_gold
  * For database tables: SHOW TABLES FROM database.global
  * If user wants ALL tables, respond with a text message listing ALL tables from BOTH catalogs as shown in the Knowledge Base. Do NOT use semicolons to separate queries.
- When user asks about any data, you MUST generate a SELECT query.
- ANY date before ${currentDate} is historical data - query it directly.
- Execute the query and get the final output always.
- MATCH USER INPUT TO KNOWN VALUES: If user says "mens" match to "Men", if user says "running shoes" match to sub_category "Running"
- STAY FOCUSED: You are ONLY a database assistant. Do NOT answer general knowledge questions, trivia, or anything unrelated to the database.
- If user asks about topics not related to the database (e.g., famous people, history, science, etc.), politely respond: "I'm your database assistant and can only help with queries related to your business data. Please ask me something about your sales, products, brands, or other data in the database."

IMPORTANT RULES:
1. NEVER generate INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE, or any write operations.
2. ALWAYS use fully qualified table names: catalog.schema.table_name
3. Tables from different catalogs can be JOINed together in the same query.
4. Keep queries efficient - use LIMIT 100 by default. However, if user explicitly asks for "all data", "complete data", "no limit", "full data", or similar phrases, do NOT add any LIMIT clause.
5. For date/time filtering, use appropriate Trino date functions.
6. String comparisons: Use LOWER() for case-insensitive matching.
7. For aggregations:
   - When user asks for "sales data" over a time period, ALWAYS aggregate by date.
   - Example: SELECT date, SUM(net_sale), SUM(ads_sale) ... GROUP BY date
   - Do NOT return multiple rows for the same date unless specifically asked for granular data.

TYPE CASTING FOR CROSS-CATALOG JOINS (CRITICAL):
- The master_brand_id AND master_platform_id columns are VARCHAR in lakehouse tables but UUID in database tables.
- NEVER use IN (subquery) for cross-catalog filtering - it will fail due to type mismatch.
- ALWAYS use JOIN with explicit CAST: 
  JOIN database.global.t_master_brand b ON CAST(a.master_brand_id AS uuid) = b.master_brand_id
  JOIN database.global.t_master_platform p ON CAST(a.master_platform_id AS uuid) = p.master_platform_id

DATE FILTERING EXAMPLES:
- "November 2025": WHERE date >= DATE '2025-11-01' AND date < DATE '2025-12-01'
- "last 3 months": WHERE date >= CURRENT_DATE - INTERVAL '3' MONTH

PLATFORM FILTERING:
- "on Amazon": JOIN database.global.t_master_platform p ON CAST(a.master_platform_id AS uuid) = p.master_platform_id WHERE LOWER(p.platform_name) = 'amazon'

HANDLING SPECIAL REQUESTS:
- "show tables", "list tables", "available tables": Use SHOW TABLES query for each catalog
- For past dates (before ${currentDate}): Query historical data directly
- For future dates (after ${currentDate}): Use type "forecast" to predict future trends based on historical data.
- "sales analysis": Use the appropriate sales table from ACCESSIBLE TABLES section above
- "last year": For current date ${currentDate}, "last year" means 2025 (Jan 1 to Dec 31, 2025)

ENTITY RECOGNITION (CRITICAL - BE SMART ABOUT THIS):
- BRAND NAMES: Stored in database.global.t_master_brand (brand_name column)
  Examples: "Asian", "Nat Habit", "Mamaearth" - these are company/brand names
  Filter: JOIN database.global.t_master_brand b ON CAST(a.master_brand_id AS uuid) = b.master_brand_id WHERE LOWER(b.brand_name) = 'asian'
  
- PRODUCT NAMES: Stored in product_name column of sales tables
  Examples: "boston-01", "shampoo-500ml", "face-wash" - these are specific product SKUs/names
  Filter: WHERE LOWER(product_name) LIKE '%boston%'
  
- HOW TO DISTINGUISH:
  * If user mentions "[brand] sales" or "sales of [brand]" → brand_name
  * If user mentions "[product] sold by [brand]" → product_name filtered with brand_name
  * Product names often have codes, numbers, or technical identifiers (e.g., "boston-01", "SKU-123")
  * Brand names are typically company names without codes
  
- COMBINED QUERIES (brand + product):
  Example: "sales of [product] by [brand]"
  Use the sales table from ACCESSIBLE TABLES, JOIN with brand table if enabled, filter by brand_name and product_name

YEAR-OVER-YEAR COMPARISONS:
- For "compare 2025 vs 2026" or "YoY comparison":
  * Query 2025 data and 2026 data separately
  * Use UNION ALL or pivot the results for easy comparison
  * Example: Compare same product between two years
- For comparing SAME MONTH across years (e.g., "Jan 2025 vs Jan 2026"):
  * Use EXTRACT(MONTH FROM date) = [month_number] to filter for specific month
  * Group by EXTRACT(YEAR FROM date) to separate the years
  * Example pattern for "compare January sales 2025 vs 2026":
    SELECT EXTRACT(YEAR FROM date) AS year, SUM(net_sale) AS total_sales
    FROM [sales_table_from_accessible_tables]
    WHERE EXTRACT(MONTH FROM date) = 1 AND EXTRACT(YEAR FROM date) IN (2025, 2026)
    GROUP BY EXTRACT(YEAR FROM date) ORDER BY year

ALWAYS EXECUTE QUERIES FOR:
- "show tables" / "list tables" / "what tables": Generate and execute SHOW TABLES queries
- "describe [table]": Execute DESCRIBE query
- Any question about data: Generate and execute SELECT query
- NEVER just describe what's in the schema - ALWAYS run a query to show actual data

DATABASE SCHEMA (USE ONLY TABLES LISTED HERE):
${knowledgeBaseContext}

RESPONSE FORMAT - YOU MUST RESPOND WITH ONLY VALID JSON:
- For queries: {"type": "query", "sql": "YOUR SQL HERE", "explanation": "Brief explanation"}
- For forecasts: {"type": "forecast", "sql": "SQL TO FETCH HISTORICAL DATA", "explanation": "Brief explanation", "forecast_period": "30"}. Set forecast_period to number of future data points needed (e.g., 30 for a month, 90 for a quarter).
- For text responses: {"type": "text", "message": "Your response"}
- For errors: {"type": "error", "message": "Error description"}
- NO markdown code blocks, NO extra text, ONLY the JSON object.`;

        const messages = [
            { role: "system", content: systemPrompt },
            ...conversationHistory.slice(-10),
            { role: "user", content: userMessage },
        ];

        try {
            const response = await this.client.chat.completions.create({
                model: "gpt-4o",
                messages: messages,
                temperature: 0.1,
                max_tokens: 2000,
            });

            const content = response.choices[0].message.content;

            try {
                let jsonContent = content;
                // Try to find JSON block enclosed in markdown
                const jsonMatch = content.match(/```json\n?([\s\S]*?)\n?```/) || content.match(/```([\s\S]*?)```/);

                if (jsonMatch) {
                    jsonContent = jsonMatch[1];
                } else {
                    // Fallback: Try to find the first { and last }
                    const firstBrace = content.indexOf('{');
                    const lastBrace = content.lastIndexOf('}');
                    if (firstBrace !== -1 && lastBrace !== -1) {
                        jsonContent = content.substring(firstBrace, lastBrace + 1);
                    }
                }

                return JSON.parse(jsonContent.trim());
            } catch (parseError) {
                console.warn("Failed to parse JSON from AI response, treating as text:", parseError);
                return { type: "text", message: content };
            }
        } catch (error) {
            console.error("OpenAI API error:", error);
            throw new Error(`Failed to generate response: ${error.message}`);
        }
    }

    async explainResults(userQuestion, sql, results) {
        const prompt = `The user asked: "${userQuestion}"
    
The following SQL query was executed:
${sql}

The query returned ${results.rows.length} rows with columns: ${results.columns.join(", ")}

${results.rows.length > 0 ? `Sample data (first 5 rows):\n${JSON.stringify(results.rows.slice(0, 5), null, 2)}` : "No data was returned."}

Please provide a brief, helpful summary of these results in 1-3 sentences.`;

        try {
            const response = await this.client.chat.completions.create({
                model: "gpt-4o-mini",
                messages: [
                    { role: "system", content: "You are a helpful data analyst. Provide clear, concise summaries of query results." },
                    { role: "user", content: prompt },
                ],
                temperature: 0.3,
                max_tokens: 500,
            });

            return response.choices[0].message.content;
        } catch (error) {
            console.error("Error explaining results:", error);
            return `Query returned ${results.rows.length} rows.`;
        }
    }

    async generateForecast(userQuestion, historicalData, forecastPeriod = 30) {
        // Prepare data summary for OpenAI (limit to prevent token overflow)
        const columns = historicalData.columns;
        const rows = historicalData.rows;
        const dateColIdx = columns.findIndex(c => c.toLowerCase().includes('date'));
        const valueColIdx = columns.findIndex((c, i) => i !== dateColIdx && rows.length > 0 && !isNaN(parseFloat(rows[0][i])));

        if (dateColIdx === -1 || valueColIdx === -1) {
            return { success: false, message: "Cannot identify date and value columns for forecasting." };
        }

        // Get last 30-50 data points for context
        const recentRows = rows.slice(-50);
        const dataForAI = recentRows.map(row => ({
            date: row[dateColIdx],
            value: parseFloat(row[valueColIdx])
        })).filter(d => d.date && !isNaN(d.value));

        const prompt = `The user asked: "${userQuestion}"

Here is the historical sales data (last ${dataForAI.length} data points):
${JSON.stringify(dataForAI, null, 2)}

Based on this data, please:
1. Analyze the trend (increasing, decreasing, seasonal patterns, etc.)
2. Predict the next ${forecastPeriod} days of data
3. Provide a summary of your prediction

RESPOND WITH ONLY VALID JSON in this exact format:
{
  "analysis": "Brief description of the trend you observed",
  "predictions": [
    {"date": "YYYY-MM-DD", "value": predicted_number},
    ...
  ],
  "summary": "Brief summary of the forecast"
}`;

        try {
            const response = await this.client.chat.completions.create({
                model: "gpt-4o",
                messages: [
                    {
                        role: "system",
                        content: "You are an expert data analyst and forecaster. Analyze the given time series data and predict future values. Use appropriate forecasting techniques based on the data patterns. Respond ONLY with valid JSON."
                    },
                    { role: "user", content: prompt },
                ],
                temperature: 0.2,
                max_tokens: 4000,
            });

            const content = response.choices[0].message.content;

            // Parse JSON response
            let jsonContent = content;
            const jsonMatch = content.match(/```json\n?([\s\S]*?)\n?```/) || content.match(/```([\s\S]*?)```/);
            if (jsonMatch) {
                jsonContent = jsonMatch[1];
            } else {
                const firstBrace = content.indexOf('{');
                const lastBrace = content.lastIndexOf('}');
                if (firstBrace !== -1 && lastBrace !== -1) {
                    jsonContent = content.substring(firstBrace, lastBrace + 1);
                }
            }

            const forecast = JSON.parse(jsonContent.trim());
            return {
                success: true,
                analysis: forecast.analysis,
                predictions: forecast.predictions,
                summary: forecast.summary,
                dateColumn: columns[dateColIdx],
                valueColumn: columns[valueColIdx]
            };
        } catch (error) {
            console.error("Error generating forecast:", error);
            return { success: false, message: `Failed to generate forecast: ${error.message}` };
        }
    }
}

module.exports = new OpenAIService();
