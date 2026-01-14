const trinoService = require("../services/trinoService");
const openaiService = require("../services/openaiService");

const conversations = new Map();

exports.chat = async (req, res) => {
    try {
        const { message, sessionId = "default", brand } = req.body;

        if (!message || typeof message !== "string") {
            return res.status(400).json({ success: false, error: "Message is required" });
        }

        if (!conversations.has(sessionId)) {
            conversations.set(sessionId, []);
        }
        const history = conversations.get(sessionId);

        const aiResponse = await openaiService.generateSQL(message, history, brand);
        history.push({ role: "user", content: message });

        let response;

        if (aiResponse.type === "query" || aiResponse.type === "forecast") {
            try {
                const results = await trinoService.executeQuery(aiResponse.sql);
                let responseData = {
                    columns: results.columns,
                    rows: results.rows,
                    rowCount: results.rows.length,
                };

                // Handle Forecast using OpenAI
                if (aiResponse.type === "forecast") {
                    const forecastPeriod = parseInt(aiResponse.forecast_period) || 30;
                    console.log(`Forecast requested for ${forecastPeriod} periods - sending to OpenAI`);

                    const forecastResult = await openaiService.generateForecast(message, results, forecastPeriod);
                    console.log(`OpenAI forecast result: ${forecastResult.success ? 'success' : 'failed'}`);

                    if (forecastResult.success && forecastResult.predictions) {
                        // Add "Forecast" column and merge predictions cleanly
                        const newColumns = [...results.columns, "Forecast"];
                        const dateColIdx = results.columns.findIndex(c => c.toLowerCase().includes('date'));
                        const valueColIdx = results.columns.findIndex((c, i) =>
                            i !== dateColIdx && results.rows.length > 0 && !isNaN(parseFloat(results.rows[0][i]))
                        );

                        // Filter out historical rows that have NULL values (incomplete data)
                        const validHistoricalRows = results.rows.filter(row =>
                            row[dateColIdx] && row[valueColIdx] !== null && row[valueColIdx] !== undefined
                        );

                        // Create a set of existing dates to avoid duplicates
                        const existingDates = new Set(validHistoricalRows.map(row => row[dateColIdx]));

                        // Transform valid historical rows: add null for Forecast column
                        const newRows = validHistoricalRows.map(row => [...row, null]);

                        // Get the last actual value to connect the forecast line
                        if (newRows.length > 0) {
                            const lastRow = newRows[newRows.length - 1];
                            lastRow[newColumns.length - 1] = parseFloat(lastRow[valueColIdx]); // Connect forecast line
                        }

                        // Add prediction rows for dates that don't already exist
                        for (const pred of forecastResult.predictions) {
                            if (!existingDates.has(pred.date)) {
                                const newRow = new Array(newColumns.length).fill(null);
                                newRow[dateColIdx] = pred.date;
                                newRow[newColumns.length - 1] = pred.value; // Forecast column
                                newRows.push(newRow);
                            }
                        }

                        responseData.columns = newColumns;
                        responseData.rows = newRows;
                        responseData.rowCount = newRows.length;
                        responseData.forecastAnalysis = forecastResult.analysis;
                        responseData.forecastSummary = forecastResult.summary;
                    }
                }

                const explanation = await openaiService.explainResults(message, aiResponse.sql, {
                    columns: responseData.columns,
                    rows: responseData.rows
                });

                response = {
                    success: true,
                    type: "query_result", // Keep type as query_result so frontend renders it as a table/chart
                    sql: aiResponse.sql,
                    queryExplanation: aiResponse.explanation,
                    resultExplanation: explanation,
                    columns: responseData.columns,
                    rows: responseData.rows,
                    rowCount: responseData.rowCount,
                    fromCache: results.fromCache || false,
                    isForecast: aiResponse.type === "forecast"
                };

                history.push({
                    role: "assistant",
                    content: JSON.stringify({
                        type: aiResponse.type,
                        sql: aiResponse.sql,
                        rowCount: responseData.rowCount
                    }),
                });
            } catch (queryError) {
                response = {
                    success: false,
                    type: "query_error",
                    sql: aiResponse.sql,
                    error: queryError.message,
                };
                history.push({ role: "assistant", content: `Query failed: ${queryError.message}` });
            }
        } else if (aiResponse.type === "error") {
            response = { success: false, type: "error", message: aiResponse.message };
            history.push({ role: "assistant", content: aiResponse.message });
        } else {
            response = { success: true, type: "text", message: aiResponse.message };
            history.push({ role: "assistant", content: aiResponse.message });
        }

        if (history.length > 20) {
            conversations.set(sessionId, history.slice(-20));
        }

        res.json(response);
    } catch (error) {
        console.error("Chat error:", error);
        res.status(500).json({ success: false, type: "error", error: error.message });
    }
};

exports.getTables = async (req, res) => {
    try {
        const tables = await trinoService.getTables();
        res.json({ success: true, tables });
    } catch (error) {
        console.error("Get tables error:", error);
        res.status(500).json({ success: false, error: error.message });
    }
};

exports.getTableSchema = async (req, res) => {
    try {
        const { tableName } = req.params;
        if (!tableName) {
            return res.status(400).json({ success: false, error: "Table name is required" });
        }

        const parts = tableName.split('.');
        if (parts.length !== 3) {
            return res.status(400).json({ success: false, error: "Table name should be in format: catalog.schema.table" });
        }

        const [catalog, schema, table] = parts;
        const tableSchema = await trinoService.getTableSchema(catalog, schema, table);
        res.json({ success: true, table: tableName, columns: tableSchema });
    } catch (error) {
        console.error("Get schema error:", error);
        res.status(500).json({ success: false, error: error.message });
    }
};

exports.refreshSchema = async (req, res) => {
    try {
        await trinoService.refreshSchema();
        res.json({ success: true, message: "Schema cache refreshed" });
    } catch (error) {
        console.error("Refresh schema error:", error);
        res.status(500).json({ success: false, error: error.message });
    }
};

exports.clearHistory = async (req, res) => {
    try {
        const { sessionId = "default" } = req.body;
        conversations.delete(sessionId);
        res.json({ success: true, message: "Conversation history cleared" });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
};

exports.getCacheStats = async (req, res) => {
    try {
        const stats = trinoService.getCacheStats();
        res.json({ success: true, ...stats });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
};

exports.clearCache = async (req, res) => {
    try {
        trinoService.clearCache();
        res.json({ success: true, message: "Query cache cleared" });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
};

// Helper function to calculate linear regression and generate forecast
function calculateForecast(columns, rows, periods = 30) {
    console.log(`calculateForecast called with ${rows.length} rows, ${periods} periods`);
    // 1. Identify Date and Metric columns
    let dateColIdx = -1;
    let metricColIdx = -1;

    for (let i = 0; i < columns.length; i++) {
        const colName = columns[i].toLowerCase();
        if (colName.includes('date') || colName.includes('time') || colName.includes('day') || colName.includes('month')) {
            dateColIdx = i;
        } else if (rows.length > 0 && !isNaN(parseFloat(rows[0][i]))) {
            // Pick first numeric column as metric if not already found
            if (metricColIdx === -1) metricColIdx = i;
        }
    }

    if (dateColIdx === -1 || metricColIdx === -1 || rows.length < 2) {
        return null; // Cannot forecast
    }

    // 2. Prepare data for regression (x = timestamp, y = value)
    const dataPoints = rows.map(row => {
        const dateStr = row[dateColIdx];
        const val = parseFloat(row[metricColIdx]);
        if (!dateStr || isNaN(val)) return null;
        return { x: new Date(dateStr).getTime(), y: val, date: new Date(dateStr) };
    }).filter(p => p !== null);

    if (dataPoints.length < 2) return null;

    // Sort by date
    dataPoints.sort((a, b) => a.x - b.x);

    // 3. Linear Regression (Least Squares)
    const n = dataPoints.length;
    let sumX = 0, sumY = 0, sumXY = 0, sumXX = 0;

    for (const p of dataPoints) {
        sumX += p.x;
        sumY += p.y;
        sumXY += (p.x * p.y);
        sumXX += (p.x * p.x);
    }

    const slope = (n * sumXY - sumX * sumY) / (n * sumXX - sumX * sumX);
    const intercept = (sumY - slope * sumX) / n;

    // 4. Generate Future Points
    const lastPoint = dataPoints[dataPoints.length - 1];
    const newRows = [];

    // Add "Forecast" column header
    const newColumns = [...columns, "Forecast"];

    // Transform existing rows: [Date, Val] -> [Date, Val, null]
    rows.forEach(row => {
        newRows.push([...row, null]);
    });

    // Generate N future points
    // Detect interval (avg diff between points)
    const avgInterval = (dataPoints[dataPoints.length - 1].x - dataPoints[0].x) / (n - 1);

    let currentX = lastPoint.x;

    // Connect the lines: Set 'Forecast' value for last row to Actual value
    const lastRowIndex = newRows.length - 1;
    newRows[lastRowIndex][newColumns.length - 1] = lastPoint.y;

    for (let i = 1; i <= periods; i++) {
        currentX += avgInterval;
        const futureDate = new Date(currentX);
        const futureVal = slope * currentX + intercept;

        // Format date back to string (ISO YYYY-MM-DD)
        const dateStr = futureDate.toISOString().split('T')[0];

        const newRow = new Array(newColumns.length).fill(null);
        newRow[dateColIdx] = dateStr;
        newRow[newColumns.length - 1] = futureVal; // Forecast column

        newRows.push(newRow);
    }

    return { columns: newColumns, rows: newRows };
}

exports.getBrands = async (req, res) => {
    try {
        const query = "SELECT distinct brand_name FROM database.global.t_master_brand ORDER BY brand_name";
        const results = await trinoService.executeQuery(query);
        const brands = results.rows.map(row => row[0]);
        res.json({ success: true, brands });
    } catch (error) {
        console.error("Error fetching brands:", error);
        res.status(500).json({ success: false, error: "Failed to fetch brands" });
    }
};
