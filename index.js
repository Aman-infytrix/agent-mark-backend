require("dotenv").config();
const express = require("express");
const cors = require("cors");

const chatRoutes = require("./routes/chatRoutes");
const trinoService = require("./services/trinoService");
const openaiService = require("./services/openaiService");

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());

// Routes
app.use("/api", chatRoutes);

// Health check
app.get("/health", (req, res) => {
    res.json({ status: "ok", timestamp: new Date().toISOString() });
});

// Initialize services and start server
async function startServer() {
    try {
        console.log("🚀 Starting Trino Chatbot Backend...");

        // Initialize OpenAI service with Trino service reference
        openaiService.initialize(trinoService);
        console.log("✅ OpenAI service initialized");

        // Try to load schema from Trino (but don't fail if unavailable)
        console.log("📊 Attempting to load database schema...");
        try {
            const schema = await trinoService.getFullSchema();
            const tableCount = Object.keys(schema).length;
            console.log(`✅ Loaded schema for ${tableCount} tables`);
        } catch (schemaError) {
            console.warn(`⚠️  Could not load schema at startup: ${schemaError.message}`);
            console.warn("   Schema will be loaded on first request when Trino becomes available.");
        }

        // Start server
        app.listen(PORT, () => {
            console.log(`\n🎉 Server running on http://localhost:${PORT}`);
            console.log(`📖 API Endpoints:`);
            console.log(`   POST /api/chat          - Send a chat message`);
            console.log(`   GET  /api/tables        - Get list of tables`);
            console.log(`   GET  /api/schema/:table - Get table schema`);
            console.log(`   POST /api/refresh-schema - Refresh schema cache`);
            console.log(`   POST /api/clear-history - Clear chat history`);
            console.log(`   GET  /health            - Health check\n`);
        });
    } catch (error) {
        console.error("❌ Failed to start server:", error.message);
        process.exit(1);
    }
}

startServer();
