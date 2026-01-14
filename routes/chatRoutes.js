const express = require("express");
const router = express.Router();
const chatController = require("../controller/chatController");

// Chat endpoint
router.post("/chat", chatController.chat);

// Tables and schema
router.get("/tables", chatController.getTables);
router.get("/brands", chatController.getBrands);
router.get("/schema/:tableName", chatController.getTableSchema);

// Cache management
router.post("/refresh-schema", chatController.refreshSchema);
router.get("/cache/stats", chatController.getCacheStats);
router.post("/cache/clear", chatController.clearCache);

// Conversation management
router.post("/clear-history", chatController.clearHistory);

module.exports = router;
