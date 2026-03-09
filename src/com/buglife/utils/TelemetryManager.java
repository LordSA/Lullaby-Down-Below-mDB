package com.buglife.utils;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * TelemetryManager — The modern MongoDB-backed Overseer.
 * 
 * Instead of rigid SQL tables, we use a flexible document-based system.
 * This tracks gameplay events, performance metrics, and player behavior
 * without slowing down the game loop (asynchronous execution).
 */
public class TelemetryManager {
    private static final Logger logger = LoggerFactory.getLogger(TelemetryManager.class);
    
    // Connection constants - should ideally move to config.json later
    private static final String CONNECTION_STRING = "mongodb://localhost:27017";
    private static final String DATABASE_NAME = "lullaby_down_below";
    private static final String COLLECTION_TELEMETRY = "gameplay_events";
    
    private static MongoClient mongoClient;
    private static MongoDatabase database;
    private static final ExecutorService executor = Executors.newSingleThreadExecutor();
    private static boolean initialized = false;

    /**
     * Initialize the MongoDB connection.
     * This is called on game startup.
     */
    public static void initialize() {
        if (initialized) return;
        
        executor.submit(() -> {
            try {
                logger.info("Connecting to MongoDB at {}...", CONNECTION_STRING);
                mongoClient = MongoClients.create(CONNECTION_STRING);
                database = mongoClient.getDatabase(DATABASE_NAME);
                initialized = true;
                logger.info("Telemetry system initialized successfully (MongoDB)");
                
                // Log session start
                logEvent("session_start", new Document()
                    .append("os", System.getProperty("os.name"))
                    .append("java_version", System.getProperty("java.version")));
            } catch (Exception e) {
                logger.error("Failed to initialize Telemetry: {}", e.getMessage());
            }
        });
    }

    /**
     * Log a gameplay event to MongoDB asynchronously.
     * 
     * @param eventType The type of event (e.g., "level_start", "player_death")
     * @param data      A Document containing event-specific key-value pairs
     */
    public static void logEvent(String eventType, Document data) {
        if (executor.isShutdown()) return;
        
        executor.submit(() -> {
            try {
                if (!initialized) {
                    // Try to init if not already (safeguard)
                    if (mongoClient == null) {
                        mongoClient = MongoClients.create(CONNECTION_STRING);
                        database = mongoClient.getDatabase(DATABASE_NAME);
                        initialized = true;
                    } else {
                        return; // Still waiting for init
                    }
                }
                
                MongoCollection<Document> collection = database.getCollection(COLLECTION_TELEMETRY);
                
                Document event = new Document()
                    .append("type", eventType)
                    .append("timestamp", new Date())
                    .append("data", data);
                
                collection.insertOne(event);
                logger.debug("Telemetry event logged: {}", eventType);
            } catch (Exception e) {
                logger.warn("Failed to log telemetry event: {}", e.getMessage());
            }
        });
    }

    /**
     * Convenience method for logging simple level progress.
     */
    public static void logLevelProgress(String playerName, String levelName, String status) {
        logEvent("level_progress", new Document()
            .append("player", playerName)
            .append("level", levelName)
            .append("status", status));
    }

    /**
     * Log performance metrics.
     */
    public static void logPerformance(double fps, double updateMs, double renderMs) {
        logEvent("performance_snapshot", new Document()
            .append("fps", fps)
            .append("update_ms", updateMs)
            .append("render_ms", renderMs));
    }

    /**
     * Cleanup resources on game exit.
     */
    public static void shutdown() {
        executor.shutdown();
        if (mongoClient != null) {
            mongoClient.close();
        }
        logger.info("Telemetry system shut down");
    }
}
