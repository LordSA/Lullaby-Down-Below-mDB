package com.buglife.utils;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.github.cdimascio.dotenv.Dotenv;
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
/**
 * TelemetryManager — The modern MongoDB-backed Overseer.
 * 
 * Matches the schema and logic of the Python Overseer script:
 * - Collections: players, sessions, deaths, events, savefiles, mapzones
 * - Primary Key: player_id (autoincrement style), session_id
 */
public class TelemetryManager {
    private static final Logger logger = LoggerFactory.getLogger(TelemetryManager.class);
    
    private static String connectionString;
    private static String databaseName;
    
    private static MongoClient mongoClient;
    private static MongoDatabase database;
    private static final ExecutorService executor = Executors.newSingleThreadExecutor();
    private static boolean initialized = false;
    
    // Session state
    private static Integer currentPlayerId;
    private static Integer currentSessionId;

    /**
     * Initialize the MongoDB connection using .env variables.
     * This is called on game startup.
     */
    public static void initialize() {
        if (initialized) return;

        // Load environment variables
        try {
            Dotenv dotenv = Dotenv.configure()
                    .directory("./")
                    .ignoreIfMissing()
                    .load();

            connectionString = dotenv.get("MONGODB_URI");
            databaseName = dotenv.get("MONGODB_DATABASE", "lullaby_down_below");
            
            if (connectionString == null) {
                logger.error("MONGODB_URI is missing in .env file!");
                return;
            }
            
            logger.info("Environment loaded. Target database: {}", databaseName);
        } catch (Exception e) {
            logger.warn("Could not load .env file, using defaults: {}", e.getMessage());
        }
        
        executor.submit(() -> {
            try {
                logger.info("Connecting to MongoDB Atlas...");
                mongoClient = MongoClients.create(connectionString);
                database = mongoClient.getDatabase(databaseName);
                
                // Fast connection check (ping)
                database.runCommand(new Document("ping", 1));
                
                initialized = true;
                logger.info("Telemetry system connected to Atlas: {}", databaseName);
                
                // Initialize indexes like the Python Overseer
                database.getCollection("players").createIndex(new Document("username", 1), new com.mongodb.client.model.IndexOptions().unique(true));
                database.getCollection("players").createIndex(new Document("player_id", 1), new com.mongodb.client.model.IndexOptions().unique(true));
                database.getCollection("sessions").createIndex(new Document("session_id", 1), new com.mongodb.client.model.IndexOptions().unique(true));
                
            } catch (Exception e) {
                logger.error("Critical: Telemetry could not connect to Atlas! {}", e.getMessage());
            }
        });
    }

    /**
     * Register or login a player (Equivalent to /player/register).
     */
    public static void registerPlayer(String username, String password) {
        if (!initialized && mongoClient == null) return;

        executor.submit(() -> {
            try {
                MongoCollection<Document> players = database.getCollection("players");
                Document existing = players.find(new Document("username", username)).first();
                
                if (existing != null) {
                    currentPlayerId = existing.getInteger("player_id");
                    logger.info("Player logged in: {} (ID: {})", username, currentPlayerId);
                } else {
                    // Get next ID (simple max + 1 logic like Python script)
                    Document maxPlayer = players.find().sort(new Document("player_id", -1)).first();
                    int nextPid = (maxPlayer != null) ? maxPlayer.getInteger("player_id") + 1 : 1;
                    
                    Document newPlayer = new Document()
                        .append("player_id", nextPid)
                        .append("username", username)
                        .append("password", password)
                        .append("created_at", new Date());
                    
                    players.insertOne(newPlayer);
                    currentPlayerId = nextPid;
                    logger.info("New player registered: {} (ID: {})", username, nextPid);
                }
                
                // Automatically start session after registration/login
                startSession();
            } catch (Exception e) {
                logger.error("Failed to register/login player: {}", e.getMessage());
            }
        });
    }

    /**
     * Start a new gameplay session (Equivalent to /session/start).
     */
    public static void startSession() {
        if (currentPlayerId == null) return;
        
        executor.submit(() -> {
            try {
                MongoCollection<Document> sessions = database.getCollection("sessions");
                Document maxSession = sessions.find().sort(new Document("session_id", -1)).first();
                int nextSid = (maxSession != null) ? maxSession.getInteger("session_id") + 1 : 1;
                
                Document sessionDoc = new Document()
                    .append("session_id", nextSid)
                    .append("player_id", currentPlayerId)
                    .append("start_time", new Date());
                
                sessions.insertOne(sessionDoc);
                currentSessionId = nextSid;
                logger.info("Session started: {}", nextSid);
            } catch (Exception e) {
                logger.error("Failed to start session: {}", e.getMessage());
            }
        });
    }

    /**
     * End the current session (Equivalent to /session/end).
     */
    public static void endSession() {
        if (currentSessionId == null) return;
        
        executor.submit(() -> {
            try {
                database.getCollection("sessions").updateOne(
                    new Document("session_id", currentSessionId),
                    new Document("$set", new Document("end_time", new Date()))
                );
                logger.info("Session ended: {}", currentSessionId);
            } catch (Exception e) {
                logger.error("Failed to end session: {}", e.getMessage());
            }
        });
    }

    /**
     * Log a death event (Equivalent to /death).
     */
    public static void logDeath(String areaCode, double x, double y, String cause) {
        if (currentSessionId == null) return;
        
        executor.submit(() -> {
            try {
                Document deathDoc = new Document()
                    .append("session_id", currentSessionId)
                    .append("area_code", areaCode)
                    .append("death_x", x)
                    .append("death_y", y)
                    .append("death_cause", cause)
                    .append("recorded_at", new Date());
                
                database.getCollection("deaths").insertOne(deathDoc);
                logger.debug("Death recorded: {} in {}", cause, areaCode);
            } catch (Exception e) {
                logger.error("Failed to log death: {}", e.getMessage());
            }
        });
    }

    /**
     * Log a general player event (Equivalent to /event).
     */
    public static void logPlayerEvent(String eventType, String areaCode, Double x, Double y, Object value) {
        if (currentSessionId == null) return;

        executor.submit(() -> {
            try {
                Document eventDoc = new Document()
                    .append("session_id", currentSessionId)
                    .append("event_type", eventType)
                    .append("area_code", areaCode)
                    .append("event_x", x)
                    .append("event_y", y)
                    .append("event_value", value)
                    .append("event_time", new Date());
                
                database.getCollection("events").insertOne(eventDoc);
            } catch (Exception e) {
                logger.error("Failed to log player event: {}", e.getMessage());
            }
        });
    }

    /**
     * Sync save file completion (Equivalent to /save/upload).
     */
    public static void syncSaveFile(int slot, double completionPct) {
        if (currentPlayerId == null) return;

        executor.submit(() -> {
            try {
                database.getCollection("savefiles").updateOne(
                    new Document("player_id", currentPlayerId).append("slot_number", slot),
                    new Document("$set", new Document("completion_pct", completionPct)
                                        .append("last_updated", new Date())),
                    new com.mongodb.client.model.UpdateOptions().upsert(true)
                );
                logger.info("Save record synced to cloud for player {}", currentPlayerId);
            } catch (Exception e) {
                logger.error("Failed to sync save file: {}", e.getMessage());
            }
        });
    }

    /**
     * Log performance metrics as an event.
     */
    public static void logPerformance(double fps, double updateMs, double renderMs) {
        logPlayerEvent("performance", null, null, null, 
            new Document("fps", fps).append("update_ms", updateMs).append("render_ms", renderMs));
    }

    /**
     * Cleanup resources on game exit.
     */
    public static void shutdown() {
        endSession();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(3, java.util.concurrent.TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
        if (mongoClient != null) {
            mongoClient.close();
        }
        logger.info("Telemetry system shut down");
    }
}
