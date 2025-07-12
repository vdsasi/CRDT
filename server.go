package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	// Connection limits
	maxConnectionsPerSession = 200
	maxTotalConnections     = 1000
	
	// Channel buffer sizes
	clientSendBufferSize    = 512
	hubBroadcastBufferSize  = 2048
	
	// Rate limiting
	maxOpsPerSecondPerClient = 100
	rateLimitWindow         = time.Second
	
	// Timeouts
	writeTimeout = 10 * time.Second
	readTimeout  = 60 * time.Second
	pingInterval = 30 * time.Second
	
	// Resource limits
	maxCRDTSize             = 10000000 // 10MB
	maxOperationsPerSession = 10000
	operationBatchSize      = 100
)


var globalConnectionCount int32

type RateLimiter struct {
	tokens    int
	maxTokens int
	lastReset time.Time
	mu        sync.Mutex
}

// Operation represents a single edit operation
type Operation struct {
	ID          string    `json:"id" bson:"id"`
	Type        string    `json:"type" bson:"type"`
	Position    int       `json:"position" bson:"position"`
	Character   string    `json:"character,omitempty" bson:"character,omitempty"`
	CharacterID string    `json:"character_id,omitempty" bson:"character_id,omitempty"`
	Timestamp   time.Time `json:"timestamp" bson:"timestamp"`
	UserID      string    `json:"user_id" bson:"user_id"`
	SessionID   string    `json:"session_id" bson:"session_id"`
}

// Character represents a character in the CRDT with positioning info
type Character struct {
	ID        string    `json:"id" bson:"id"`
	Value     string    `json:"value" bson:"value"`
	Position  float64   `json:"position" bson:"position"`
	Timestamp time.Time `json:"timestamp" bson:"timestamp"`
	UserID    string    `json:"user_id" bson:"user_id"`
	Deleted   bool      `json:"deleted" bson:"deleted"`
}

// CRDT represents our Conflict-free Replicated Data Type for text
type CRDT struct {
	Characters []Character `json:"characters" bson:"characters"`
	mu         sync.RWMutex
	sizeBytes  int64
}

// Session represents a collaborative editing session
type Session struct {
	ID          string    `json:"id" bson:"_id"`
	Title       string    `json:"title" bson:"title"`
	CreatedBy   string    `json:"created_by" bson:"created_by"`
	CreatedAt   time.Time `json:"created_at" bson:"created_at"`
	UpdatedAt   time.Time `json:"updated_at" bson:"updated_at"`
	ActiveUsers []string  `json:"active_users" bson:"active_users"`
	CRDT        *CRDT     `json:"crdt" bson:"crdt"`
}

// PubSubMessage represents a message sent through Redis pub/sub
type PubSubMessage struct {
	Type      string      `json:"type"`
	SessionID string      `json:"session_id"`
	UserID    string      `json:"user_id"`
	Data      interface{} `json:"data"`
	Timestamp time.Time   `json:"timestamp"`
}

// PersistenceManager handles Redis and MongoDB operations

func NewRateLimiter(maxTokens int) *RateLimiter {
	return &RateLimiter{
		tokens:    maxTokens,
		maxTokens: maxTokens,
		lastReset: time.Now(),
	}
}


func (rl *RateLimiter) Allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	
	now := time.Now()
	if now.Sub(rl.lastReset) >= rateLimitWindow {
		rl.tokens = rl.maxTokens
		rl.lastReset = now
	}
	
	if rl.tokens > 0 {
		rl.tokens--
		return true
	}
	return false
}


// Connection pool for WebSocket clients
type ConnectionPool struct {
	mu               sync.RWMutex
	sessionConns     map[string]map[*Client]bool
	totalConnections int32
}

func NewConnectionPool() *ConnectionPool {
	return &ConnectionPool{
		sessionConns: make(map[string]map[*Client]bool),
	}
}

func (cp *ConnectionPool) CanAddConnection(sessionID string) bool {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	
	if atomic.LoadInt32(&cp.totalConnections) >= maxTotalConnections {
		return false
	}
	
	if conns, exists := cp.sessionConns[sessionID]; exists {
		return len(conns) < maxConnectionsPerSession
	}
	return true
}

func (cp *ConnectionPool) AddConnection(sessionID string, client *Client) bool {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	
	if atomic.LoadInt32(&cp.totalConnections) >= maxTotalConnections {
		return false
	}
	
	if _, exists := cp.sessionConns[sessionID]; !exists {
		cp.sessionConns[sessionID] = make(map[*Client]bool)
	}
	
	if len(cp.sessionConns[sessionID]) >= maxConnectionsPerSession {
		return false
	}
	
	cp.sessionConns[sessionID][client] = true
	atomic.AddInt32(&cp.totalConnections, 1)
	return true
}

func (cp *ConnectionPool) RemoveConnection(sessionID string, client *Client) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	
	if conns, exists := cp.sessionConns[sessionID]; exists {
		delete(conns, client)
		atomic.AddInt32(&cp.totalConnections, -1)
		
		if len(conns) == 0 {
			delete(cp.sessionConns, sessionID)
		}
	}
}

type PersistenceManager struct {
	redisClient *redis.Client
	mongoClient *mongo.Client
	mongoDB     *mongo.Database
	ctx         context.Context
	
	// Operation batching
	opBatch      map[string][]Operation
	opBatchMu    sync.Mutex
	batchTicker  *time.Ticker
}




// NewPersistenceManager creates a new persistence manager
func NewPersistenceManager() *PersistenceManager {
	ctx := context.Background()

	// Redis client with connection pooling
	rdb := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		Password:     "",
		DB:           0,
		PoolSize:     100,            // Increased pool size
		MinIdleConns: 10,             // Minimum idle connections
		MaxRetries:   2,              // Reduce retries for faster failure
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolTimeout:  4 * time.Second,
	})

	// Test Redis connection
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatal("Failed to connect to Redis:", err)
	}

	// MongoDB client with connection pooling
	mongoOpts := options.Client().
		ApplyURI("mongodb://appuser:apppassword@localhost:27017/admin").
		SetMaxPoolSize(100).                    // Max connections in pool
		SetMinPoolSize(10).                     // Min connections to maintain
		SetMaxConnIdleTime(30 * time.Second).  // Close idle connections
		SetServerSelectionTimeout(5 * time.Second)

	mongoClient, err := mongo.Connect(ctx, mongoOpts)
	if err != nil {
		log.Fatal("Failed to connect to MongoDB:", err)
	}

	// Test MongoDB connection
	err = mongoClient.Ping(ctx, nil)
	if err != nil {
		log.Fatal("Failed to ping MongoDB:", err)
	}

	mongoDB := mongoClient.Database("crdt_editor")

	pm := &PersistenceManager{
		redisClient: rdb,
		mongoClient: mongoClient,
		mongoDB:     mongoDB,
		ctx:         ctx,
		opBatch:     make(map[string][]Operation),
		batchTicker: time.NewTicker(100 * time.Millisecond), // Batch every 100ms
	}
	
	// Start batch processor
	go pm.processBatches()
	
	return pm
}

// Batch processing for operations
func (pm *PersistenceManager) processBatches() {
	for range pm.batchTicker.C {
		pm.flushBatches()
	}
}

func (pm *PersistenceManager) flushBatches() {
	pm.opBatchMu.Lock()
	batches := pm.opBatch
	pm.opBatch = make(map[string][]Operation)
	pm.opBatchMu.Unlock()
	
	for sessionID, ops := range batches {
		if len(ops) > 0 {
			go pm.saveBatchedOperations(sessionID, ops)
		}
	}
}


func (pm *PersistenceManager) saveBatchedOperations(sessionID string, operations []Operation) error {
	pipe := pm.redisClient.Pipeline()
	key := fmt.Sprintf("operations:%s", sessionID)
	
	for _, op := range operations {
		operationData, _ := json.Marshal(op)
		score := float64(op.Timestamp.UnixNano())
		pipe.ZAdd(pm.ctx, key, &redis.Z{Score: score, Member: operationData})
	}
	
	// Keep only last N operations
	pipe.ZRemRangeByRank(pm.ctx, key, 0, -maxOperationsPerSession-1)
	pipe.Expire(pm.ctx, key, 24*time.Hour)
	
	_, err := pipe.Exec(pm.ctx)
	return err
}

func (pm *PersistenceManager) AddOperationToBatch(sessionID string, operation Operation) {
	pm.opBatchMu.Lock()
	defer pm.opBatchMu.Unlock()
	
	pm.opBatch[sessionID] = append(pm.opBatch[sessionID], operation)
	
	// Force flush if batch is too large
	if len(pm.opBatch[sessionID]) >= operationBatchSize {
		ops := pm.opBatch[sessionID]
		delete(pm.opBatch, sessionID)
		go pm.saveBatchedOperations(sessionID, ops)
	}
}



// PublishMessage publishes a message to Redis pub/sub
func (pm *PersistenceManager) PublishMessage(sessionID string, msgType string, userID string, data interface{}) error {
	message := PubSubMessage{
		Type:      msgType,
		SessionID: sessionID,
		UserID:    userID,
		Data:      data,
		Timestamp: time.Now(),
	}

	messageBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return pm.redisClient.Publish(pm.ctx, fmt.Sprintf("session:%s", sessionID), messageBytes).Err()
}

// SaveSessionToRedis saves session state to Redis with atomic operation
func (pm *PersistenceManager) SaveSessionToRedis(sessionID string, session *Session) error {
	sessionData, err := json.Marshal(session)
	if err != nil {
		return err
	}

	pipe := pm.redisClient.Pipeline()
	
	// Save session with TTL of 24 hours
	pipe.Set(pm.ctx, fmt.Sprintf("session:%s", sessionID), sessionData, 24*time.Hour)
	
	// Update session index
	pipe.SAdd(pm.ctx, "active_sessions", sessionID)
	
	// Save last updated timestamp
	pipe.Set(pm.ctx, fmt.Sprintf("session:%s:updated", sessionID), time.Now().Unix(), 24*time.Hour)
	
	_, err = pipe.Exec(pm.ctx)
	return err
}

// GetSessionFromRedis retrieves session from Redis
func (pm *PersistenceManager) GetSessionFromRedis(sessionID string) (*Session, error) {
	sessionData, err := pm.redisClient.Get(pm.ctx, fmt.Sprintf("session:%s", sessionID)).Result()
	if err == redis.Nil {
		return nil, nil // Session not found
	}
	if err != nil {
		return nil, err
	}

	var session Session
	err = json.Unmarshal([]byte(sessionData), &session)
	if err != nil {
		return nil, err
	}

	return &session, nil
}

// SaveOperationToRedis saves operation with proper ordering
func (pm *PersistenceManager) SaveOperationToRedis(sessionID string, operation Operation) error {
	operationData, err := json.Marshal(operation)
	if err != nil {
		return err
	}

	pipe := pm.redisClient.Pipeline()
	
	// Save to operations list for the session (using timestamp as score for ordering)
	key := fmt.Sprintf("operations:%s", sessionID)
	score := float64(operation.Timestamp.UnixNano())
	pipe.ZAdd(pm.ctx, key, &redis.Z{Score: score, Member: operationData})
	
	// Keep only last 1000 operations
	pipe.ZRemRangeByRank(pm.ctx, key, 0, -1001)
	
	// Set TTL for operations list
	pipe.Expire(pm.ctx, key, 24*time.Hour)
	
	_, err = pipe.Exec(pm.ctx)
	return err
}

// GetOperationsFromRedis retrieves operations in chronological order
func (pm *PersistenceManager) GetOperationsFromRedis(sessionID string, since time.Time) ([]Operation, error) {
	key := fmt.Sprintf("operations:%s", sessionID)
	sinceScore := float64(since.UnixNano())
	
	results, err := pm.redisClient.ZRangeByScore(pm.ctx, key, &redis.ZRangeBy{
		Min: fmt.Sprintf("(%f", sinceScore),
		Max: "+inf",
	}).Result()
	
	if err != nil {
		return nil, err
	}
	
	var operations []Operation
	for _, result := range results {
		var op Operation
		if err := json.Unmarshal([]byte(result), &op); err == nil {
			operations = append(operations, op)
		}
	}
	
	return operations, nil
}

// SaveSessionToMongoDB persists session to MongoDB
func (pm *PersistenceManager) SaveSessionToMongoDB(session *Session) error {
	collection := pm.mongoDB.Collection("sessions")
	
	session.UpdatedAt = time.Now()
	
	filter := bson.M{"_id": session.ID}
	update := bson.M{"$set": session}
	opts := options.Update().SetUpsert(true)
	
	_, err := collection.UpdateOne(pm.ctx, filter, update, opts)
	return err
}

// GetSessionFromMongoDB retrieves session from MongoDB
func (pm *PersistenceManager) GetSessionFromMongoDB(sessionID string) (*Session, error) {
	collection := pm.mongoDB.Collection("sessions")
	
	var session Session
	err := collection.FindOne(pm.ctx, bson.M{"_id": sessionID}).Decode(&session)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	
	return &session, nil
}

// GetAllSessions retrieves all sessions from MongoDB
func (pm *PersistenceManager) GetAllSessions() ([]Session, error) {
	collection := pm.mongoDB.Collection("sessions")
	
	cursor, err := collection.Find(pm.ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(pm.ctx)
	
	var sessions []Session
	err = cursor.All(pm.ctx, &sessions)
	return sessions, err
}

// StartPeriodicPersistence starts a goroutine that periodically saves active sessions to MongoDB
func (pm *PersistenceManager) StartPeriodicPersistence(hub *Hub) {
	ticker := time.NewTicker(30 * time.Second) // Save every 30 seconds
	go func() {
		for range ticker.C {
			pm.persistActiveSessions(hub)
		}
	}()
}

func (pm *PersistenceManager) persistActiveSessions(hub *Hub) {
	activeSessions, err := pm.redisClient.SMembers(pm.ctx, "active_sessions").Result()
	if err != nil {
		log.Printf("Error getting active sessions: %v", err)
		return
	}

	for _, sessionID := range activeSessions {
		session, err := pm.GetSessionFromRedis(sessionID)
		if err != nil {
			log.Printf("Error getting session %s from Redis: %v", sessionID, err)
			continue
		}
		
		if session != nil {
			err = pm.SaveSessionToMongoDB(session)
			if err != nil {
				log.Printf("Error saving session %s to MongoDB: %v", sessionID, err)
			} else {
				log.Printf("Persisted session %s to MongoDB", sessionID)
			}
		}
	}
}

// NewCRDT creates a new CRDT instance
func NewCRDT() *CRDT {
	return &CRDT{
		Characters: make([]Character, 0, 1000), // Pre-allocate for performance
		sizeBytes:  0,
	}
}

func (c *CRDT) CanAcceptOperation() bool {
	return atomic.LoadInt64(&c.sizeBytes) < maxCRDTSize
}


var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
	EnableCompression: true,
}

// ApplyOperation applies an operation to the CRDT
func (h *Hub) ApplyOperation(op Operation) {
	// Rate limit check
	atomic.AddInt64(&h.metrics.totalOps, 1)
	
	mutex := h.getSessionMutex(op.SessionID)
	mutex.Lock()
	defer mutex.Unlock()
	
	h.mu.RLock()
	session, exists := h.sessions[op.SessionID]
	h.mu.RUnlock()
	
	if !exists || session == nil {
		atomic.AddInt64(&h.metrics.failedOps, 1)
		return
	}
	
	// Check CRDT size limit
	if !session.CRDT.CanAcceptOperation() {
		atomic.AddInt64(&h.metrics.failedOps, 1)
		log.Printf("CRDT size limit reached for session %s", op.SessionID)
		return
	}
	
	// Apply the operation
	session.CRDT.ApplyOperation(op)
	session.UpdatedAt = time.Now()
	
	// Add to batch instead of immediate save
	h.persistence.AddOperationToBatch(op.SessionID, op)
	
	// Update session in Redis (with debouncing)
	go func() {
		h.persistence.SaveSessionToRedis(op.SessionID, session)
	}()
	
	// Publish to other clients
	h.persistence.PublishMessage(op.SessionID, "operation", op.UserID, op)
}

// ApplyOperations applies multiple operations in timestamp order
func (c *CRDT) ApplyOperations(operations []Operation) {
	// Sort operations by timestamp to ensure consistent application order
	sort.Slice(operations, func(i, j int) bool {
		return operations[i].Timestamp.Before(operations[j].Timestamp)
	})
	
	for _, op := range operations {
		c.ApplyOperation(op)
	}
}

// generatePosition generates a position between existing characters
func (c *CRDT) generatePosition(index int) float64 {
	visibleChars := c.getVisibleCharacters()
	
	if len(visibleChars) == 0 {
		return 1.0
	}

	if index <= 0 {
		// Add small random component to avoid conflicts
		return visibleChars[0].Position / 2.0 + (rand.Float64() * 0.0001)
	}
	
	if index >= len(visibleChars) {
		return visibleChars[len(visibleChars)-1].Position + 1.0 + (rand.Float64() * 0.0001)
	}
	
	// Between two positions with small random component
	prev := visibleChars[index-1].Position
	next := visibleChars[index].Position
	return (prev + next) / 2.0 + (rand.Float64() * 0.0001)
}

// getVisibleCharacters returns non-deleted characters sorted by position
func (c *CRDT) getVisibleCharacters() []Character {
	var visible []Character
	for _, char := range c.Characters {
		if !char.Deleted {
			visible = append(visible, char)
		}
	}
	sort.Slice(visible, func(i, j int) bool {
		return visible[i].Position < visible[j].Position
	})
	return visible
}

// sortCharacters sorts all characters by position
func (c *CRDT) sortCharacters() {
	sort.Slice(c.Characters, func(i, j int) bool {
		return c.Characters[i].Position < c.Characters[j].Position
	})
}

// markDeleted marks a character as deleted
func (c *CRDT) markDeleted(characterID string) {
	for i := range c.Characters {
		if c.Characters[i].ID == characterID {
			c.Characters[i].Deleted = true
			break
		}
	}
}

// GetText returns the current text content
func (c *CRDT) GetText() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	visible := c.getVisibleCharacters()
	var text string
	for _, char := range visible {
		text += char.Value
	}
	return text
}

// GetState returns the current CRDT state
func (c *CRDT) GetState() *CRDT {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	stateCopy := &CRDT{
		Characters: make([]Character, len(c.Characters)),
	}
	copy(stateCopy.Characters, c.Characters)
	return stateCopy
}

// Client represents a connected WebSocket client
type Client struct {
	ID           string
	UserID       string
	SessionID    string
	Conn         *websocket.Conn
	Send         chan []byte
	LastSeen     time.Time
	hub          *Hub
	subscription *redis.PubSub
	rateLimiter  *RateLimiter
	mu           sync.Mutex
}

// Hub maintains active clients and broadcasts messages
// Add sessionMutexes for per-session locking
type Hub struct {
	clients        map[*Client]bool
	sessions       map[string]*Session
	broadcast      chan []byte
	register       chan *Client
	unregister     chan *Client
	mu             sync.RWMutex
	persistence    *PersistenceManager
	sessionMutexes map[string]*sync.RWMutex
	connPool       *ConnectionPool
	
	// Performance metrics
	metrics struct {
		totalOps      int64
		failedOps     int64
		activeClients int64
	}
}


// NewHub creates a new Hub
func NewHub(persistence *PersistenceManager) *Hub {
	hub := &Hub{
		clients:        make(map[*Client]bool),
		sessions:       make(map[string]*Session),
		broadcast:      make(chan []byte, hubBroadcastBufferSize),
		register:       make(chan *Client, 100),
		unregister:     make(chan *Client, 100),
		persistence:    persistence,
		sessionMutexes: make(map[string]*sync.RWMutex),
		connPool:       NewConnectionPool(),
	}
	
	// Start multiple worker goroutines for better concurrency
	numWorkers := runtime.NumCPU()
	for i := 0; i < numWorkers; i++ {
		go hub.runWorker()
	}
	
	// Start periodic persistence
	persistence.StartPeriodicPersistence(hub)
	
	// Start cleanup routines
	hub.StartStaleUserCleanup(30*time.Second, 90*time.Second)
	hub.StartCleanupRoutine()
	
	return hub
}

func (h *Hub) runWorker() {
	for {
		select {
		case client := <-h.register:
			h.handleRegister(client)
		case client := <-h.unregister:
			h.handleUnregister(client)
		case message := <-h.broadcast:
			h.handleBroadcast(message)
		}
	}
}

func (h *Hub) handleRegister(client *Client) {
	// Check connection limits
	if !h.connPool.CanAddConnection(client.SessionID) {
		log.Printf("Connection limit reached for session %s", client.SessionID)
		client.Conn.Close()
		return
	}
	
	if !h.connPool.AddConnection(client.SessionID, client) {
		log.Printf("Failed to add connection to pool")
		client.Conn.Close()
		return
	}
	
	h.mu.Lock()
	h.clients[client] = true
	h.mu.Unlock()
	
	atomic.AddInt64(&h.metrics.activeClients, 1)
	client.LastSeen = time.Now()
	
	session := h.GetOrCreateSession(client.SessionID, "Collaborative Document", client.UserID)
	
	// Setup pub/sub for this client
	h.setupPubSubForClient(client)
	
	// Add user to active users
	h.addUserToSession(client.SessionID, client.UserID)
	
	// Send current state asynchronously
	go func() {
		state := session.CRDT.GetState()
		stateMsg, _ := json.Marshal(map[string]interface{}{
			"type": "state",
			"data": state,
		})
		
		select {
		case client.Send <- stateMsg:
		case <-time.After(5 * time.Second):
			log.Printf("Timeout sending state to client %s", client.ID)
		}
		
		// Send user list
		h.sendSessionUsersToClient(client.SessionID, client)
	}()
	
	// Notify others about new user
	h.persistence.PublishMessage(client.SessionID, "user_joined", client.UserID, map[string]string{
		"user_id": client.UserID,
	})
}

func (h *Hub) handleUnregister(client *Client) {
	h.mu.Lock()
	if _, ok := h.clients[client]; ok {
		delete(h.clients, client)
		close(client.Send)
		h.mu.Unlock()
		
		atomic.AddInt64(&h.metrics.activeClients, -1)
		h.connPool.RemoveConnection(client.SessionID, client)
		
		// Close subscription
		if client.subscription != nil {
			client.subscription.Close()
		}
		
		// Remove user from active users
		h.removeUserFromSession(client.SessionID, client.UserID)
		
		// Notify other users
		h.persistence.PublishMessage(client.SessionID, "user_left", client.UserID, map[string]string{
			"user_id": client.UserID,
		})
	} else {
		h.mu.Unlock()
	}
}

func (h *Hub) handleBroadcast(message []byte) {
	h.mu.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for client := range h.clients {
		clients = append(clients, client)
	}
	h.mu.RUnlock()
	
	// Send to clients without holding the lock
	for _, client := range clients {
		select {
		case client.Send <- message:
		default:
			// Client buffer full, disconnect
			go func(c *Client) {
				h.unregister <- c
			}(client)
		}
	}
}

// getSessionMutex returns or creates a mutex for a session
func (h *Hub) getSessionMutex(sessionID string) *sync.RWMutex {
	h.mu.Lock()
	defer h.mu.Unlock()
	mutex, exists := h.sessionMutexes[sessionID]
	if !exists {
		mutex = &sync.RWMutex{}
		h.sessionMutexes[sessionID] = mutex
	}
	return mutex
}

// CreateSession creates a new collaborative session
func (h *Hub) CreateSession(sessionID, title, createdBy string) *Session {
	sessionMutex := h.getSessionMutex(sessionID)
	sessionMutex.Lock()
	defer sessionMutex.Unlock()

	session := &Session{
		ID:          sessionID,
		Title:       title,
		CreatedBy:   createdBy,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		ActiveUsers: []string{},
		CRDT:        NewCRDT(),
	}

	h.mu.Lock()
	h.sessions[sessionID] = session
	h.mu.Unlock()
	
	// Save to Redis immediately
	h.persistence.SaveSessionToRedis(sessionID, session)
	
	return session
}

// GetOrCreateSession gets an existing session or creates a new one
// In GetOrCreateSession, check if session already has content
func (h *Hub) GetOrCreateSession(sessionID, title, userID string) *Session {
	sessionMutex := h.getSessionMutex(sessionID)
	sessionMutex.Lock()
	defer sessionMutex.Unlock()

	h.mu.RLock()
	session, exists := h.sessions[sessionID]
	h.mu.RUnlock()

	if exists {
		return session
	}

	// Try to get from Redis first
	session, err := h.persistence.GetSessionFromRedis(sessionID)
	if err != nil {
		log.Printf("Error getting session from Redis: %v", err)
	}

	if session == nil {
		// Try to get from MongoDB
		session, err = h.persistence.GetSessionFromMongoDB(sessionID)
		if err != nil {
			log.Printf("Error getting session from MongoDB: %v", err)
		}
	}

	// If still not found, create new session
	if session == nil {
		session = &Session{
			ID:          sessionID,
			Title:       title,
			CreatedBy:   userID,
			CreatedAt:   time.Now(),
			UpdatedAt:   time.Now(),
			ActiveUsers: []string{},
			CRDT:        NewCRDT(),
		}
	} else {
		// IMPORTANT: Initialize CRDT if it's nil
		if session.CRDT == nil {
			session.CRDT = NewCRDT()
		}
	}

	h.mu.Lock()
	h.sessions[sessionID] = session
	h.mu.Unlock()
	
	// Save to Redis
	h.persistence.SaveSessionToRedis(sessionID, session)
	
	return session
}

// setupPubSubForClient sets up Redis pub/sub for a client
func (h *Hub) setupPubSubForClient(client *Client) {
	channelName := fmt.Sprintf("session:%s", client.SessionID)
	pubsub := h.persistence.redisClient.Subscribe(h.persistence.ctx, channelName)
	client.subscription = pubsub
	
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Recovered from panic in pub/sub: %v", r)
			}
			pubsub.Close()
		}()
		
		ch := pubsub.Channel()
		for msg := range ch {
			var pubsubMsg PubSubMessage
			if err := json.Unmarshal([]byte(msg.Payload), &pubsubMsg); err != nil {
				continue
			}
			
			if pubsubMsg.UserID == client.UserID {
				continue
			}
			
			messageBytes, _ := json.Marshal(map[string]interface{}{
				"type": pubsubMsg.Type,
				"data": pubsubMsg.Data,
			})
			
			select {
			case client.Send <- messageBytes:
			case <-time.After(100 * time.Millisecond):
				// Don't block on slow clients
				continue
			}
		}
	}()
}

// Run starts the hub
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true
			client.LastSeen = time.Now()

			session := h.GetOrCreateSession(client.SessionID, "Collaborative Document", client.UserID)

			// Setup pub/sub for this client
			h.setupPubSubForClient(client)

			// Add user to active users
			h.addUserToSession(client.SessionID, client.UserID)

			// Send current state to new client (but don't apply operations again)
			state := session.CRDT.GetState()
			stateMsg, _ := json.Marshal(map[string]interface{}{
				"type": "state",
				"data": state,
			})
			select {
			case client.Send <- stateMsg:
			default:
				close(client.Send)
				delete(h.clients, client)
			}

			// Send current user list to the new client
			h.sendSessionUsersToClient(client.SessionID, client)

			// Notify other users about new user
			h.persistence.PublishMessage(client.SessionID, "user_joined", client.UserID, map[string]string{
				"user_id": client.UserID,
			})
		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.Send)
				
				// Close subscription
				if client.subscription != nil {
					client.subscription.Close()
				}
				
				// Remove user from active users
				h.removeUserFromSession(client.SessionID, client.UserID)
				
				// Notify other users about user leaving
				h.persistence.PublishMessage(client.SessionID, "user_left", client.UserID, map[string]string{
					"user_id": client.UserID,
				})
			}

		case message := <-h.broadcast:
			for client := range h.clients {
				select {
				case client.Send <- message:
				default:
					close(client.Send)
					delete(h.clients, client)
				}
			}
		}
	}
}

// In PersistenceManager, add methods for user set management
func (pm *PersistenceManager) AddUserToSession(sessionID, userID string) error {
	return pm.redisClient.SAdd(pm.ctx, fmt.Sprintf("session:%s:users", sessionID), userID).Err()
}

func (pm *PersistenceManager) RemoveUserFromSession(sessionID, userID string) error {
	return pm.redisClient.SRem(pm.ctx, fmt.Sprintf("session:%s:users", sessionID), userID).Err()
}

func (pm *PersistenceManager) GetSessionUsers(sessionID string) ([]string, error) {
	return pm.redisClient.SMembers(pm.ctx, fmt.Sprintf("session:%s:users", sessionID)).Result()
}

// Add user to session (thread-safe, with mutex)
func (h *Hub) addUserToSession(sessionID, userID string) {
	mutex := h.getSessionMutex(sessionID)
	mutex.Lock()
	defer mutex.Unlock()

	h.persistence.AddUserToSession(sessionID, userID)
	users, _ := h.persistence.GetSessionUsers(sessionID)

	h.mu.Lock()
	session, exists := h.sessions[sessionID]
	h.mu.Unlock()
	if !exists || session == nil {
		return
	}
	session.ActiveUsers = users
	session.UpdatedAt = time.Now()
	h.persistence.SaveSessionToRedis(sessionID, session)
	// Always broadcast updated user list to session clients
	h.broadcastSessionUsers(sessionID)
}

// Update removeUserFromSession to use Redis set for global user list
func (h *Hub) removeUserFromSession(sessionID, userID string) {
	mutex := h.getSessionMutex(sessionID)
	mutex.Lock()
	defer mutex.Unlock()

	h.persistence.RemoveUserFromSession(sessionID, userID)
	users, _ := h.persistence.GetSessionUsers(sessionID)

	h.mu.Lock()
	session, exists := h.sessions[sessionID]
	h.mu.Unlock()
	if !exists || session == nil {
		return
	}
	session.ActiveUsers = users
	session.UpdatedAt = time.Now()
	h.persistence.SaveSessionToRedis(sessionID, session)
	// Always broadcast updated user list to session clients
	h.broadcastSessionUsers(sessionID)
}

// Broadcast updated user list to all clients in the session
func (h *Hub) broadcastSessionUsers(sessionID string) {
	h.mu.RLock()
	session, exists := h.sessions[sessionID]
	h.mu.RUnlock()
	if !exists || session == nil {
		return
	}
	msg, _ := json.Marshal(map[string]interface{}{
		"type": "users",
		"data": session.ActiveUsers,
	})
	for client := range h.clients {
		if client.SessionID == sessionID {
			select {
			case client.Send <- msg:
			default:
				close(client.Send)
				delete(h.clients, client)
			}
		}
	}
}

// Add this helper to send user list to a single client
func (h *Hub) sendSessionUsersToClient(sessionID string, client *Client) {
	h.mu.RLock()
	session, exists := h.sessions[sessionID]
	h.mu.RUnlock()
	if !exists || session == nil {
		return
	}
	msg, _ := json.Marshal(map[string]interface{}{
		"type": "users",
		"data": session.ActiveUsers,
	})
	select {
	case client.Send <- msg:
	default:
		close(client.Send)
		delete(h.clients, client)
	}
}

// ApplyOperation applies an operation to the session's CRDT, persists it, and publishes it.


// WebSocket message structure
type Message struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}




// ApplyOperation applies a single operation to the CRDT (CRDT-level)
func (c *CRDT) ApplyOperation(op Operation) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch op.Type {
	case "insert":
		// Insert character at the given position
		pos := c.generatePosition(op.Position)
		char := Character{
			ID:        op.CharacterID,
			Value:     op.Character,
			Position:  pos,
			Timestamp: op.Timestamp,
			UserID:    op.UserID,
			Deleted:   false,
		}
		c.Characters = append(c.Characters, char)
		c.sortCharacters()
		atomic.AddInt64(&c.sizeBytes, int64(len(char.Value)))
	case "delete":
		// Mark character as deleted
		c.markDeleted(op.CharacterID)
	}
}

func handleWebSocket(hub *Hub, w http.ResponseWriter, r *http.Request) {
	// Check global connection limit
	if atomic.LoadInt32(&globalConnectionCount) >= maxTotalConnections {
		http.Error(w, "Server at capacity", http.StatusServiceUnavailable)
		return
	}
	
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("WebSocket upgrade error:", err)
		return
	}
	
	atomic.AddInt32(&globalConnectionCount, 1)
	defer atomic.AddInt32(&globalConnectionCount, -1)
	
	userID := r.URL.Query().Get("user_id")
	sessionID := r.URL.Query().Get("session_id")
	
	if userID == "" {
		userID = fmt.Sprintf("user_%d", time.Now().UnixNano())
	}
	
	if sessionID == "" {
		sessionID = "default"
	}
	
	client := &Client{
		ID:          fmt.Sprintf("client_%d", time.Now().UnixNano()),
		UserID:      userID,
		SessionID:   sessionID,
		Conn:        conn,
		Send:        make(chan []byte, clientSendBufferSize),
		LastSeen:    time.Now(),
		hub:         hub,
		rateLimiter: NewRateLimiter(maxOpsPerSecondPerClient),
	}
	
	hub.register <- client
	
	// Start goroutines with proper cleanup
	var wg sync.WaitGroup
	wg.Add(2)
	
	go func() {
		defer wg.Done()
		client.writePump()
	}()
	
	go func() {
		defer wg.Done()
		client.readPump(hub)
	}()
	
	// Wait for both pumps to finish
	go func() {
		wg.Wait()
		hub.unregister <- client
		conn.Close()
	}()
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingInterval)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()
	for {
		select {
		case message, ok := <-c.Send:
			c.Conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			// Batch multiple messages if available
			c.Conn.WriteMessage(websocket.TextMessage, message)
			// Try to batch more messages
			n := len(c.Send)
			for i := 0; i < n && i < 10; i++ {
				select {
				case msg := <-c.Send:
					c.Conn.WriteMessage(websocket.TextMessage, msg)
				default:
					break
				}
			}
		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// readPump reads messages from the WebSocket connection and processes them (Client-level)
func (c *Client) readPump(hub *Hub) {
	defer func() {
		c.Conn.Close()
	}()
	c.Conn.SetReadLimit(65536)
	_ = c.Conn.SetReadDeadline(time.Now().Add(readTimeout))
	c.Conn.SetPongHandler(func(string) error {
		_ = c.Conn.SetReadDeadline(time.Now().Add(readTimeout))
		return nil
	})
	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			break
		}
		c.LastSeen = time.Now()
		var msg Message
		if err := json.Unmarshal(message, &msg); err != nil {
			continue
		}
		switch msg.Type {
		case "operation":
			// Rate limit per client
			if !c.rateLimiter.Allow() {
				continue
			}
			opData, _ := json.Marshal(msg.Data)
			var op Operation
			if err := json.Unmarshal(opData, &op); err != nil {
				continue
			}
			op.UserID = c.UserID
			op.SessionID = c.SessionID
			op.Timestamp = time.Now()
			hub.ApplyOperation(op)
		}
	}
}

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	// Initialize persistence manager
	persistence := NewPersistenceManager()
	defer persistence.mongoClient.Disconnect(persistence.ctx)
	defer persistence.redisClient.Close()

	// Initialize hub
	hub := NewHub(persistence)
	go hub.Run()
	// Start stale user cleanup: every 30s, remove users idle > 90s
	hub.StartStaleUserCleanup(30*time.Second, 90*time.Second)

	// Setup routes
	r := mux.NewRouter()

	// Serve static files
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})

	// WebSocket endpoint
	r.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		handleWebSocket(hub, w, r)
	})

	r.HandleFunc("/api/session_users", sessionUsersAPIHandler(hub)).Methods("GET")

	// API endpoints
	r.HandleFunc("/api/sessions", func(w http.ResponseWriter, r *http.Request) {
		sessions, err := persistence.GetAllSessions()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(sessions)
	}).Methods("GET")

	r.HandleFunc("/api/sessions/{id}/text", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		sessionID := vars["id"]
		mutex := hub.getSessionMutex(sessionID)
		mutex.RLock()
		hub.mu.RLock()
		session, exists := hub.sessions[sessionID]
		hub.mu.RUnlock()
		mutex.RUnlock()
		if !exists || session == nil {
			// Try to get from persistence
			session, _ = persistence.GetSessionFromRedis(sessionID)
			if session == nil {
				session, _ = persistence.GetSessionFromMongoDB(sessionID)
			}
		}
		if session == nil {
			http.Error(w, "Session not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		text := session.CRDT.GetText()
		json.NewEncoder(w).Encode(map[string]string{"text": text})
	}).Methods("GET")

	// Create a new session endpoint
	r.HandleFunc("/api/sessions", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			var req struct {
				Title     string `json:"title"`
				CreatedBy string `json:"created_by"`
			}
			
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "Invalid JSON", http.StatusBadRequest)
				return
			}
			
			sessionID := fmt.Sprintf("session_%d", time.Now().UnixNano())
			session := hub.CreateSession(sessionID, req.Title, req.CreatedBy)
			
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(session)
			return
		}
		
		// GET request - list all sessions
		sessions, err := persistence.GetAllSessions()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(sessions)
	})

	// Get specific session endpoint
	r.HandleFunc("/api/sessions/{id}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		sessionID := vars["id"]
		
		sessionMutex := hub.getSessionMutex(sessionID)
		sessionMutex.RLock()
		defer sessionMutex.RUnlock()
		
		hub.mu.RLock()
		session, exists := hub.sessions[sessionID]
		hub.mu.RUnlock()
		
		if !exists {
			// Try to get from persistence
			session, _ = persistence.GetSessionFromRedis(sessionID)
			if session == nil {
				session, _ = persistence.GetSessionFromMongoDB(sessionID)
			}
		}
		
		if session == nil {
			http.Error(w, "Session not found", http.StatusNotFound)
			return
		}
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(session)
	}).Methods("GET")

	// Get session operations endpoint for debugging/sync
	r.HandleFunc("/api/sessions/{id}/operations", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		sessionID := vars["id"]
		
		// Get optional 'since' parameter for incremental sync
		sinceParam := r.URL.Query().Get("since")
		var since time.Time
		if sinceParam != "" {
			if parsed, err := time.Parse(time.RFC3339, sinceParam); err == nil {
				since = parsed
			}
		} else {
			since = time.Now().Add(-1 * time.Hour) // Default to last hour
		}
		
		operations, err := persistence.GetOperationsFromRedis(sessionID, since)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"session_id": sessionID,
			"operations": operations,
			"since":      since,
			"count":      len(operations),
		})
	}).Methods("GET")

	// Health check endpoint
	r.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		// Check Redis connection
		_, redisErr := persistence.redisClient.Ping(persistence.ctx).Result()
		
		// Check MongoDB connection
		mongoErr := persistence.mongoClient.Ping(persistence.ctx, nil)
		
		status := map[string]interface{}{
			"status":    "ok",
			"timestamp": time.Now(),
			"services": map[string]interface{}{
				"redis":   redisErr == nil,
				"mongodb": mongoErr == nil,
			},
		}
		
		if redisErr != nil || mongoErr != nil {
			status["status"] = "degraded"
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	}).Methods("GET")

	// CORS middleware
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			
			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}
			
			next.ServeHTTP(w, r)
		})
	})

	server := &http.Server{
		Addr:         ":8080",
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
		MaxHeaderBytes: 1 << 20, // 1 MB
	}
	
	// Start server
	fmt.Println("CRDT Collaborative Editor Server starting on :8080")
	fmt.Println("WebSocket endpoint: ws://localhost:8080/ws")
	fmt.Println("Health check: http://localhost:8080/health")
	fmt.Printf("Running with %d CPU cores\n", runtime.NumCPU())

	log.Fatal(server.ListenAndServe())
}

// Additional helper methods that might be useful

// GetSessionStats returns statistics about a session
func (h *Hub) GetSessionStats(sessionID string) map[string]interface{} {
	sessionMutex := h.getSessionMutex(sessionID)
	sessionMutex.RLock()
	defer sessionMutex.RUnlock()
	
	h.mu.RLock()
	session, exists := h.sessions[sessionID]
	h.mu.RUnlock()
	
	if !exists {
		return nil
	}
	
	stats := map[string]interface{}{
		"session_id":       sessionID,
		"title":           session.Title,
		"created_by":      session.CreatedBy,
		"created_at":      session.CreatedAt,
		"updated_at":      session.UpdatedAt,
		"active_users":    len(session.ActiveUsers),
		"user_list":       session.ActiveUsers,
		"character_count": len(session.CRDT.Characters),
		"text_length":     len(session.CRDT.GetText()),
	}
	
	// Count deleted vs active characters
	deleted := 0
	for _, char := range session.CRDT.Characters {
		if char.Deleted {
			deleted++
		}
	}
	stats["deleted_characters"] = deleted
	stats["active_characters"] = len(session.CRDT.Characters) - deleted
	
	return stats
}

// CleanupInactiveSessions removes sessions that haven't been active for a while
func (h *Hub) CleanupInactiveSessions(maxAge time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	
	cutoff := time.Now().Add(-maxAge)
	
	for sessionID, session := range h.sessions {
		if session.UpdatedAt.Before(cutoff) && len(session.ActiveUsers) == 0 {
			// Save to MongoDB before removing from memory
			h.persistence.SaveSessionToMongoDB(session)
			
			// Remove from memory
			delete(h.sessions, sessionID)
			delete(h.sessionMutexes, sessionID)
			
			log.Printf("Cleaned up inactive session: %s", sessionID)
		}
	}
}

// StartCleanupRoutine starts a routine to periodically clean up inactive sessions
func (h *Hub) StartCleanupRoutine() {
	ticker := time.NewTicker(5 * time.Minute) // Run every 5 minutes
	go func() {
		for range ticker.C {
			h.CleanupInactiveSessions(30 * time.Minute) // Remove sessions inactive for 30 minutes
		}
	}()
}

// Add a background goroutine to clean up stale users
type staleUser struct {
	client *Client
	lastSeen time.Time
}

func (h *Hub) StartStaleUserCleanup(interval, maxIdle time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			now := time.Now()
			var toRemove []*Client
			h.mu.RLock()
			for client := range h.clients {
				if now.Sub(client.LastSeen) > maxIdle {
					toRemove = append(toRemove, client)
				}
			}
			h.mu.RUnlock()
			for _, client := range toRemove {
				h.unregister <- client
			}
		}
	}()
}


// UserInfo represents a user for the API response
type UserInfo struct {
	UserID string `json:"user_id"`
	Name   string `json:"name"`
}

// /api/session_users?session_id=...&offset=...&limit=...
func sessionUsersAPIHandler(hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sessionID := r.URL.Query().Get("session_id")
		offsetStr := r.URL.Query().Get("offset")
		limitStr := r.URL.Query().Get("limit")

		offset := 0
		limit := 10
		if o, err := strconv.Atoi(offsetStr); err == nil {
			offset = o
		}
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}

		// Get session from memory (live sessions)
		sessionMutex := hub.getSessionMutex(sessionID)
		sessionMutex.RLock()
		hub.mu.RLock()
		session, exists := hub.sessions[sessionID]
		hub.mu.RUnlock()
		sessionMutex.RUnlock()

		var users []UserInfo
		if exists && session != nil {
			for _, userID := range session.ActiveUsers {
				users = append(users, UserInfo{
					UserID: userID,
					Name:   userID, // Replace with actual name if you store it
				})
			}
		}

		// Pagination
		total := len(users)
		start := offset
		if start > total {
			start = total
		}
		end := start + limit
		if end > total {
			end = total
		}
		pagedUsers := users[start:end]

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"users": pagedUsers,
			"count": total,
		})
	}
}