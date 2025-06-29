package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Operation represents a single edit operation
type Operation struct {
	ID        string    `json:"id" bson:"id"`
	Type      string    `json:"type" bson:"type"` // "insert" or "delete"
	Position  int       `json:"position" bson:"position"`
	Character string    `json:"character,omitempty" bson:"character,omitempty"`
	Timestamp time.Time `json:"timestamp" bson:"timestamp"`
	UserID    string    `json:"user_id" bson:"user_id"`
	SessionID string    `json:"session_id" bson:"session_id"`
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
type PersistenceManager struct {
	redisClient *redis.Client
	mongoClient *mongo.Client
	mongoDB     *mongo.Database
	ctx         context.Context
}

// NewPersistenceManager creates a new persistence manager
func NewPersistenceManager() *PersistenceManager {
	ctx := context.Background()

	// Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	// Test Redis connection
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatal("Failed to connect to Redis:", err)
	}

	// MongoDB client
	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://appuser:apppassword@localhost:27017/admin"))
	if err != nil {
		log.Fatal("Failed to connect to MongoDB:", err)
	}

	// Test MongoDB connection
	err = mongoClient.Ping(ctx, nil)
	if err != nil {
		log.Fatal("Failed to ping MongoDB:", err)
	}

	mongoDB := mongoClient.Database("crdt_editor")

	return &PersistenceManager{
		redisClient: rdb,
		mongoClient: mongoClient,
		mongoDB:     mongoDB,
		ctx:         ctx,
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
		Characters: make([]Character, 0),
	}
}

// ApplyOperation applies an operation to the CRDT
func (c *CRDT) ApplyOperation(op Operation) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch op.Type {
	case "insert":
		pos := c.generatePosition(op.Position)
		char := Character{
			ID:        op.ID,
			Value:     op.Character,
			Position:  pos,
			Timestamp: op.Timestamp,
			UserID:    op.UserID,
			Deleted:   false,
		}
		c.Characters = append(c.Characters, char)
		c.sortCharacters()
	case "delete":
		c.markDeleted(op.ID)
	}
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
		return visibleChars[0].Position / 2.0
	}
	
	if index >= len(visibleChars) {
		return visibleChars[len(visibleChars)-1].Position + 1.0
	}
	
	// Between two positions
	prev := visibleChars[index-1].Position
	next := visibleChars[index].Position
	return (prev + next) / 2.0
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
func (c *CRDT) markDeleted(id string) {
	for i := range c.Characters {
		if c.Characters[i].ID == id {
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
}

// Hub maintains active clients and broadcasts messages
// Add sessionMutexes for per-session locking
type Hub struct {
	clients       map[*Client]bool
	sessions      map[string]*Session
	broadcast     chan []byte
	register      chan *Client
	unregister    chan *Client
	mu            sync.RWMutex
	persistence   *PersistenceManager
	sessionMutexes map[string]*sync.RWMutex // NEW: per-session mutexes
}

// NewHub creates a new Hub
func NewHub(persistence *PersistenceManager) *Hub {
	hub := &Hub{
		clients:     make(map[*Client]bool),
		sessions:    make(map[string]*Session),
		broadcast:   make(chan []byte),
		register:    make(chan *Client),
		unregister:  make(chan *Client),
		persistence: persistence,
		sessionMutexes: make(map[string]*sync.RWMutex), // NEW
	}
	// Start periodic persistence
	persistence.StartPeriodicPersistence(hub)
	return hub
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
	// Subscribe to session channel
	channelName := fmt.Sprintf("session:%s", client.SessionID)
	pubsub := h.persistence.redisClient.Subscribe(h.persistence.ctx, channelName)
	client.subscription = pubsub
	
	// Listen for messages in a separate goroutine
	go func() {
		defer pubsub.Close()
		
		ch := pubsub.Channel()
		for msg := range ch {
			var pubsubMsg PubSubMessage
			if err := json.Unmarshal([]byte(msg.Payload), &pubsubMsg); err != nil {
				log.Printf("Error unmarshaling pub/sub message: %v", err)
				continue
			}
			
			// Don't send messages back to the sender
			if pubsubMsg.UserID == client.UserID {
				continue
			}
			
			// Forward message to client
			messageBytes, _ := json.Marshal(map[string]interface{}{
				"type": pubsubMsg.Type,
				"data": pubsubMsg.Data,
			})
			
			select {
			case client.Send <- messageBytes:
			default:
				// Client channel is full, close connection
				h.unregister <- client
				return
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

			// Get recent operations and apply them to ensure consistency
			since := time.Now().Add(-1 * time.Hour) // Get operations from last hour
			recentOps, err := h.persistence.GetOperationsFromRedis(client.SessionID, since)
			if err == nil && len(recentOps) > 0 {
				sessionMutex := h.getSessionMutex(client.SessionID)
				sessionMutex.Lock()
				session.CRDT.ApplyOperations(recentOps)
				sessionMutex.Unlock()
			}

			// Send current state to new client
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

// ApplyOperation applies an operation and publishes it via Redis pub/sub
func (h *Hub) ApplyOperation(op Operation) {
	mutex := h.getSessionMutex(op.SessionID)
	mutex.Lock()
	session, exists := h.sessions[op.SessionID]
	if exists && session != nil {
		session.CRDT.ApplyOperation(op)
		session.UpdatedAt = time.Now()
		h.persistence.SaveOperationToRedis(op.SessionID, op)
		h.persistence.SaveSessionToRedis(op.SessionID, session)
	}
	mutex.Unlock()
	// Broadcast operation to all clients in the same session
	opMsg, _ := json.Marshal(map[string]interface{}{
		"type": "operation",
		"data": op,
	})
	for client := range h.clients {
		if client.SessionID == op.SessionID {
			select {
			case client.Send <- opMsg:
			default:
				close(client.Send)
				delete(h.clients, client)
			}
		}
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins in this example
	},
}

// WebSocket message structure
type Message struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

func handleWebSocket(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("WebSocket upgrade error:", err)
		return
	}

	userID := r.URL.Query().Get("user_id")
	sessionID := r.URL.Query().Get("session_id")
	
	if userID == "" {
		userID = fmt.Sprintf("user_%d", time.Now().UnixNano())
	}
	
	if sessionID == "" {
		sessionID = "default"
	}

	client := &Client{
		ID:        fmt.Sprintf("client_%d", time.Now().UnixNano()),
		UserID:    userID,
		SessionID: sessionID,
		Conn:      conn,
		Send:      make(chan []byte, 256),
		LastSeen:  time.Now(),
		hub:       hub,
	}

	hub.register <- client

	go client.writePump()
	go client.readPump(hub)
}

func (c *Client) readPump(hub *Hub) {
	defer func() {
		hub.unregister <- c
		c.Conn.Close()
	}()

	// Set read deadline and pong handler
	c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		c.LastSeen = time.Now()
		return nil
	})

	for {
		var msg Message
		err := c.Conn.ReadJSON(&msg)
		if err != nil {
			break
		}

		c.LastSeen = time.Now()

		switch msg.Type {
		case "operation":
			opData, _ := json.Marshal(msg.Data)
			var op Operation
			json.Unmarshal(opData, &op)
			op.UserID = c.UserID
			op.SessionID = c.SessionID
			op.Timestamp = time.Now()
			hub.ApplyOperation(op)
		case "ping":
			// Respond to ping with pong
			pongMsg, _ := json.Marshal(map[string]string{"type": "pong"})
			c.Send <- pongMsg
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(54 * time.Second)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.Send:
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			c.Conn.WriteMessage(websocket.TextMessage, message)

		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func main() {
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

	// Start server
	fmt.Println("CRDT Collaborative Editor Server starting on :8080")
	fmt.Println("WebSocket endpoint: ws://localhost:8080/ws")
	fmt.Println("Health check: http://localhost:8080/health")
	
	log.Fatal(http.ListenAndServe(":8080", r))
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