// test_client.go
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// TestClient represents a simulated user
type TestClient struct {
	ID            string
	UserID        string
	SessionID     string
	conn          *websocket.Conn
	currentText   string
	mu            sync.RWMutex
	operationsSent int64
	operationsRecv int64
	errors        int64
	latencies     []time.Duration
	connected     int32  // Changed to atomic int32
	stopChan      chan struct{}
	stopOnce      sync.Once  // Ensures Disconnect is called only once
}

// TestScenario defines different editing patterns
type TestScenario struct {
	Name            string
	InsertProbability float64
	DeleteProbability float64
	BurstProbability  float64
	ThinkTime        time.Duration
	BurstSize        int
}

var scenarios = map[string]TestScenario{
	"normal": {
		Name:              "Normal Typing",
		InsertProbability: 0.8,
		DeleteProbability: 0.2,
		BurstProbability:  0.1,
		ThinkTime:         100 * time.Millisecond,
		BurstSize:         5,
	},
	"aggressive": {
		Name:              "Aggressive Editing",
		InsertProbability: 0.7,
		DeleteProbability: 0.3,
		BurstProbability:  0.3,
		ThinkTime:         50 * time.Millisecond,
		BurstSize:         10,
	},
	"code": {
		Name:              "Code Writing",
		InsertProbability: 0.9,
		DeleteProbability: 0.1,
		BurstProbability:  0.4,
		ThinkTime:         200 * time.Millisecond,
		BurstSize:         20,
	},
	"review": {
		Name:              "Document Review",
		InsertProbability: 0.3,
		DeleteProbability: 0.7,
		BurstProbability:  0.1,
		ThinkTime:         500 * time.Millisecond,
		BurstSize:         3,
	},
}

// SimulationConfig holds the test configuration
type SimulationConfig struct {
	ServerURL      string
	NumUsers       int
	SessionID      string
	Duration       time.Duration
	Scenario       string
	RampUpTime     time.Duration
	MetricsInterval time.Duration
}

// SimulationMetrics tracks overall performance
type SimulationMetrics struct {
	TotalOperationsSent int64
	TotalOperationsRecv int64
	TotalErrors         int64
	ConnectedClients    int64
	StartTime          time.Time
	mu                 sync.RWMutex
	clientMetrics      map[string]*ClientMetrics
}

type ClientMetrics struct {
	OperationsSent int64
	OperationsRecv int64
	Errors         int64
	AvgLatency     time.Duration
}

// NewTestClient creates a new test client
func NewTestClient(userID, sessionID string) *TestClient {
	return &TestClient{
		ID:           fmt.Sprintf("test_client_%s", userID),
		UserID:       userID,
		SessionID:    sessionID,
		currentText:  "",
		latencies:    make([]time.Duration, 0),
		stopChan:     make(chan struct{}),
		connected:    0,
	}
}

func (tc *TestClient) IsConnected() bool {
	return atomic.LoadInt32(&tc.connected) == 1
}

// Connect establishes WebSocket connection
func (tc *TestClient) Connect(serverURL string) error {
	u, err := url.Parse(serverURL)
	if err != nil {
		return err
	}
	
	u.Scheme = "ws"
	u.Path = "/ws"
	q := u.Query()
	q.Set("user_id", tc.UserID)
	q.Set("session_id", tc.SessionID)
	u.RawQuery = q.Encode()
	
	tc.conn, _, err = websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return err
	}
	
	atomic.StoreInt32(&tc.connected, 1)
	atomic.AddInt64(&metrics.ConnectedClients, 1)
	
	// Start read pump
	go tc.readPump()
	
	// Start ping routine
	go tc.pingRoutine()
	
	return nil
}

// Disconnect closes the connection
func (tc *TestClient) Disconnect() {
	tc.stopOnce.Do(func() {
		atomic.StoreInt32(&tc.connected, 0)
		close(tc.stopChan)
		if tc.conn != nil {
			tc.conn.Close()
		}
		atomic.AddInt64(&metrics.ConnectedClients, -1)
	})
}


// readPump reads messages from server
func (tc *TestClient) readPump() {
	defer tc.Disconnect()
	
	// Set read deadline
	tc.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	tc.conn.SetPongHandler(func(string) error {
		tc.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	
	for {
		select {
		case <-tc.stopChan:
			return
		default:
			var msg map[string]interface{}
			err := tc.conn.ReadJSON(&msg)
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Printf("Client %s websocket error: %v", tc.UserID, err)
				}
				atomic.AddInt64(&tc.errors, 1)
				atomic.AddInt64(&metrics.TotalErrors, 1)
				return
			}
			
			msgType, _ := msg["type"].(string)
			
			switch msgType {
			case "operation":
				atomic.AddInt64(&tc.operationsRecv, 1)
				atomic.AddInt64(&metrics.TotalOperationsRecv, 1)
				tc.handleOperation(msg["data"])
			case "state":
				tc.handleState(msg["data"])
			case "users":
				// Handle user list update
			case "pong":
				// Handle pong response
			}
		}
	}
}

// pingRoutine sends periodic pings
func (tc *TestClient) pingRoutine() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-tc.stopChan:
			return
		case <-ticker.C:
			if tc.IsConnected() {
				if err := tc.sendMessage(map[string]string{"type": "ping"}); err != nil {
					log.Printf("Client %s ping failed: %v", tc.UserID, err)
					tc.Disconnect()
					return
				}
			}
		}
	}
}

// handleOperation processes incoming operations
func (tc *TestClient) handleOperation(data interface{}) {
	// In a real implementation, you'd update your local CRDT state
	// For testing, we'll just track that we received it
}

// handleState processes state updates
func (tc *TestClient) handleState(data interface{}) {
	// Update local state representation
	if stateData, ok := data.(map[string]interface{}); ok {
		if chars, ok := stateData["characters"].([]interface{}); ok {
			tc.mu.Lock()
			// Rebuild text from characters
			text := ""
			for _, char := range chars {
				if charMap, ok := char.(map[string]interface{}); ok {
					if !charMap["deleted"].(bool) {
						text += charMap["value"].(string)
					}
				}
			}
			tc.currentText = text
			tc.mu.Unlock()
		}
	}
}

// sendMessage sends a message to the server
func (tc *TestClient) sendMessage(msg interface{}) error {
	if !tc.IsConnected() {
		return fmt.Errorf("not connected")
	}
	
	tc.mu.Lock()
	defer tc.mu.Unlock()
	
	if tc.conn == nil {
		return fmt.Errorf("connection is nil")
	}
	
	// Set write deadline
	tc.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return tc.conn.WriteJSON(msg)
}

// generateOperation generates a random operation based on scenario
func (tc *TestClient) generateOperation(scenario TestScenario) map[string]interface{} {
	tc.mu.RLock()
	textLen := len(tc.currentText)
	tc.mu.RUnlock()
	
	r := rand.Float64()
	
	if r < scenario.InsertProbability || textLen == 0 {
		// Insert operation
		position := rand.Intn(textLen + 1)
		char := generateRandomChar()
		
		return map[string]interface{}{
			"type": "operation",
			"data": map[string]interface{}{
				"id":        fmt.Sprintf("op_%s_%d", tc.UserID, time.Now().UnixNano()),
				"type":      "insert",
				"position":  position,
				"character": char,
			},
		}
	} else {
		// Delete operation
		if textLen > 0 {
			position := rand.Intn(textLen)
			return map[string]interface{}{
				"type": "operation",
				"data": map[string]interface{}{
					"id":       fmt.Sprintf("op_%s_%d", tc.UserID, time.Now().UnixNano()),
					"type":     "delete",
					"position": position,
				},
			}
		}
	}
	
	// Fallback to insert if text is empty
	return map[string]interface{}{
		"type": "operation",
		"data": map[string]interface{}{
			"id":        fmt.Sprintf("op_%s_%d", tc.UserID, time.Now().UnixNano()),
			"type":      "insert",
			"position":  0,
			"character": generateRandomChar(),
		},
	}
}

// SimulateEditing simulates user editing behavior
func (tc *TestClient) SimulateEditing(scenario TestScenario, duration time.Duration) {
	endTime := time.Now().Add(duration)
	
	for time.Now().Before(endTime) {
		select {
		case <-tc.stopChan:
			return
		default:
			if !tc.IsConnected() {
				return
			}
			
			// Check for burst mode
			operations := 1
			if rand.Float64() < scenario.BurstProbability {
				operations = scenario.BurstSize
			}
			
			// Send operations
			for i := 0; i < operations; i++ {
				start := time.Now()
				op := tc.generateOperation(scenario)
				
				if err := tc.sendMessage(op); err != nil {
					atomic.AddInt64(&tc.errors, 1)
					atomic.AddInt64(&metrics.TotalErrors, 1)
					if !tc.IsConnected() {
						return
					}
				} else {
					atomic.AddInt64(&tc.operationsSent, 1)
					atomic.AddInt64(&metrics.TotalOperationsSent, 1)
					
					// Track latency
					tc.mu.Lock()
					tc.latencies = append(tc.latencies, time.Since(start))
					tc.mu.Unlock()
				}
				
				// Small delay between burst operations
				if i < operations-1 {
					time.Sleep(10 * time.Millisecond)
				}
			}
			
			// Think time between operations/bursts
			time.Sleep(scenario.ThinkTime)
		}
	}
}

// Helper functions
func generateRandomChar() string {
	chars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 .,!?\n"
	return string(chars[rand.Intn(len(chars))])
}

var metrics *SimulationMetrics

// RunSimulation runs the multi-user simulation
func RunSimulation(config SimulationConfig) {
	metrics = &SimulationMetrics{
		StartTime:     time.Now(),
		clientMetrics: make(map[string]*ClientMetrics),
	}
	
	scenario, exists := scenarios[config.Scenario]
	if !exists {
		log.Fatalf("Unknown scenario: %s", config.Scenario)
	}
	
	log.Printf("Starting simulation with %d users, scenario: %s", config.NumUsers, scenario.Name)
	
	// Create clients
	clients := make([]*TestClient, config.NumUsers)
	var wg sync.WaitGroup
	
	// Start metrics reporter
	stopMetrics := make(chan struct{})
	go func() {
		reportMetrics(config.MetricsInterval, stopMetrics)
	}()
	
	// Ramp up users gradually
	usersPerSecond := float64(config.NumUsers) / config.RampUpTime.Seconds()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	
	userIndex := 0
	
	// Ramp up phase
	for range ticker.C {
		usersToStart := int(usersPerSecond)
		if userIndex+usersToStart > config.NumUsers {
			usersToStart = config.NumUsers - userIndex
		}
		
		for i := 0; i < usersToStart; i++ {
			userID := fmt.Sprintf("test_user_%d", userIndex)
			client := NewTestClient(userID, config.SessionID)
			clients[userIndex] = client
			
			wg.Add(1)
			go func(c *TestClient, idx int) {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						log.Printf("Client %s panicked: %v", c.UserID, r)
					}
				}()
				
				// Connect with retry
				var err error
				for retries := 0; retries < 3; retries++ {
					err = c.Connect(config.ServerURL)
					if err == nil {
						break
					}
					log.Printf("Client %s connection attempt %d failed: %v", c.UserID, retries+1, err)
					time.Sleep(time.Second)
				}
				
				if err != nil {
					log.Printf("Client %s failed to connect after retries: %v", c.UserID, err)
					return
				}
				
				// Wait a bit for initial state sync
				time.Sleep(500 * time.Millisecond)
				
				// Start editing
				c.SimulateEditing(scenario, config.Duration)
			}(client, userIndex)
			
			userIndex++
		}
		
		if userIndex >= config.NumUsers {
			break
		}
	}
	
	// Wait for all clients to finish
	wg.Wait()
	
	// Stop metrics reporter
	close(stopMetrics)
	time.Sleep(time.Second)
	
	// Cleanup
	for _, client := range clients {
		if client != nil && client.IsConnected() {
			client.Disconnect()
		}
	}
	
	// Final report
	printFinalReport(clients)
}

// reportMetrics periodically reports metrics
func reportMetrics(interval time.Duration, stop chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			elapsed := time.Since(metrics.StartTime)
			sent := atomic.LoadInt64(&metrics.TotalOperationsSent)
			recv := atomic.LoadInt64(&metrics.TotalOperationsRecv)
			errors := atomic.LoadInt64(&metrics.TotalErrors)
			connected := atomic.LoadInt64(&metrics.ConnectedClients)
			
			opsPerSec := float64(sent) / elapsed.Seconds()
			
			log.Printf("[%s] Connected: %d, Sent: %d, Recv: %d, Errors: %d, Ops/sec: %.2f",
				elapsed.Round(time.Second),
				connected,
				sent,
				recv,
				errors,
				opsPerSec,
			)
		}
	}
}

// printFinalReport prints the final simulation report
func printFinalReport(clients []*TestClient) {
	fmt.Println("\n=== SIMULATION REPORT ===")
	fmt.Printf("Duration: %s\n", time.Since(metrics.StartTime))
	fmt.Printf("Total Operations Sent: %d\n", metrics.TotalOperationsSent)
	fmt.Printf("Total Operations Received: %d\n", metrics.TotalOperationsRecv)
	fmt.Printf("Total Errors: %d\n", metrics.TotalErrors)
	
	// Calculate per-client statistics
	var totalLatency time.Duration
	var latencyCount int
	
	for _, client := range clients {
		if client != nil {
			client.mu.RLock()
			for _, lat := range client.latencies {
				totalLatency += lat
				latencyCount++
			}
			client.mu.RUnlock()
		}
	}
	
	if latencyCount > 0 {
		avgLatency := totalLatency / time.Duration(latencyCount)
		fmt.Printf("Average Operation Latency: %v\n", avgLatency)
	}
	
	opsPerSec := float64(metrics.TotalOperationsSent) / time.Since(metrics.StartTime).Seconds()
	fmt.Printf("Operations per Second: %.2f\n", opsPerSec)
	
	// Success rate
	totalAttempts := metrics.TotalOperationsSent + metrics.TotalErrors
	if totalAttempts > 0 {
		successRate := float64(metrics.TotalOperationsSent) / float64(totalAttempts) * 100
		fmt.Printf("Success Rate: %.2f%%\n", successRate)
	}
}

func main() {
	var (
		serverURL  = flag.String("server", "http://localhost:8080", "Server URL")
		numUsers   = flag.Int("users", 10, "Number of simulated users")
		sessionID  = flag.String("session", "", "Session ID (empty for new session)")
		duration   = flag.Duration("duration", 2*time.Minute, "Test duration")
		scenario   = flag.String("scenario", "normal", "Test scenario (normal, aggressive, code, review)")
		rampUp     = flag.Duration("rampup", 10*time.Second, "Ramp up time")
		metrics    = flag.Duration("metrics", 5*time.Second, "Metrics reporting interval")
	)
	
	flag.Parse()
	
	// Generate session ID if not provided
	if *sessionID == "" {
		*sessionID = fmt.Sprintf("test_session_%d", time.Now().UnixNano())
	}
	
	config := SimulationConfig{
		ServerURL:       *serverURL,
		NumUsers:        *numUsers,
		SessionID:       *sessionID,
		Duration:        *duration,
		Scenario:        *scenario,
		RampUpTime:      *rampUp,
		MetricsInterval: *metrics,
	}
	
	RunSimulation(config)
}