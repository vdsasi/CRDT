// consistency_test.go
package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

// ConsistencyChecker verifies that all clients eventually see the same content
type ConsistencyChecker struct {
	ServerURL  string
	SessionID  string
	NumClients int
	mu         sync.Mutex
	snapshots  map[string]string // client_id -> text snapshot
}

// NewConsistencyChecker creates a new consistency checker
func NewConsistencyChecker(serverURL, sessionID string, numClients int) *ConsistencyChecker {
	return &ConsistencyChecker{
		ServerURL:  serverURL,
		SessionID:  sessionID,
		NumClients: numClients,
		snapshots:  make(map[string]string),
	}
}

// CheckConsistency verifies all clients have the same view
func (cc *ConsistencyChecker) CheckConsistency() (bool, error) {
	// Get the canonical text from the server
	resp, err := http.Get(fmt.Sprintf("%s/api/sessions/%s/text", cc.ServerURL, cc.SessionID))
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	
	var result map[string]string
	if err := json.Unmarshal(body, &result); err != nil {
		return false, err
	}
	
	serverText := result["text"]
	
	// Compare with all client snapshots
	cc.mu.Lock()
	defer cc.mu.Unlock()
	
	consistent := true
	for clientID, clientText := range cc.snapshots {
		if clientText != serverText {
			fmt.Printf("Inconsistency detected! Client %s has different text\n", clientID)
			fmt.Printf("Server: %q\n", serverText)
			fmt.Printf("Client: %q\n", clientText)
			consistent = false
		}
	}
	
	return consistent, nil
}

// UpdateSnapshot updates a client's text snapshot
func (cc *ConsistencyChecker) UpdateSnapshot(clientID, text string) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.snapshots[clientID] = text
}