// stress_test_runner.go
package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"
)

// TestPlan defines a test scenario
type TestPlan struct {
	Name        string
	Users       int
	Duration    time.Duration
	Scenario    string
	RampUp      time.Duration
}

var testPlans = []TestPlan{
	{
		Name:     "Light Load",
		Users:    5,
		Duration: 1 * time.Minute,
		Scenario: "normal",
		RampUp:   5 * time.Second,
	},
	{
		Name:     "Medium Load",
		Users:    25,
		Duration: 2 * time.Minute,
		Scenario: "aggressive",
		RampUp:   15 * time.Second,
	},
	{
		Name:     "Heavy Load",
		Users:    50,
		Duration: 3 * time.Minute,
		Scenario: "code",
		RampUp:   30 * time.Second,
	},
	{
		Name:     "Stress Test",
		Users:    100,
		Duration: 5 * time.Minute,
		Scenario: "aggressive",
		RampUp:   60 * time.Second,
	},
}

func runTestPlan(plan TestPlan, serverURL string) {
	fmt.Printf("\n=== Running Test: %s ===\n", plan.Name)
	fmt.Printf("Users: %d, Duration: %s, Scenario: %s\n", plan.Users, plan.Duration, plan.Scenario)
	
	sessionID := fmt.Sprintf("stress_test_%d", time.Now().UnixNano())
	
	cmd := exec.Command("go", "run", "test_client.go",
		"-server", serverURL,
		"-users", fmt.Sprintf("%d", plan.Users),
		"-session", sessionID,
		"-duration", plan.Duration.String(),
		"-scenario", plan.Scenario,
		"-rampup", plan.RampUp.String(),
	)
	
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	
	if err := cmd.Run(); err != nil {
		log.Printf("Test failed: %v", err)
	}
	
	// Wait between tests
	fmt.Println("\nWaiting 30 seconds before next test...")
	time.Sleep(30 * time.Second)
}

func main() {
	serverURL := "http://localhost:8080"
	if len(os.Args) > 1 {
		serverURL = os.Args[1]
	}
	
	fmt.Printf("Running stress tests against: %s\n", serverURL)
	
	for _, plan := range testPlans {
		runTestPlan(plan, serverURL)
	}
	
	fmt.Println("\n=== All tests completed ===")
}