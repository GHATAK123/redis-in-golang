package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-redis/redis/v8"
)

// Create a Redis client
var rdb = redis.NewClient(&redis.Options{
	Addr: "localhost:6379", // Redis address
})

var ctx = context.Background()

// Struct to parse JSON payload
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Set/Update key-value in Redis via JSON payload
func upsertKeyHandler(w http.ResponseWriter, r *http.Request) {
	var data KeyValue
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Upsert operation (Set new or Update existing key)
	err = rdb.Set(ctx, data.Key, data.Value, time.Hour).Err()
	if err != nil {
		http.Error(w, "Failed to store key", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"message": "Key stored/updated successfully"})
}

// Get key-value from Redis via URL parameter
func getKeyHandler(w http.ResponseWriter, r *http.Request) {
	// Extract key from URL
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Key parameter is required", http.StatusBadRequest)
		return
	}

	// Fetch value from Redis
	val, err := rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, "Error fetching key", http.StatusInternalServerError)
		return
	}

	// Send response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"key": key, "value": val})
}

func getAllKeysHandler(w http.ResponseWriter, r *http.Request) {
	var cursor uint64
	var keys []string
	var err error

	// Fetch all keys using SCAN
	for {
		var newKeys []string
		newKeys, cursor, err = rdb.Scan(ctx, cursor, "*", 10).Result()
		if err != nil {
			http.Error(w, "Error fetching keys", http.StatusInternalServerError)
			return
		}
		keys = append(keys, newKeys...)
		if cursor == 0 {
			break
		}
	}

	// Fetch values for each key
	var result []KeyValue
	for _, key := range keys {
		val, err := rdb.Get(ctx, key).Result()
		if err == nil {
			result = append(result, KeyValue{Key: key, Value: val})
		}
	}

	// Return JSON response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func deleteKeyHandler(w http.ResponseWriter, r *http.Request) {
	var data KeyValue

	// Decode JSON request
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Delete key from Redis
	deleted, err := rdb.Del(ctx, data.Key).Result()
	if err != nil {
		http.Error(w, "Error deleting key", http.StatusInternalServerError)
		return
	}

	// Check if the key was actually deleted
	if deleted == 0 {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	// Encode and send response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"message": "Key deleted successfully"})
}

func main() {
	// Register handlers
	http.HandleFunc("/set", upsertKeyHandler)      // Set/Update key via POST
	http.HandleFunc("/get", getKeyHandler)         // Get key via GET
	http.HandleFunc("/get-all", getAllKeysHandler) // Get all keys
	http.HandleFunc("/delete", deleteKeyHandler)

	// Start server
	fmt.Println("Server running on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
