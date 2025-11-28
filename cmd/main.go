package main

import (
	"fmt"

	lsmkv "github.com/charithesh16/lsm-kv"
)

// func main() {
// 	store, err := lsmkv.NewKVStore("./data")
// 	if err != nil {
// 		log.Fatalf("Failed to create kv store: %v", err)
// 	}
// 	store.Put([]byte("name"), []byte("Gemini"))
// 	store.Put([]byte("city"), []byte("New York"))
// 	fmt.Println("Wrote data and exiting...")
// }

func main() {
	store, _ := lsmkv.NewKVStore("./data")

	val, _ := store.Get([]byte("name"))
	fmt.Printf("Recovered name: %s\n", val) // Should print "Gemini"

	val, _ = store.Get([]byte("city"))
	fmt.Printf("Recovered city: %s\n", val) // Should print "New York"

	store.Delete([]byte("name"))
	_, err := store.Get([]byte("name"))
	if err != nil {
		fmt.Println("ERROR:", err)
	}
}
