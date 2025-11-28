package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	lsmkv "github.com/charithesh16/lsm-kv"
)

func main() {
	store, err := lsmkv.NewKVStore("../data")
	if err != nil {
		log.Fatalf("Failed to create kv store: %v", err)
	}
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("LSM-KV Store Interactive Shell")
	fmt.Println("Commands: put \"key\" \"value\", get \"key\", delete \"key\", sizeOfMemtable, printMemtable, exit")

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()
		parts := parseInput(line)

		if len(parts) == 0 {
			continue
		}

		command := strings.ToLower(parts[0])

		switch command {
		case "put":
			if len(parts) != 3 {
				fmt.Println("Usage: put \"key\" \"value\"")
				continue
			}
			key, value := parts[1], parts[2]
			err := store.Put([]byte(key), []byte(value))
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("OK")
			}

		case "get":
			if len(parts) != 2 {
				fmt.Println("Usage: get \"key\"")
				continue
			}
			key := parts[1]
			val, err := store.Get([]byte(key))
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("%s\n", string(val))
			}

		case "delete":
			if len(parts) != 2 {
				fmt.Println("Usage: delete \"key\"")
				continue
			}
			key := parts[1]
			err := store.Delete([]byte(key))
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("OK")
			}

		case "sizeofmemtable":
			fmt.Printf("Memtable size: %d bytes\n", store.GetMemtableSize())

		case "printmemtable":
			store.PrintMemtable()

		case "exit", "quit":
			fmt.Println("Bye!")
			return

		default:
			fmt.Println("Unknown command")
		}
	}
}

func parseInput(input string) []string {
	var args []string
	var currentArg strings.Builder
	inQuote := false

	for _, r := range input {
		switch r {
		case '"':
			inQuote = !inQuote
		case ' ':
			if !inQuote {
				if currentArg.Len() > 0 {
					args = append(args, currentArg.String())
					currentArg.Reset()
				}
			} else {
				currentArg.WriteRune(r)
			}
		default:
			currentArg.WriteRune(r)
		}
	}
	if currentArg.Len() > 0 {
		args = append(args, currentArg.String())
	}
	return args
}
