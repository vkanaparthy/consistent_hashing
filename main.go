package main

import (
	"fmt"
)

func main() {

	var serverInfos []*ServerInfo
	var regions = [4]string{"us-west-1", "us-east-2", "eu-east-3", "sp-east-4"}
	for i := range 4 {
		serverName := fmt.Sprintf("cache-%d-redis-%s", i, regions[i])
		///fmt.Println(serverName)
		serverInfos = append(serverInfos, &ServerInfo{
			name: serverName,
			data: nil,
		})
	}

	consistentHash, err := NewConsistentHash(50, serverInfos)
	if err != nil {
		fmt.Println("Error create new instance of hash")
	}

	fmt.Println(consistentHash.nodes)
	fmt.Printf("keys: %v\n", consistentHash.keys)

	// assign data to servers
	data := []string{"file1.txt", "file2.txt", "file3.txt", "file4.txt", "file5.txt"}
	for _, file := range data {
		server, err := consistentHash.addData(file)
		if err != nil {
			fmt.Printf("Error assigning file: %s", file)
		}
		fmt.Printf("Assigned file: %s to server : %s\n", file, server)
	}
}
