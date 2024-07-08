package main

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"

	"github.com/spaolacci/murmur3"
)

type ServerInfo struct {
	//server name
	name string
	// keys this server holds
	data map[string]int
}

type ConsistentHash struct {
	hashring int
	// cache servers
	nodes []*ServerInfo
	// addresses the server assigned
	keys []int
}

func NewConsistentHash(hashring_length int, serverInfos []*ServerInfo) (*ConsistentHash, error) {

	consistentHash := &ConsistentHash{
		hashring: hashring_length,
		nodes:    make([]*ServerInfo, 0),
		keys:     make([]int, 0),
	}

	for i := range serverInfos {
		err := consistentHash.addServer(serverInfos[i])
		if err != nil {
			fmt.Printf("error adding server: %s, error: %s\n", serverInfos[i].name, err.Error())
		}
	}
	return consistentHash, nil
}

func (serverInfo ServerInfo) String() string {
	return fmt.Sprintf("Server:  %s", serverInfo.name)
}

func (chash *ConsistentHash) addServer(s *ServerInfo) error {

	key_location_on_hashring := calculate_slot(s) % chash.hashring
	fmt.Printf("slot key:  %d\n", key_location_on_hashring)
	// find the insert index where
	index := sort.Search(len(chash.keys), func(i int) bool { return chash.keys[i] >= key_location_on_hashring })
	if index > 0 && chash.keys[index-1] == key_location_on_hashring {
		return errors.New("hash slot already taken !!")
	}

	fmt.Printf("Index : %d\n\n", index)
	chash.nodes = insertServerInfo(chash.nodes, s, index)
	chash.keys = insertKey(chash.keys, key_location_on_hashring, index)
	return nil
}

func (chash *ConsistentHash) addData(data_key string) (string, error) {

	data_key_hash := get_slot_sha256(data_key) % chash.hashring

	index := sort.Search(len(chash.keys), func(i int) bool { return chash.keys[i] >= data_key_hash })

	index = index % len(chash.keys)

	fmt.Printf("Data hash: %d, data index: %d\n", data_key_hash, index)
	return chash.nodes[index].name, nil
}

func (chash *ConsistentHash) removeServer(s *ServerInfo) error {
	return nil
}

func insertKey(a []int, c int, i int) []int {
	return append(a[:i], append([]int{c}, a[i:]...)...)
}

func insertServerInfo(a []*ServerInfo, serverInfo *ServerInfo, index int) []*ServerInfo {
	return append(a[:index], append([]*ServerInfo{serverInfo}, a[index:]...)...)
}

func calculate_slot(s *ServerInfo) int {
	//calculate ring_slot
	return get_slot_sha256(s.name)

}

func get_slot_sha256(name string) int {

	h := sha256.New()
	h.Write([]byte(name))
	sum := h.Sum(nil)
	//number_val := new(big.Int).SetBytes(sum)
	//hash := sha256.Sum256([]byte(name))
	return toInt(sum)
}

// Find a slots on the hashring
func get_slot_murmur(name string) string {
	hash := murmur3.New128()
	hash.Write([]byte(name))
	return base64.RawURLEncoding.EncodeToString(hash.Sum(nil))
}

func toInt(bytes []byte) int {
	result := 0
	for i := 0; i < 4; i++ {
		result = result << 8
		result += int(bytes[i])

	}

	return result
}
