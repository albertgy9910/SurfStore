package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	consistentHashRing := c.ServerMap
	hashes := []string{}

	for hash := range consistentHashRing {
		hashes = append(hashes, hash)   
	}
	sort.Strings(hashes)
	responsibleServer := ""
	for _, hash := range hashes {
		if hash > blockId {
			responsibleServer = consistentHashRing[hash]
			break
		}
	}
	if responsibleServer == "" {
		responsibleServer = consistentHashRing[hashes[0]]
	}
	return responsibleServer
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	consistentHashRing := &ConsistentHashRing{
		ServerMap: make(map[string]string),
	}
	for _, serverAddr := range serverAddrs {
		consistentHashRing.ServerMap[consistentHashRing.Hash("blockstore"+serverAddr)] = serverAddr
	}
	return consistentHashRing
}
