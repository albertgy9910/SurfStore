package surfstore

import (
	context "context"

	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
	mutex sync.Mutex
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	v := fileMetaData.Version
	n := fileMetaData.Filename
	m.mutex.Lock()
	_, ok := m.FileMetaMap[n]
	if ok {
		if v != m.FileMetaMap[n].Version+1 {
			// Update: Fail
			v = -1
		} else {
			m.FileMetaMap[n] = fileMetaData
		}
	} else {
		m.FileMetaMap[n] = fileMetaData
	}
	m.mutex.Unlock()
	return &Version{Version: v}, nil
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	var blockStoreMap BlockStoreMap
	blockStoreMap.BlockStoreMap = make(map[string]*BlockHashes)
	for _, hash := range blockHashesIn.Hashes {
		server := m.ConsistentHashRing.GetResponsibleServer(hash)
		if blockStoreMap.BlockStoreMap[server] == nil {
			blockStoreMap.BlockStoreMap[server] = &BlockHashes{
				Hashes: make([]string, 0),
			}
		}
		blockStoreMap.BlockStoreMap[server].Hashes = append(blockStoreMap.BlockStoreMap[server].Hashes, hash)
	}
	return &blockStoreMap, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
