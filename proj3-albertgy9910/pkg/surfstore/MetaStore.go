package surfstore

import (
	context "context"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
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

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
