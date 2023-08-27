package surfstore

import (
	context "context"
	"sync"

	"google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
	mutex sync.Mutex
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	// use lock to ensure unchanged while fetching
	bs.mutex.Lock()
	block := bs.BlockMap[blockHash.Hash]
	bs.mutex.Unlock()
	return block, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	hash := GetBlockHashString(block.BlockData)
	// use lock to ensure unchanged while fetching
	bs.mutex.Lock()
	bs.BlockMap[hash] = block
	bs.mutex.Unlock()
	return &Success{Flag: true}, nil

}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	var blockHashesOut BlockHashes

	for _, hash := range blockHashesIn.Hashes {
		bs.mutex.Lock()
		if _, ok := bs.BlockMap[hash]; ok {
			blockHashesOut.Hashes = append(blockHashesOut.Hashes, hash)
		}
		bs.mutex.Unlock()
	}

	return &blockHashesOut, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	blockHashes := &BlockHashes{
		Hashes: make([]string, 0),
	}
	for hash := range bs.BlockMap {
		blockHashes.Hashes = append(blockHashes.Hashes, hash)
	}

	return blockHashes, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
