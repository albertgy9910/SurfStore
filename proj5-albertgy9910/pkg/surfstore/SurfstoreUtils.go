package surfstore

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"reflect"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// if not exist, create file
	idxdbPath := ConcatPath(client.BaseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(idxdbPath); errors.Is(err, os.ErrNotExist) {
		_, err := os.Create(idxdbPath)
		if err != nil {
			log.Fatal("Error During creating file: ", err)
		}
	}

	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Fatal("Error During reading base dir: ", err)
	}
	localIndex, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Fatal("Error During loading from file: ", err)
	}

	var blockStoreAddrs []string
	if err := client.GetBlockStoreAddrs(&blockStoreAddrs); err != nil {
		fmt.Println("Error During getting block store address: ", err)
	}

	remoteIndex := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&remoteIndex); err != nil {
		fmt.Println("Error During getting fileInfoMap from server: ", err)
	}

	hashMap := make(map[string][]string)
	for _, file := range files {
		if file.Name() == DEFAULT_META_FILENAME {
			continue
		}
		if file.Size() == 0 {
			hashMap[file.Name()] = []string{"-1"}
		} else {
			numofBlocks := int(math.Ceil(float64(file.Size()) / float64(client.BlockSize)))

			// read file
			openFile, err := os.Open(client.BaseDir + "/" + file.Name())
			if err != nil {
				log.Fatal("Error During reading file in base dir: ", err)
			}

			// generate hash for this block
			for i := 0; i < numofBlocks; i++ {
				bytes := make([]byte, client.BlockSize)
				length, err := openFile.Read(bytes)
				if err != nil {
					log.Fatal("Error During reading bytes in base dir: ", err)
				}
				bytes = bytes[:length]
				blockHashString := GetBlockHashString(bytes)
				hashMap[file.Name()] = append(hashMap[file.Name()], blockHashString)
			}
			openFile.Close()
		}

		value, ok := localIndex[file.Name()]
		if !ok {
			fileMeta := FileMetaData{
				Filename:      file.Name(),
				Version:       1,
				BlockHashList: hashMap[file.Name()],
			}
			localIndex[file.Name()] = &fileMeta
		} else {
			if !reflect.DeepEqual(hashMap[file.Name()], value.BlockHashList) {
				localIndex[file.Name()].BlockHashList = hashMap[file.Name()]
				localIndex[file.Name()].Version++
			}
		}
	}
	// deleted files checker
	for fName, fileMeta := range localIndex {
		_, ok := hashMap[fName]
		if !ok {
			if fileMeta.BlockHashList[0] != "0" || len(fileMeta.BlockHashList) != 1 {
				fileMeta.BlockHashList = []string{"0"}
				fileMeta.Version++
			}
		}
	}
	// sync from local to server
	syncUp(client, localIndex, remoteIndex)

	//sync form server to local
	syncDown(client, remoteIndex, localIndex)

}

func syncUp(client RPCClient, localIndex map[string]*FileMetaData, remoteIndex map[string]*FileMetaData) {
	for fileName, localFileMeta := range localIndex {
		serverFileMeta, ok := remoteIndex[fileName]
		// S1: file exits on server and has been changed on local
		// S2: file doesn't exit on server
		if (ok && serverFileMeta.Version < localFileMeta.Version) || !ok {
			upServer(client, localFileMeta)
		}
	}
}

func syncDown(client RPCClient, remoteIndex map[string]*FileMetaData, localIndex map[string]*FileMetaData) {
	for fileName, serverMetaData := range remoteIndex {
		localFileMeta, ok := localIndex[fileName]
		if !ok {
			// file doesn't exit on local
			localIndex[fileName] = &FileMetaData{}
			downLocal(client, localIndex[fileName], serverMetaData)
		} else {
			// file exits on local and has been changed by others
			if localFileMeta.Version < serverMetaData.Version || (localFileMeta.Version == serverMetaData.Version &&
				!reflect.DeepEqual(serverMetaData.BlockHashList, localFileMeta.BlockHashList)) {
				downLocal(client, localFileMeta, serverMetaData)
			}
		}
	}
	WriteMetaFile(localIndex, client.BaseDir)
}

func upServer(client RPCClient, localMeta *FileMetaData) error {
	var VersionSign int32
	filePath := ConcatPath(client.BaseDir, localMeta.Filename)
	if _, err := os.Stat(filePath); errors.Is(err, os.ErrNotExist) {
		err = client.UpdateFile(localMeta, &VersionSign)
		if err != nil {
			log.Fatal("Error During uploading files: ", err)
		}
		localMeta.Version = VersionSign
		return err
	}

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Error During opening files: ", err)
	}
	defer file.Close()

	stat, _ := file.Stat()

	numofBlocks := int(math.Ceil(float64(stat.Size()) / float64(client.BlockSize)))
	blockStoreMap := make(map[string][]string)
	if err := client.GetBlockStoreMap(localMeta.BlockHashList, &blockStoreMap); err != nil {
		panic(err)
	}
	for i := 0; i < numofBlocks; i++ {
		bytes := make([]byte, client.BlockSize)
		len, err := file.Read(bytes)
		if err != nil && err != io.EOF {
			log.Fatal("Error During reading bytes in base dir: ", err)
		}
		bytes = bytes[:len]

		block := Block{BlockData: bytes, BlockSize: int32(len)}

		var sign bool
		for addr, hashes := range blockStoreMap {
			if contains(hashes, localMeta.BlockHashList[i]) {
				if err := client.PutBlock(&block, addr, &sign); err != nil {
					panic(err)
				}
				break
			}
		}
	}

	if err := client.UpdateFile(localMeta, &VersionSign); err != nil {
		log.Println("Error During uploading: ", err)
		localMeta.Version = -1
		return err
	}
	localMeta.Version = VersionSign
	return nil
}

func downLocal(client RPCClient, localMeta *FileMetaData, serverMeta *FileMetaData) error {
	filePath := ConcatPath(client.BaseDir, serverMeta.Filename)
	file, err := os.Create(filePath)
	if err != nil {
		log.Println("Error During creating: ", err)
	}
	defer file.Close()

	*localMeta = *serverMeta

	//deleted files
	if serverMeta.BlockHashList[0] == "0" && len(serverMeta.BlockHashList) == 1 {
		err := os.Remove(filePath)
		if err != nil {
			log.Fatal("Error During removing: ", err)
			return err
		}
		return nil
	}

	blockStoreMap := make(map[string][]string)
	if err := client.GetBlockStoreMap(serverMeta.BlockHashList, &blockStoreMap); err != nil {
		panic(err)
	}

	for _, hash := range serverMeta.BlockHashList {
		var block Block

		for addr, hashes := range blockStoreMap {
			if contains(hashes, hash) {
				err := client.GetBlock(hash, addr, &block)
				if err != nil {
					fmt.Println("Error During getting block: ", err)
				}
				break
			}
		}
		file.Write(block.BlockData)
	}

	return nil
}

func contains(s []string, str string) bool {
	for _, a := range s {
		if a == str {
			return true
		}
	}
	return false
}
