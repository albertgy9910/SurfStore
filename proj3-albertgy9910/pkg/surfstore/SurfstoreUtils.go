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

	var blockStoreAddr string
	if err := client.GetBlockStoreAddr(&blockStoreAddr); err != nil {
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
	syncUp(client, blockStoreAddr, localIndex, remoteIndex)

	//sync form server to local
	syncDown(client, blockStoreAddr, remoteIndex, localIndex)

}

func syncUp(client RPCClient, blockStoreAddr string, localIndex map[string]*FileMetaData, remoteIndex map[string]*FileMetaData) {
	for fileName, localFileMeta := range localIndex {
		serverFileMeta, ok := remoteIndex[fileName]
		// S1: file exits on server and has been changed on local
		// S2: file doesn't exit on server
		if (ok && serverFileMeta.Version < localFileMeta.Version) || !ok {
			upServer(client, localFileMeta, blockStoreAddr)
		}
	}
}

func syncDown(client RPCClient, blockStoreAddr string, remoteIndex map[string]*FileMetaData, localIndex map[string]*FileMetaData) {
	for fileName, serverMetaData := range remoteIndex {
		localFileMeta, ok := localIndex[fileName]
		if !ok {
			// file doesn't exit on local
			localIndex[fileName] = &FileMetaData{}
			downLocal(client, localIndex[fileName], serverMetaData, blockStoreAddr)
		} else {
			// file exits on local and has been changed by others
			if localFileMeta.Version < serverMetaData.Version || (localFileMeta.Version == serverMetaData.Version &&
				!reflect.DeepEqual(serverMetaData.BlockHashList, localFileMeta.BlockHashList)) {
				downLocal(client, localFileMeta, serverMetaData, blockStoreAddr)
			}
		}
	}
	WriteMetaFile(localIndex, client.BaseDir)
}

func upServer(client RPCClient, localMeta *FileMetaData, blockStoreAddr string) error {
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
	for i := 0; i < numofBlocks; i++ {
		bytes := make([]byte, client.BlockSize)
		len, err := file.Read(bytes)
		if err != nil && err != io.EOF {
			log.Fatal("Error During reading bytes in base dir: ", err)
		}
		bytes = bytes[:len]

		block := Block{BlockData: bytes, BlockSize: int32(len)}

		sign := true
		if err := client.PutBlock(&block, blockStoreAddr, &sign); err != nil {
			log.Fatal("Error During putting block: ", err)
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

func downLocal(client RPCClient, localMeta *FileMetaData, serverMeta *FileMetaData, blockStoreAddr string) error {
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

	for _, hash := range serverMeta.BlockHashList {
		var block Block
		err := client.GetBlock(hash, blockStoreAddr, &block)
		if err != nil {
			fmt.Println("Error During getting block: ", err)
		}
		file.Write(block.BlockData)
	}

	return nil
}
