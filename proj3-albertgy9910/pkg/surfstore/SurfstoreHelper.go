package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `INSERT into indexes (fileName,version,hashIndex,hashValue) VALUES (?,?,?,?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	defer db.Close()
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()

	statement, err = db.Prepare(insertTuple)
	if err != nil {
		log.Fatal("Error During insert: ", err)
		return err
	}
	for _, fileMeta := range fileMetas {
		for i, val := range fileMeta.BlockHashList {
			_, err = statement.Exec(fileMeta.Filename, fileMeta.Version, i, val)
			if err != nil {
				log.Fatal("Error During Data Write: ", err)
				return err
			}
		}
	}

	return nil
}

/*
Reading Local Metadata File Related
*/
const getTuplesByFileName string = `SELECT fileName, version, hashIndex, hashValue from indexes where fileName = ?;`
const getDistinctFileName string = `SELECT distinct fileName from indexes;`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}
	defer db.Close()

	statement, err := db.Prepare(createTable)
	if err != nil {
		fmt.Println(err.Error())
	}
	statement.Exec()

	rows, err := db.Query(getDistinctFileName)
	if err != nil {
		log.Fatal("Error During reading data: ", err)
	}

	var fName string
	var fileNames []string
	for rows.Next() {
		rows.Scan(&fName)
		fileNames = append(fileNames, fName)
	}

	for _, name := range fileNames {
		rows, err := db.Query(getTuplesByFileName, name)
		if err != nil {
			log.Fatal("Error During reading data", err)
		}

		var fName string
		var version int32
		var hashIdx int
		var hashVal string
		var blockList []string
		for rows.Next() {
			rows.Scan(&fName, &version, &hashIdx, &hashVal)
			blockList = append(blockList, hashVal)
		}

		var fileMeta FileMetaData
		fileMeta.Filename = fName
		fileMeta.Version = version
		fileMeta.BlockHashList = blockList

		fileMetaMap[fName] = &fileMeta

	}
	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
