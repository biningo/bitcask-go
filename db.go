package bitcask_go

import (
	"errors"
	"io"
	"os"
	"sync"
)

/**
*@Author icepan
*@Date 11/17/21 19:00
*@Describe
**/

const LogFileName = "bitcask.db"

var (
	KeyIsNotExist = errors.New("key is not exist")
)

type DB struct {
	cache   sync.Map
	dbFile  *DBFile
	dirPath string
}

func Open(dirPath string) (*DB, error) {
	dbFile, err := newDBFile(dirPath + "/" + LogFileName)
	if err != nil {
		return nil, err
	}
	db := &DB{
		cache:   sync.Map{},
		dbFile:  dbFile,
		dirPath: dirPath,
	}
	err = db.load()
	return db, err
}

func (db *DB) load() error {
	var offset int64
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err != io.EOF {
				return err
			}
			return nil
		}
		db.cache.Store(string(e.Key), offset)
		offset += e.Size()
	}
}

func (db *DB) Get(key string) (val string, err error) {
	mapData, ok := db.cache.Load(key)
	if !ok {
		return "", KeyIsNotExist
	}
	offset := mapData.(int64)
	e, err := db.dbFile.Read(offset)
	if err != nil {
		return "", err
	}
	return string(e.Val), nil
}

func (db *DB) Del(key string) error {

	if _, ok := db.cache.Load(key); !ok {
		return KeyIsNotExist
	}

	e := NewEntry([]byte(key), nil, DEL)
	if err := db.dbFile.Write(e); err != nil {
		return err
	}
	db.cache.Delete(key)
	return nil
}

func (db *DB) Add(key string, val string) error {
	offset := db.dbFile.Offset
	e := NewEntry([]byte(key), []byte(val), ADD)
	if err := db.dbFile.Write(e); err != nil {
		return err
	}
	db.cache.Store(key, offset)
	return nil
}

func (db *DB) Merge() error {
	var offset int64
	var newEntries []*Entry

	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
		dataMap, ok := db.cache.Load(string(e.Key))
		if !ok {
			offset += e.Size()
			continue
		}
		//内存中索引和文件里一样则表示数据是可以持久化的
		newOffset := dataMap.(int64)
		if newOffset == offset {
			newEntries = append(newEntries, e)
		}
		offset += e.Size()
	}
	if err := os.Truncate(db.dirPath+"/"+LogFileName, 0); err != nil {
		return err
	}
	db.dbFile.Offset = 0
	offset = 0
	for _, entry := range newEntries {
		if err := db.dbFile.Write(entry); err != nil {
			return err
		}
		db.cache.Store(string(entry.Key), offset)
		offset += entry.Size()
	}
	return nil
}
