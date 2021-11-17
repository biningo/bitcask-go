package bitcask_go

import (
	"fmt"
	"testing"
)

/**
*@Author icepan
*@Date 11/17/21 19:27
*@Describe
**/

func TestDB_Add(t *testing.T) {
	dirPath := "/home/pb/temp"
	db, err := Open(dirPath)
	if err != nil {
		t.Error(err)
	}
	if err := db.Add("name", "lyer"); err != nil {
		t.Error(err)
	}
	v, err := db.Get("name")
	if err != nil {
		t.Error(err)
	}
	fmt.Println(v)
}

func TestDB_GET(t *testing.T) {
	dirPath := "/home/pb/temp"
	db, err := Open(dirPath)
	if err != nil {
		t.Error(err)
	}
	v, err := db.Get("name")
	fmt.Println(v)
	v, err = db.Get("age")
	fmt.Println(v, err)
}

func TestDB_Merge(t *testing.T) {
	dirPath := "/home/pb/temp"
	db, err := Open(dirPath)
	if err != nil {
		t.Error(err)
	}
	db.Add("age", "18")
	db.Add("name", "abc")
	db.Add("address", "China")
	db.Del("age")
	db.Add("name", "def")
	if err = db.Merge(); err != nil {
		t.Error(err)
	}
	db.cache.Range(func(key, value interface{}) bool {
		k := key.(string)
		v, _ := db.Get(k)
		fmt.Println(k,v)
		return true
	})
}
