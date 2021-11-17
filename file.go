package bitcask_go

import "os"

/**
*@Author icepan
*@Date 11/17/21 18:28
*@Describe
**/

type DBFile struct {
	File   *os.File
	Offset int64
}

func newDBFile(filename string) (*DBFile, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return &DBFile{
		File:   file,
		Offset: stat.Size(),
	}, nil
}

func (df *DBFile) Read(offset int64) (e *Entry, err error) {
	buf := make([]byte, HeaderSize)
	if _, err = df.File.ReadAt(buf, offset); err != nil {
		return
	}
	e = DecodeEntry(buf)
	offset += HeaderSize
	if e.KeySize > 0 {
		key := make([]byte, e.KeySize)
		if _, err = df.File.ReadAt(key, offset); err != nil {
			return
		}
		e.Key = key
	}
	offset += int64(e.KeySize)
	if e.ValSize > 0 {
		val := make([]byte, e.ValSize)
		if _, err = df.File.ReadAt(val, offset); err != nil {
			return
		}
		e.Val = val
	}
	return
}

func (df *DBFile) Write(e *Entry) (err error) {
	data := e.Encode()
	_, err = df.File.WriteAt(data, df.Offset)
	df.Offset += int64(e.Size())
	return err
}
