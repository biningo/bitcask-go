package bitcask_go

import "encoding/binary"

/**
*@Author icepan
*@Date 11/17/21 18:01
*@Describe
**/

type MarkType uint16

const (
	ADD MarkType = iota
	DEL
)
const HeaderSize = 10 // 32*2+16=4*2+2

type Entry struct {
	Key     []byte
	Val     []byte
	KeySize uint32
	ValSize uint32
	Mark    MarkType //标记 删除还是添加
}

func NewEntry(key, val []byte, mark MarkType) *Entry {
	return &Entry{
		Key:     key,
		Val:     val,
		KeySize: uint32(len(key)),
		ValSize: uint32(len(val)),
		Mark:    mark,
	}
}

func (e *Entry) Size() int64 {
	return int64(HeaderSize + e.KeySize + e.ValSize)
}
func (e *Entry) Encode() []byte {
	buf := make([]byte, e.Size())
	binary.BigEndian.PutUint32(buf[:4], e.KeySize)
	binary.BigEndian.PutUint32(buf[4:8], e.ValSize)
	binary.BigEndian.PutUint16(buf[8:10], uint16(e.Mark))
	copy(buf[HeaderSize:HeaderSize+e.KeySize], e.Key)
	copy(buf[HeaderSize+e.KeySize:], e.Val)
	return buf
}

func DecodeEntry(buf []byte) *Entry {
	keySize := binary.BigEndian.Uint32(buf[:4])
	valSize := binary.BigEndian.Uint32(buf[4:8])
	mark := MarkType(binary.BigEndian.Uint16(buf[8:10]))
	return &Entry{KeySize: keySize, ValSize: valSize, Mark: mark}
}
