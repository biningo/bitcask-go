## Bitcask demo

redis是纯内存模型的，把kv数据都保存在内存中，但是一旦数据量很大的话就会占用大量内存 ，所以为了节省内存我们可以把key和数据保存在磁盘上的offset保存在内存中，读取的话则需要读一次内存再读一次磁盘
   
references: https://github.com/roseduan/minidb

