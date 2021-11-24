## Bitcask demo

redis是纯内存模型的，把kv数据都保存在内存中，但是一旦数据量很大的话就会占用大量内存 ，所以为了节省内存我们可以把value保存在磁盘里，在内存中只保存key和数据在磁盘对应的offset，读取的话则需要读两次: 先从内存读key对应的offset,再工具offset读一次磁盘即可获取数据。由于增删查改都是顺序IO，只需要不定期的进行merge文件，所以读写磁盘的速度还是挺快的。
   
references: https://github.com/roseduan/minidb

