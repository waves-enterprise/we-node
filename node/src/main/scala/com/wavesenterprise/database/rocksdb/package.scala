package com.wavesenterprise.database

import java.nio.ByteBuffer
import org.rocksdb._

package object rocksdb {

  def keysByPrefix[T](iterator: RocksIterator, prefix: Short)(implicit toKey: Array[Byte] => MainDBKey[T]): Iterator[MainDBKey[T]] = {
    val prefixArray = ByteBuffer.allocate(2).putShort(prefix).array()
    iterator.seekToFirst()
    val keys = Vector.newBuilder[MainDBKey[T]]
    while (iterator.isValid) {
      val key = iterator.key()
      if (key.length >= 2 && key.take(2).sameElements(prefixArray)) {
        keys += key
      }
      iterator.next()
    }
    keys.result().toIterator
  }
}
