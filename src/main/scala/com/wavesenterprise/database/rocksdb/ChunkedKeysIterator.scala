package com.wavesenterprise.database.rocksdb

import scala.collection.{AbstractIterator, mutable}

/**
  * Iterates over data and contract keys grouped in chunks
  */
private[database] class ChunkedKeysIterator(chunkCount: Int, chunkReader: Int => Seq[String], knownKeys: Iterable[String])
    extends AbstractIterator[Iterator[String]] {

  private[this] val knownKeysSet = knownKeys.to[mutable.Set]

  private[this] var chunkNo   = 0
  private[this] var completed = false

  def hasNext: Boolean = !completed

  private def hasNextChunk: Boolean = chunkNo < chunkCount

  private def nextChunkKeysIterator: Iterator[String] = {
    val keys = chunkReader(chunkNo)
    chunkNo += 1
    keys.iterator.map { key =>
      /* Known keys could duplicate keys in chunk, we have to remove it */
      knownKeysSet.remove(key)
      key
    }
  }

  def next(): Iterator[String] = {
    if (hasNext) {
      if (hasNextChunk) {
        nextChunkKeysIterator
      } else {
        completed = true
        knownKeysSet.iterator
      }
    } else {
      Iterator.empty.next()
    }
  }
}
