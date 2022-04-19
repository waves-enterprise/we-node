package com.wavesenterprise.database

import com.wavesenterprise.database.docker.KeysPagination
import com.wavesenterprise.database.rocksdb.ChunkedKeysIterator
import org.scalatest.{FreeSpec, Matchers}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ChunkedKeysIteratorSpec extends FreeSpec with Matchers with ScalaCheckPropertyChecks {

  private val chunkCount                                  = 10
  private val countInChunk                                = 100
  private def generateChunk(chunkNo: Int): Vector[String] = Range.inclusive(1, countInChunk).map(i => s"key_${chunkNo}_$i").toVector

  "test without limit and offset" - {
    "test without known keys" in {
      val keysIterator = new ChunkedKeysIterator(chunkCount, generateChunk, Set.empty)
      val pagination   = new KeysPagination(keysIterator.flatten)
      val chunkNo      = chunkCount / 2
      val actual       = pagination.paginatedKeys(None, None, Some((s: String) => s.startsWith(s"key_$chunkNo"))).toVector
      val expected     = generateChunk(chunkNo)
      actual shouldBe expected
    }

    "test with known keys" in {
      val knownKeys    = generateChunk(chunkCount)
      val keysIterator = new ChunkedKeysIterator(chunkCount, generateChunk, knownKeys)
      val pagination   = new KeysPagination(keysIterator.flatten)
      val actual       = pagination.paginatedKeys(None, None, Some((s: String) => s.startsWith(s"key_$chunkCount"))).toVector
      actual should contain theSameElementsAs knownKeys
    }
  }

  "test with limit and without offset" - {
    val limit = countInChunk / 2
    "test without known keys" in {
      val keysIterator = new ChunkedKeysIterator(chunkCount, generateChunk, Set.empty)
      val pagination   = new KeysPagination(keysIterator.flatten)
      val chunkNo      = chunkCount / 2
      val actual       = pagination.paginatedKeys(None, Some(limit), Some((s: String) => s.startsWith(s"key_$chunkNo"))).toVector
      val expected     = generateChunk(chunkNo).take(limit)
      actual shouldBe expected
    }

    "test with known keys" in {
      val knownKeys    = generateChunk(chunkCount)
      val keysIterator = new ChunkedKeysIterator(chunkCount, generateChunk, knownKeys)
      val pagination   = new KeysPagination(keysIterator.flatten)
      val actual       = pagination.paginatedKeys(None, Some(limit), Some((s: String) => s.startsWith(s"key_$chunkCount"))).toVector
      actual should have size limit
      actual.intersect(knownKeys) should have size limit
    }
  }

  "test without limit and with offset" - {
    "test without known keys" in {
      val offset       = 8 * countInChunk
      val keysIterator = new ChunkedKeysIterator(chunkCount, generateChunk, Set.empty)
      val pagination   = new KeysPagination(keysIterator.flatten)
      val actual       = pagination.paginatedKeys(Some(offset), None, None).toVector
      val expected     = generateChunk(8) ++ generateChunk(9)
      actual shouldBe expected
    }

    "test with known keys" in {
      val offset       = 9 * countInChunk
      val knownKeys    = generateChunk(chunkCount)
      val keysIterator = new ChunkedKeysIterator(chunkCount, generateChunk, knownKeys)
      val pagination   = new KeysPagination(keysIterator.flatten)
      val actual       = pagination.paginatedKeys(Some(offset), None, None).toVector
      val expected     = generateChunk(9) ++ knownKeys
      actual should contain theSameElementsAs expected
    }
  }

  "test with limit and offset" - {
    val halfChunkSize = countInChunk / 2
    val limit         = countInChunk + halfChunkSize
    "test without known keys" in {
      val offset       = 4 * countInChunk
      val keysIterator = new ChunkedKeysIterator(chunkCount, generateChunk, Set.empty)
      val pagination   = new KeysPagination(keysIterator.flatten)
      val actual       = pagination.paginatedKeys(Some(offset), Some(limit), None).toVector
      val expected     = generateChunk(4) ++ generateChunk(5).take(halfChunkSize)
      actual shouldBe expected
    }

    "test with known keys" in {
      val offset       = 9 * countInChunk
      val knownKeys    = generateChunk(chunkCount)
      val keysIterator = new ChunkedKeysIterator(chunkCount, generateChunk, knownKeys)
      val pagination   = new KeysPagination(keysIterator.flatten)
      val actual       = pagination.paginatedKeys(Some(offset), Some(limit), None).toVector
      actual.slice(0, countInChunk) shouldBe generateChunk(9)
      actual.slice(countInChunk, limit).intersect(knownKeys) should have size halfChunkSize
    }

    "test edge case" in {
      val offset       = 10 * countInChunk
      val keysIterator = new ChunkedKeysIterator(chunkCount, generateChunk, Set.empty)
      val pagination   = new KeysPagination(keysIterator.flatten)
      val actual       = pagination.paginatedKeys(Some(offset), Some(limit), None).toVector
      actual shouldBe Vector.empty
    }
  }
}
