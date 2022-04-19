package com.wavesenterprise.database

import java.nio.ByteBuffer

import com.google.common.primitives.{Ints, Shorts}
import com.wavesenterprise.database.rocksdb.ColumnFamily.DefaultCF
import com.wavesenterprise.database.rocksdb._
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.utils.ResourceUtils.withResource

class InternalRocksDBSet[T](name: String,
                            columnFamily: ColumnFamily,
                            prefix: Array[Byte],
                            itemEncoder: T => Array[Byte],
                            itemDecoder: Array[Byte] => T) {

  private[database] val sizeKey: Key[Option[Int]] = {
    val keyBytes = ByteBuffer
      .allocate(Shorts.BYTES + prefix.length)
      .putShort(WEKeys.SetSizePrefix)
      .put(prefix)
      .array()
    Key.opt(s"$name-set-size", columnFamily, keyBytes, Ints.fromByteArray, Ints.toByteArray)
  }

  private[database] lazy val startTarget: Array[Byte] =
    ByteBuffer
      .allocate(prefix.length + 1)
      .put(prefix)
      .put(RocksDBSet.StartSuffix)
      .array()

  private def buildSetItemKey(value: T): Key[T] = {
    val valueBytes = itemEncoder(value)

    val keyBytes = ByteBuffer
      .allocate(prefix.length + 1 + valueBytes.length)
      .put(prefix)
      .put(RocksDBSet.StartSuffix)
      .put(valueBytes)
      .array()

    Key(s"$name-set-item", columnFamily, keyBytes, itemDecoder, itemEncoder)
  }

  protected[database] def add(rw: RW, value: T): Boolean = {
    val itemKey = buildSetItemKey(value)
    val oldSize = rw.get(sizeKey).getOrElse(0)

    if (!rw.has(itemKey)) {
      rw.put(sizeKey, Some(oldSize + 1))
      rw.put(itemKey, value)
      true
    } else {
      false
    }
  }

  protected[database] def add(rw: RW, values: Iterable[T]): Int = {
    val oldSize = rw.get(sizeKey).getOrElse(0)
    val newSize = addMany(rw, values, oldSize)

    val diff = newSize - oldSize
    if (diff > 0) rw.put(sizeKey, Some(newSize))
    diff
  }

  protected[database] def remove(rw: RW, value: T): Boolean = {
    val itemKey = buildSetItemKey(value)
    val oldSize = rw.get(sizeKey).getOrElse(0)

    if (rw.has(itemKey)) {
      rw.put(sizeKey, Some(oldSize - 1))
      rw.delete(itemKey)
      true
    } else {
      false
    }
  }

  protected[database] def remove(rw: RW, values: Iterable[T]): Int = {
    val oldSize = rw.get(sizeKey).getOrElse(0)
    val newSize = removeMany(rw, values, oldSize)
    val diff    = oldSize - newSize
    if (diff > 0) rw.put(sizeKey, Some(newSize))
    diff
  }

  protected[database] def addAndRemoveDisjoint(rw: RW, valuesToAdd: Set[T], valuesToRemove: Set[T]): Unit = {
    require(valuesToAdd.intersect(valuesToRemove).isEmpty, "Operation is available only for disjoint sets")

    val oldSize = rw.get(sizeKey).getOrElse(0)

    val sizeAfterAdd = addMany(rw, valuesToAdd, oldSize)
    val newSize      = removeMany(rw, valuesToRemove, sizeAfterAdd)

    if (oldSize != newSize) rw.put(sizeKey, Some(newSize))
  }

  private def addMany(rw: RW, values: Iterable[T], oldSize: Int): Int = {
    values.foldLeft(oldSize) {
      case (acc, v) =>
        val itemKey = buildSetItemKey(v)
        if (!rw.has(itemKey)) {
          rw.put(itemKey, v)
          acc + 1
        } else {
          acc
        }
    }
  }

  private def removeMany(rw: RW, values: Iterable[T], oldSize: Int): Int = {
    values.foldLeft(oldSize) {
      case (acc, v) =>
        val itemKey = buildSetItemKey(v)
        if (rw.has(itemKey)) {
          rw.delete(itemKey)
          acc - 1
        } else {
          acc
        }
    }
  }

  protected[database] def size(ro: ReadOnlyDB): Int = {
    ro.get(sizeKey).getOrElse(0)
  }

  protected[database] def isEmpty(ro: ReadOnlyDB): Boolean =
    size(ro) < 1

  @inline protected[database] def nonEmpty(ro: ReadOnlyDB): Boolean =
    !isEmpty(ro)

  protected[database] def contains(ro: ReadOnlyDB, value: T): Boolean = {
    val itemKey = buildSetItemKey(value)
    ro.has(itemKey)
  }

  protected[database] def members(ro: ReadOnlyDB): Set[T] = withResource(ro.iterator(columnFamily)) { iterator =>
    val size = ro.get(sizeKey).getOrElse(0)

    iterator.seek(startTarget)

    if (iterator.isValid && iterator.key.sameElements(startTarget))
      iterator.next()

    (1 to size).view.collect {
      case _ if iterator.isValid =>
        val item = itemDecoder(iterator.value())
        iterator.next()
        item
    }.toSet
  }

  // TODO: test against address cache and remove if cache performs better
  protected[database] def rawBytes(ro: ReadOnlyDB): Set[ByteStr] = withResource(ro.iterator(columnFamily)) { iterator =>
    val size = ro.get(sizeKey).getOrElse(0)

    iterator.seek(startTarget)

    if (iterator.isValid && iterator.key.sameElements(startTarget))
      iterator.next()

    (1 to size).view.collect {
      case _ if iterator.isValid =>
        val item = ByteStr(iterator.value())
        iterator.next()
        item
    }.toSet
  }

  protected[database] def clear(rw: RW): Unit = {
    remove(rw, members(rw))
    rw.delete(sizeKey)
  }
}

class RocksDBSet[T](name: String,
                    columnFamily: ColumnFamily,
                    prefix: Array[Byte],
                    storage: RocksDBStorage,
                    itemEncoder: T => Array[Byte],
                    itemDecoder: Array[Byte] => T)
    extends InternalRocksDBSet[T](name, columnFamily, prefix, itemEncoder, itemDecoder) {

  def this(name: String, prefix: Array[Byte], storage: RocksDBStorage, itemEncoder: T => Array[Byte], itemDecoder: Array[Byte] => T) {
    this(name, DefaultCF, prefix, storage, itemEncoder, itemDecoder)
  }

  @inline def add(value: T): Boolean = storage.readWrite(rw => add(rw, value))

  @inline def add(values: Iterable[T]): Int = storage.readWrite(rw => add(rw, values))

  @inline def remove(value: T): Boolean = storage.readWrite(rw => remove(rw, value))

  @inline def remove(values: Iterable[T]): Int = storage.readWrite(rw => remove(rw, values))

  @inline def addAndRemoveDisjoint(valuesToAdd: Set[T], valuesToRemove: Set[T]): Unit =
    storage.readWrite(rw => addAndRemoveDisjoint(rw, valuesToAdd, valuesToRemove))

  @inline def size: Int = storage.readOnly(size)

  @inline def isEmpty: Boolean = storage.readOnly(isEmpty)

  @inline def nonEmpty: Boolean = storage.readOnly(nonEmpty)

  @inline def contains(value: T): Boolean = storage.readOnly(ro => contains(ro, value))

  @inline def members: Set[T] = storage.readOnly(members)

  @inline def rawBytes: Set[ByteStr] = storage.readOnly(rawBytes)

  @inline def clear(): Unit = storage.readWrite(clear)
}

object RocksDBSet {
  private[database] val StartSuffix: Byte = Byte.MinValue
}
