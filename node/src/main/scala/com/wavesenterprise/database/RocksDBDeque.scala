package com.wavesenterprise.database

import com.google.common.primitives.Ints
import com.wavesenterprise.database.rocksdb._
import cats.implicits._

import java.nio.ByteBuffer

class RocksDBDeque[T](
    name: String,
    columnFamily: ColumnFamily,
    prefix: Array[Byte],
    storage: RocksDBStorage,
    itemEncoder: T => Array[Byte],
    itemDecoder: Array[Byte] => T
) extends InternalRocksDBDeque[T](name, columnFamily, prefix, itemEncoder, itemDecoder) {
  def this(
      name: String,
      prefix: Array[Byte],
      storage: RocksDBStorage,
      itemEncoder: T => Array[Byte],
      itemDecoder: Array[Byte] => T
  ) {
    this(name, ColumnFamily.DefaultCF, prefix, storage, itemEncoder, itemDecoder)
  }
  @inline def addFirst(value: T): Unit =
    storage.readWrite(rw => addFirst(rw, value))

  @inline def addFirstN(values: Iterable[T]): Unit =
    storage.readWrite(rw => values.foreach(addFirst(rw, _)))

  @inline def addLast(value: T): Unit =
    storage.readWrite(rw => addLast(rw, value))

  @inline def addLastN(values: Iterable[T]): Unit =
    storage.readWrite(rw => values.foreach(addLast(rw, _)))

  @inline def pollFirst: Option[T] = {
    storage.readWrite(pollFirst)
  }

  @inline def pollFirstN(n: Int): Seq[T] = {
    storage.readWrite(rw => (0 until n).flatMap(_ => pollFirst(rw)))
  }

  @inline def pollLast: Option[T] = {
    storage.readWrite(pollLast)
  }

  @inline def pollLastN(n: Int): Seq[T] = {
    storage.readWrite(rw => pollLastN(rw, n))
  }

  @inline def head: Option[T] = {
    storage.readOnly(peekFirst)
  }

  @inline def take(n: Int): Seq[T] = {
    storage.readOnly(ro => (0 until n).flatMap(_ => peekLast(ro)))
  }

  @inline def last: Option[T] = {
    storage.readOnly(peekLast)
  }

  def takeRight(n: Int): Seq[T] = {
    slice(size - n, size)
  }

  @inline def contains(value: T): Boolean =
    storage.readOnly(ro => contains(ro, value))

  @inline def isEmpty: Boolean =
    storage.readOnly(isEmpty)

  @inline def nonEmpty: Boolean =
    storage.readOnly(nonEmpty)

  @inline def size: Int =
    storage.readOnly(size)

  @inline def clear(): Unit =
    storage.readWrite(clear)

  @inline def toList: List[T] = {
    storage.readOnly(toList)
  }

  def slice(from: Int, until: Int): List[T] = {
    storage.readOnly(ro => slice(from, until, ro))
  }
}

class InternalRocksDBDeque[T](
    name: String,
    columnFamily: ColumnFamily,
    prefix: Array[Byte],
    itemEncoder: T => Array[Byte],
    itemDecoder: Array[Byte] => T
) {
  case class DequeMetaKey(
      head: Int,
      tail: Int,
      size: Int
  )

  object DequeMetaKey {
    def decodeMetaKey(encodedMetaKey: Array[Byte]): DequeMetaKey = {
      val buffer = ByteBuffer.wrap(encodedMetaKey)
      val head   = buffer.getInt
      val tail   = buffer.getInt
      val size   = buffer.getInt

      DequeMetaKey(head, tail, size)
    }

    def encodeMetaKey(metaKey: DequeMetaKey): Array[Byte] =
      ByteBuffer.allocate(Ints.BYTES * 12)
        .putInt(metaKey.head)
        .putInt(metaKey.tail)
        .putInt(metaKey.size)
        .array()

    lazy val initState: DequeMetaKey = {
      val minHead      = 1000
      val maxHead      = Int.MaxValue - 1000
      val initHeadTail = minHead + (maxHead - minHead) / 2

      DequeMetaKey(
        head = initHeadTail,
        tail = initHeadTail,
        size = 0
      )
    }
  }

  private[database] val metaKey: Key[Option[DequeMetaKey]] = {
    val meta = "meta-key"

    val keyBytes = ByteBuffer
      .allocate(prefix.length + meta.length)
      .put(prefix)
      .put(meta.getBytes)
      .array()

    Key.opt(s"$name-deque-meta-key", columnFamily, keyBytes, DequeMetaKey.decodeMetaKey, DequeMetaKey.encodeMetaKey)
  }

  private def putMeta(head: Int, tail: Int, size: Int, rw: RW): Unit =
    rw.put(metaKey, DequeMetaKey(head, tail, size).some)

  private def encodeDequeKey(seq: Int): Key[T] = {
    val keyBytes = ByteBuffer
      .allocate(prefix.length + 1 + Ints.BYTES)
      .put(prefix)
      .putInt(seq)
      .array()

    Key(s"$name-deque-item", columnFamily, keyBytes, itemDecoder, itemEncoder)
  }

  private def headUntilSize(ro: ReadOnlyDB): Range = {
    val meta = ro.get(metaKey).getOrElse(DequeMetaKey.initState)
    val head = meta.head
    val size = meta.size

    head until head + size
  }

  protected[database] def addFirst(rw: RW, value: T): Unit = {
    val meta = rw.get(metaKey).getOrElse(DequeMetaKey.initState)
    val size = meta.size
    var head = meta.head

    if (size == 0) {
      val itemKey = encodeDequeKey(head)
      rw.put(itemKey, value)
    } else {
      head -= 1
      val itemKey = encodeDequeKey(head)
      rw.put(itemKey, value)
    }

    putMeta(head, meta.tail, size + 1, rw)
  }

  protected[database] def addLast(rw: RW, value: T): Unit = {
    val meta = rw.get(metaKey).getOrElse(DequeMetaKey.initState)
    val size = meta.size
    var tail = meta.tail

    if (size == 0) {
      val itemKey = encodeDequeKey(tail)
      rw.put(itemKey, value)
    } else {
      tail += 1
      val itemKey = encodeDequeKey(tail)
      rw.put(itemKey, value)
    }

    putMeta(meta.head, tail, size + 1, rw)
  }

  protected[database] def addLastN(rw: RW, values: Iterable[T]): Unit = {
    values.size match {
      case 0 => ()
      case 1 => addLast(rw, values.head)
      case _ =>
        val meta    = rw.get(metaKey).getOrElse(DequeMetaKey.initState)
        val size    = meta.size
        var newTail = if (size == 0) meta.tail else meta.tail + 1

        var itemKey = encodeDequeKey(newTail)
        rw.put(itemKey, values.head)

        for ((value, idx) <- values.zipWithIndex.tail) {
          itemKey = encodeDequeKey(newTail + idx)
          rw.put(itemKey, value)
        }

        newTail += values.size - 1

        putMeta(meta.head, newTail, size + values.size, rw)
    }

  }

  protected[database] def pollFirst(rw: RW): Option[T] =
    if (nonEmpty(rw)) {
      val meta    = rw.get(metaKey).getOrElse(DequeMetaKey.initState)
      val itemKey = encodeDequeKey(meta.head)
      val value   = rw.get(itemKey)

      if (meta.size == 1) {
        clear(rw)
      } else {
        putMeta(meta.head + 1, meta.tail, meta.size - 1, rw)
        rw.delete(itemKey)
      }

      value.some
    } else None

  protected[database] def pollLast(rw: RW): Option[T] =
    if (nonEmpty(rw)) {
      val meta    = rw.get(metaKey).getOrElse(DequeMetaKey.initState)
      val itemKey = encodeDequeKey(meta.tail)
      val value   = rw.get(itemKey)

      if (meta.size == 1) {
        clear(rw)
      } else {
        putMeta(meta.head, meta.tail - 1, meta.size - 1, rw)
        rw.delete(itemKey)
      }

      value.some
    } else None

  protected[database] def pollLastN(rw: RW, n: Int): Seq[T] = {
    val meta = rw.get(metaKey).getOrElse(DequeMetaKey.initState)
    require(meta.size >= n, "There is no as many elements as you try to remove")

    meta.size match {
      case 0 => Seq()
      case 1 =>
        val itemKey = encodeDequeKey(meta.tail)
        val value   = rw.get(itemKey)

        clear(rw)

        Seq(value)
      case _ =>
        val values = (0 until n).map { i =>
          val itemKey = encodeDequeKey(meta.tail - i)
          val value   = rw.get(itemKey)

          rw.delete(itemKey)

          value
        }

        val dequeResultSize = meta.size - n

        if (dequeResultSize == 0) {
          rw.put(metaKey, DequeMetaKey.initState.some)
        } else {
          putMeta(meta.head, meta.tail - n, dequeResultSize, rw)
        }

        values
    }
  }

  protected[database] def peekFirst(ro: ReadOnlyDB): Option[T] = {
    if (nonEmpty(ro)) {
      val head    = ro.get(metaKey).getOrElse(DequeMetaKey.initState).head
      val itemKey = encodeDequeKey(head)

      ro.get(itemKey).some
    } else None
  }

  protected[database] def peekLast(ro: ReadOnlyDB): Option[T] =
    if (isEmpty(ro)) {
      None
    } else {
      val head    = ro.get(metaKey).getOrElse(DequeMetaKey.initState).tail
      val itemKey = encodeDequeKey(head)

      ro.get(itemKey).some
    }

  protected[database] def contains(ro: ReadOnlyDB, value: T): Boolean = {
    headUntilSize(ro).exists { i =>
      val itemKey = encodeDequeKey(i)
      val item    = ro.get(itemKey)

      item == value
    }
  }

  protected[database] def toList(ro: ReadOnlyDB): List[T] = {
    headUntilSize(ro).view.map { i =>
      val itemKey = encodeDequeKey(i)
      val item    = ro.get(itemKey)
      item
    }.toList
  }

  protected[database] def clear(rw: RW): Unit = {
    // delete all keys
    headUntilSize(rw).foreach { i =>
      val itemKey = encodeDequeKey(i)
      rw.delete(itemKey)
    }

    // reset meta key
    rw.put(metaKey, DequeMetaKey.initState.some)
  }

  protected[database] def size(ro: ReadOnlyDB): Int =
    ro.get(metaKey).getOrElse(DequeMetaKey.initState).size

  protected[database] def isEmpty(ro: ReadOnlyDB): Boolean = {
    size(ro) == 0
  }

  protected[database] def nonEmpty(ro: ReadOnlyDB): Boolean =
    !isEmpty(ro)

  protected[database] def slice(from: Int, until: Int, ro: ReadOnlyDB): List[T] = {
    val head = ro.get(metaKey).getOrElse(DequeMetaKey.initState).head

    val l = math.max(from, 0)
    val r = math.min(until, size(ro))

    if (r <= 0) {
      List.empty
    } else {
      (l until r).map { i =>
        val itemKey = encodeDequeKey(i + head)
        val item    = ro.get(itemKey)
        item
      }.toList
    }
  }
}
