package com.wavesenterprise.database

import java.util

import com.wavesenterprise.database.rocksdb.ColumnFamily
import com.wavesenterprise.database.rocksdb.ColumnFamily.DefaultCF

trait Key[V] {

  def name: String

  def columnFamily: ColumnFamily

  def keyBytes: Array[Byte]

  def decode(bytes: Array[Byte]): V

  def encode(v: V): Array[Byte]
}

private class KeyImpl[V](val name: String,
                         val columnFamily: ColumnFamily,
                         val keyBytes: Array[Byte],
                         decoder: Array[Byte] => V,
                         encoder: V => Array[Byte])
    extends Key[V] {

  override def decode(bytes: Array[Byte]): V = decoder(bytes)

  override def encode(v: V): Array[Byte] = encoder(v)

  override lazy val toString: String = BigInt(keyBytes).toString(16)

  override def equals(obj: Any): Boolean = obj match {
    case that: Key[V] => that.keyBytes.sameElements(keyBytes)
    case _            => false
  }

  override def hashCode(): Int = util.Arrays.hashCode(keyBytes)
}

object Key {

  def apply[V](name: String, key: Array[Byte], decoder: Array[Byte] => V, encoder: V => Array[Byte]): Key[V] = {
    new KeyImpl[V](name, DefaultCF, key, decoder, encoder)
  }

  def apply[V](name: String, columnFamily: ColumnFamily, key: Array[Byte], decoder: Array[Byte] => V, encoder: V => Array[Byte]): Key[V] = {
    new KeyImpl[V](name, columnFamily, key, decoder, encoder)
  }

  def opt[V](name: String, key: Array[Byte], parser: Array[Byte] => V, encoder: V => Array[Byte]): Key[Option[V]] = {
    new KeyImpl[Option[V]](name, DefaultCF, key, Option(_).map(parser), _.fold[Array[Byte]](null)(encoder))
  }

  def opt[V](name: String, columnFamily: ColumnFamily, key: Array[Byte], parser: Array[Byte] => V, encoder: V => Array[Byte]): Key[Option[V]] = {
    new KeyImpl[Option[V]](name, columnFamily, key, Option(_).map(parser), _.fold[Array[Byte]](null)(encoder))
  }
}
