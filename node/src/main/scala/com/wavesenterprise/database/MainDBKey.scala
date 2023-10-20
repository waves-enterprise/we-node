package com.wavesenterprise.database

import java.util
import com.wavesenterprise.database.rocksdb.{ColumnFamily, MainDBColumnFamily}

abstract class BaseKey[V, CF <: ColumnFamily] {

  def name: String

  def columnFamily: CF

  def keyBytes: Array[Byte]

  def decode(bytes: Array[Byte]): V

  def encode(v: V): Array[Byte]
}

class BaseKeyImpl[V, CF <: ColumnFamily](val name: String,
                                         val columnFamily: CF,
                                         val keyBytes: Array[Byte],
                                         decoder: Array[Byte] => V,
                                         encoder: V => Array[Byte])
    extends BaseKey[V, CF] {

  override def decode(bytes: Array[Byte]): V = decoder(bytes)

  override def encode(v: V): Array[Byte] = encoder(v)

  override lazy val toString: String = BigInt(keyBytes).toString(16)

  override def equals(obj: Any): Boolean = obj match {
    case that: BaseKey[V, CF] => that.keyBytes.sameElements(keyBytes)
    case _                    => false
  }

  override def hashCode(): Int = util.Arrays.hashCode(keyBytes)
}

trait KeyConstructors[CF <: ColumnFamily] {

  val presetCF: CF

  def apply[V](name: String, columnFamily: CF, key: Array[Byte], decoder: Array[Byte] => V, encoder: V => Array[Byte]): BaseKey[V, CF] =
    new BaseKeyImpl[V, CF](name, columnFamily, key, decoder, encoder)

  def apply[V](name: String, key: Array[Byte], decoder: Array[Byte] => V, encoder: V => Array[Byte]): BaseKey[V, CF] =
    apply[V](name, presetCF, key, decoder, encoder)

  def opt[V](name: String, columnFamily: CF, key: Array[Byte], parser: Array[Byte] => V, encoder: V => Array[Byte]): BaseKey[Option[V], CF] =
    new BaseKeyImpl[Option[V], CF](name, columnFamily, key, Option(_).map(parser), _.fold[Array[Byte]](null)(encoder))

  def opt[V](name: String, key: Array[Byte], parser: Array[Byte] => V, encoder: V => Array[Byte]): BaseKey[Option[V], CF] =
    opt[V](name, presetCF, key, parser, encoder)

}

object MainDBKey extends KeyConstructors[MainDBColumnFamily] {
  override val presetCF: MainDBColumnFamily = MainDBColumnFamily.PresetCF
}
