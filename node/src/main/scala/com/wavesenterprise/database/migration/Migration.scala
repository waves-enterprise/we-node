package com.wavesenterprise.database.migration

import com.wavesenterprise.database.rocksdb.{BaseReadWriteDB, ColumnFamily}

trait Migration[CF <: ColumnFamily, RW <: BaseReadWriteDB[CF]] {
  def version: Int
  def apply: RW => Unit
}
