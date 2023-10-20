package com.wavesenterprise

import java.nio.file.Files

import com.wavesenterprise.database.rocksdb.MainRocksDBStorage
import org.scalatest.{BeforeAndAfterEach, TestSuite}

trait WithDB extends BeforeAndAfterEach {
  this: TestSuite =>

  private val path                                  = Files.createTempDirectory("lvl").toAbsolutePath
  private var currentDBInstance: MainRocksDBStorage = _

  def storage: MainRocksDBStorage = currentDBInstance

  protected def migrateScheme: Boolean = true

  override def beforeEach(): Unit = {
    currentDBInstance = MainRocksDBStorage.openDB(path.toAbsolutePath.toString, migrateScheme)
    super.beforeEach()
  }

  override def afterEach(): Unit =
    try {
      super.afterEach()
      storage.close()
    } finally {
      TestHelpers.deleteRecursively(path)
    }
}
