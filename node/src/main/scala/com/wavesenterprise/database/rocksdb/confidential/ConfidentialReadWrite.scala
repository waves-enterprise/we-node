package com.wavesenterprise.database.rocksdb.confidential

trait ConfidentialReadWrite {
  protected def storage: ConfidentialRocksDBStorage
  protected[database] def readOnly[A](f: ConfidentialReadOnlyDB => A): A = storage.readOnly(f)

  /**
    * Async unsafe method!
    * There is no locks inside, do not fall in trap cause of his name.
    */
  protected def readWrite[A](f: ConfidentialReadWriteDB => A): A = storage.readWrite(f)
}
