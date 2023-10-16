package com.wavesenterprise.db

import com.wavesenterprise.account.AddressScheme
import com.wavesenterprise.database.PrivacyState
import com.wavesenterprise.database.rocksdb.{MainRocksDBStorage, RocksDBWriter}
import com.wavesenterprise.history.{DefaultWESettings, Domain}
import com.wavesenterprise.settings.{ConsensusSettings, FunctionalitySettings, WESettings}
import com.wavesenterprise.state.{Blockchain, BlockchainUpdaterImpl}
import com.wavesenterprise.{NTPTime, TestHelpers, TestSchedulers}
import org.scalatest.Suite

import java.nio.file.Files

trait WithState {
  protected def withState[A](fs: FunctionalitySettings)(f: Blockchain with PrivacyState => A): A = {
    val path    = Files.createTempDirectory("rocksdb-test")
    val storage = MainRocksDBStorage.openDB(path.toAbsolutePath.toString)
    try f(new RocksDBWriter(storage, fs, ConsensusSettings.PoSSettings, 100000, 2000))
    finally {
      storage.close()
      TestHelpers.deleteRecursively(path)
    }
  }

  def withStateAndHistory(fs: FunctionalitySettings)(test: Blockchain => Any): Unit = withState(fs)(test)
}

trait WithDomain extends WithState with NTPTime with WithAddressSchema {
  _: Suite =>

  def withDomain[A](settings: WESettings = DefaultWESettings)(test: Domain => A): A = {
    try withState(settings.blockchain.custom.functionality) { blockchain =>
        withAddressSchema(settings.blockchain.custom.addressSchemeCharacter) {
          val storage = blockchain.asInstanceOf[RocksDBWriter].storage
          val bcu     = new BlockchainUpdaterImpl(blockchain, settings, ntpTime, TestSchedulers)
          try test(Domain(bcu, storage))
          finally bcu.shutdown()
        }
      }
    finally {}
  }
}

trait WithAddressSchema {
  def withAddressSchema[A](addressSchemeChar: Char)(test: => A): A = {
    val beforeTest = AddressScheme.getAddressSchema
    AddressScheme.setAddressSchemaByte(addressSchemeChar)
    try test
    finally {
      AddressScheme.setAddressSchemaByte(beforeTest.chainId.toChar)
    }
  }
}
