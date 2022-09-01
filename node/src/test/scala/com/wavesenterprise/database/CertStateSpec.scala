package com.wavesenterprise.database

import com.wavesenterprise.block.Block
import com.wavesenterprise.consensus.ConsensusPostAction
import com.wavesenterprise.database.rocksdb.RocksDBWriter
import com.wavesenterprise.history.{BlockchainFactory, DefaultWESettings}
import com.wavesenterprise.settings.ConsensusType
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.{BlockchainUpdater, CommonGen}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.{TestHelpers, TestSchedulers, TestTime, WithDB}
import org.apache.commons.codec.digest.DigestUtils
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import tools.GenHelper.ExtendedGen
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class CertStateSpec extends AnyFreeSpec with ScalaCheckPropertyChecks with WithDB with WithTestCert with CommonGen with Matchers {

  case class FixtureParams(rocksDBWriter: RocksDBWriter, blockchainUpdater: BlockchainUpdater)

  def fixture(testBlock: FixtureParams => Unit): Unit = {
    val genesisSigner               = accountGen.generateSample()
    val genesisSettings             = TestHelpers.buildGenesis(genesisSigner, Map.empty)
    val (writer, blockchainUpdater) = BlockchainFactory(DefaultWESettings, storage, new TestTime(), TestSchedulers)

    blockchainUpdater
      .processBlock(Block.genesis(genesisSettings, ConsensusType.PoS).explicitGet(), ConsensusPostAction.NoAction)
      .explicitGet()

    testBlock(FixtureParams(writer, blockchainUpdater))
  }

  "Put and get certificate" in fixture {
    case FixtureParams(writer, updater) =>
      val dn          = testCert.getSubjectX500Principal.getName
      val fingerPrint = ByteStr(DigestUtils.sha1(testCert.getEncoded))

      writer.certByDistinguishedName(dn) shouldBe None
      updater.certByDistinguishedName(dn) shouldBe None

      writer.certByPublicKey(testPka) shouldBe None
      updater.certByPublicKey(testPka) shouldBe None

      writer.certByFingerPrint(fingerPrint) shouldBe None
      updater.certByFingerPrint(fingerPrint) shouldBe None

      writer.putCert(testCert)

      writer.certByDistinguishedName(dn) shouldBe Some(testCert)
      updater.certByDistinguishedName(dn) shouldBe Some(testCert)

      writer.certByPublicKey(testPka) shouldBe Some(testCert)
      updater.certByPublicKey(testPka) shouldBe Some(testCert)

      writer.certByFingerPrint(fingerPrint) shouldBe Some(testCert)
      updater.certByFingerPrint(fingerPrint) shouldBe Some(testCert)
  }

  "Put and get certificates at height" in fixture {
    case FixtureParams(writer, updater) =>
      val testHeight = 0

      writer.certsAtHeight(testHeight) shouldBe Set.empty
      updater.certsAtHeight(testHeight) shouldBe Set.empty

      writer.putCert(testCert)
      writer.putCertsAtHeight(testHeight, Set(testCert))

      writer.certsAtHeight(testHeight) shouldBe Set(testCert)
      updater.certsAtHeight(testHeight) shouldBe Set(testCert)
  }
}
