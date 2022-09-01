package com.wavesenterprise.state

import com.wavesenterprise.account.Address
import com.wavesenterprise.crypto.SignatureLength
import com.wavesenterprise.db.WithDomain
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.transaction.GenesisTransaction
import com.wavesenterprise.{NoShrink, TestTime, TransactionGen}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class CommonSpec extends AnyFreeSpec with Matchers with WithDomain with TransactionGen with ScalaCheckPropertyChecks with NoShrink {
  private val time          = new TestTime
  private def nextTs        = time.getTimestamp()
  private val AssetIdLength = 32

  private def genesisBlock(genesisTs: Long, address: Address, initialBalance: Long) = TestBlock.create(
    genesisTs,
    ByteStr(Array.fill[Byte](SignatureLength)(0)),
    Seq(GenesisTransaction.create(address, initialBalance, genesisTs).explicitGet())
  )

  "Common Conditions" - {
    "Zero balance of absent asset" in forAll(accountGen, positiveLongGen, byteArrayGen(AssetIdLength)) {
      case (sender, initialBalance, assetId) =>
        withDomain() { d =>
          d.appendBlock(genesisBlock(nextTs, sender.toAddress, initialBalance))
          d.portfolio(sender.toAddress).balanceOf(Some(ByteStr(assetId))) shouldEqual 0L
        }
    }
  }
}
