package com.wavesenterprise.mining

import java.nio.charset.StandardCharsets.UTF_8

import com.wavesenterprise.account.Address
import com.wavesenterprise.lang.ScriptVersion.Versions.V1
import com.wavesenterprise.lang.v1.compiler.Terms
import com.wavesenterprise.state.{AssetDescription, Blockchain, ByteStr}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.transaction.smart.script.v1.ScriptV1
import com.wavesenterprise.transaction.transfer.TransferTransactionV2
import com.wavesenterprise.{CryptoHelpers, TransactionGen}
import org.scalamock.scalatest.PathMockFactory
import org.scalatest.{FreeSpec, Matchers}

class TxEstimatorsSuite extends FreeSpec with Matchers with PathMockFactory with TransactionGen {
  "scriptRunNumber" - {
    "smart account" - {
      "should not count transactions going from a regular account" in {
        val blockchain = stub[Blockchain]
        (blockchain.hasScript _).when(*).onCall((_: Address) => false).anyNumberOfTimes()

        TxEstimators.scriptRunNumber(blockchain, transferWestTx) shouldBe 0
      }

      "should count transactions going from a smart account" in {
        val blockchain = stub[Blockchain]
        (blockchain.hasScript _).when(*).onCall((_: Address) => true).anyNumberOfTimes()

        TxEstimators.scriptRunNumber(blockchain, transferWestTx) shouldBe 1
      }
    }

    "smart tokens" - {
      "should not count transactions working with a regular tokens" in {
        val blockchain = stub[Blockchain]
        (blockchain.hasScript _).when(*).onCall((_: Address) => false).anyNumberOfTimes()
        (blockchain.assetDescription _).when(*).onCall((_: ByteStr) => None).anyNumberOfTimes()

        TxEstimators.scriptRunNumber(blockchain, transferAssetsTx) shouldBe 0
      }

      "should count transactions working with smart tokens" in {
        val blockchain = stub[Blockchain]
        (blockchain.hasScript _).when(*).onCall((_: Address) => false).anyNumberOfTimes()
        (blockchain.assetDescription _).when(*).onCall((_: ByteStr) => Some(assetDescription)).anyNumberOfTimes()

        TxEstimators.scriptRunNumber(blockchain, transferAssetsTx) shouldBe 1
      }
    }

    "both - should double count transactions working with smart tokens from samrt account" in {
      val blockchain = stub[Blockchain]
      (blockchain.hasScript _).when(*).onCall((_: Address) => true).anyNumberOfTimes()
      (blockchain.assetDescription _).when(*).onCall((_: ByteStr) => Some(assetDescription)).anyNumberOfTimes()

      TxEstimators.scriptRunNumber(blockchain, transferAssetsTx) shouldBe 2
    }
  }

  private val assetId          = ByteStr("coin_id".getBytes(UTF_8))
  private val script           = ScriptV1(V1, Terms.TRUE, checkSize = false).explicitGet()
  private val senderAccount    = CryptoHelpers.generatePrivateKey
  private val recipientAccount = CryptoHelpers.generatePrivateKey

  private val transferWestTx = TransferTransactionV2
    .selfSigned(
      sender = senderAccount,
      assetId = None,
      feeAssetId = None,
      timestamp = System.currentTimeMillis(),
      amount = 1,
      fee = 100000,
      recipient = recipientAccount.toAddress,
      attachment = Array.emptyByteArray
    )
    .explicitGet()

  private val transferAssetsTx = TransferTransactionV2
    .selfSigned(
      sender = senderAccount,
      assetId = Some(assetId),
      feeAssetId = None,
      timestamp = System.currentTimeMillis(),
      amount = 1,
      fee = 100000,
      recipient = recipientAccount.toAddress,
      attachment = Array.emptyByteArray
    )
    .explicitGet()

  private val assetDescription = AssetDescription(
    issuer = senderAccount,
    height = 1,
    timestamp = System.currentTimeMillis(),
    name = "coin",
    description = "description",
    decimals = 2,
    reissuable = false,
    totalVolume = Long.MaxValue,
    script = Some(script),
    sponsorshipIsEnabled = false
  )
}
