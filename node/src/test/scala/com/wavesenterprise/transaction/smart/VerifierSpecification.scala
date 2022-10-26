package com.wavesenterprise.transaction.smart

import com.wavesenterprise.account.{PrivateKeyAccount, PublicKeyAccount}
import com.wavesenterprise.state.{AssetDescription, Blockchain}
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.transaction.assets.exchange._
import com.wavesenterprise.transaction.smart.script.ScriptCompiler
import com.wavesenterprise.{NTPTime, TransactionGen}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class VerifierSpecification extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with MockFactory with TransactionGen with NTPTime {

  property("ExchangeTransaction - blockchain's functions should be allowed during an order's verification") {
    forAll(exchangeTransactionV2Gen) { tx: ExchangeTransaction =>
      val bc = stub[Blockchain]
      Seq(tx.buyOrder.assetPair.amountAsset, tx.buyOrder.assetPair.priceAsset).flatten.foreach { assetId =>
        (bc.assetDescription _).when(assetId).returns(mkAssetDescription(tx.sender, 8))
        (bc.assetScript _).when(assetId).returns(None)
      }

      val scriptText =
        """match tx {
          |  case o: Order => height >= 0
          |  case _ => true
          |}""".stripMargin
      val script = ScriptCompiler(scriptText, isAssetScript = false).explicitGet()._1

      (bc.accountScript _).when(tx.sellOrder.sender.toAddress).returns(None)
      (bc.accountScript _).when(tx.buyOrder.sender.toAddress).returns(Some(script))
      (bc.accountScript _).when(tx.sender.toAddress).returns(None)

      (bc.height _).when().returns(0)

      Verifier(bc, 0)(tx) shouldBe 'right
    }
  }

  private def mkAssetDescription(matcherAccount: PublicKeyAccount, decimals: Int): Option[AssetDescription] =
    Some(
      AssetDescription(
        matcherAccount.toAddress.toAssetHolder,
        1,
        System.currentTimeMillis(),
        "coin",
        "description",
        decimals.toByte,
        reissuable = false,
        BigInt(0),
        None,
        sponsorshipIsEnabled = false
      ))

  private val exchangeTransactionV2Gen: Gen[ExchangeTransaction] = for {
    sender1: PrivateKeyAccount <- accountGen
    sender2: PrivateKeyAccount <- accountGen
    assetPair                  <- assetPairGen
    r                          <- exchangeV2GeneratorP(sender1, sender2, assetPair.amountAsset, assetPair.priceAsset, orderVersions = Set(2))
  } yield r

}
