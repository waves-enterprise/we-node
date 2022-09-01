package com.wavesenterprise.network

import com.wavesenterprise.RxSetup
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.block.{MicroBlock, TxMicroBlock}
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.network.message.MessageSpec.MicroBlockResponseV2Spec
import com.wavesenterprise.certs.CertChainStoreGen
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.transfer.TransferTransactionV2
import com.wavesenterprise.utils.EitherUtils._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class MicroBlockResponseV2SerializationSpec extends AnyFreeSpec with Matchers with RxSetup with CertChainStoreGen with ScalaCheckPropertyChecks {

  // creating MicroBlock
  val signer: PrivateKeyAccount = TestBlock.defaultSigner

  def buildMicroBlock(sign: ByteStr, prevSign: ByteStr): MicroBlock = {
    val tx = TransferTransactionV2.selfSigned(signer, None, None, 1, 1, 1, signer.toAddress, Array.emptyByteArray).explicitGet()
    TxMicroBlock.buildAndSign(signer, 1L, Seq(tx), prevSign, sign).explicitGet()
  }

  "encode and decode microblock with certChainStore" in {
    forAll(certChainStoreGen) { certChainStore =>
      val firstSign: ByteStr                         = buildTestSign(1)
      val secondSign: ByteStr                        = buildTestSign(2)
      val microBlock: MicroBlock                     = buildMicroBlock(firstSign, secondSign)
      val microBlockResponseV2: MicroBlockResponseV2 = MicroBlockResponseV2(microBlock, certChainStore)

      val serializedData: Array[Byte]            = MicroBlockResponseV2Spec.serializeData(microBlockResponseV2)
      val deserializedData: MicroBlockResponseV2 = MicroBlockResponseV2Spec.deserializeData(serializedData).get

      deserializedData shouldBe microBlockResponseV2
    }
  }
}
