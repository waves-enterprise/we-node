package com.wavesenterprise.network

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.crypto.SignatureLength
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.Signed
import org.scalacheck.Gen
import org.scalatest.concurrent.Eventually
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class MicroBlockInventorySpec extends AnyFreeSpec with Matchers with ScalaCheckPropertyChecks with Eventually with TransactionGen {

  private val microBlockInvV1Gen: Gen[MicroBlockInventoryV1] = for {
    acc          <- accountGen
    totalSig     <- byteArrayGen(SignatureLength)
    prevBlockSig <- byteArrayGen(SignatureLength)
  } yield MicroBlockInventoryV1(acc, ByteStr(totalSig), ByteStr(prevBlockSig))

  private val microBlockInvV2Gen: Gen[MicroBlockInventoryV2] = for {
    acc          <- accountGen
    baseBlockSig <- byteArrayGen(SignatureLength)
    totalSig     <- byteArrayGen(SignatureLength)
    prevBlockSig <- byteArrayGen(SignatureLength)
  } yield MicroBlockInventoryV2(ByteStr(baseBlockSig), acc, ByteStr(totalSig), ByteStr(prevBlockSig))

  "MicroBlockInventoryV1Spec" - {
    import com.wavesenterprise.network.message.MessageSpec.MicroBlockInventoryV1Spec._

    "serialization roundtrip" in forAll(microBlockInvV1Gen) { inv =>
      Signed.validate(inv) shouldBe 'right
      val restoredInv = deserializeData(serializeData(inv)).get
      Signed.validate(restoredInv) shouldBe 'right

      restoredInv shouldBe inv
    }
  }

  "MicroBlockInventoryV2Spec" - {
    import com.wavesenterprise.network.message.MessageSpec.MicroBlockInventoryV2Spec._

    "serialization roundtrip" in forAll(microBlockInvV2Gen) { inv =>
      Signed.validate(inv) shouldBe 'right
      val restoredInv = deserializeData(serializeData(inv)).get
      Signed.validate(restoredInv) shouldBe 'right

      restoredInv shouldBe inv
    }
  }

}
