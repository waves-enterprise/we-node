package com.wavesenterprise.database

import com.google.common.primitives.Ints
import com.wavesenterprise.TransactionGen
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.{FreeSpec, Matchers}

class DatabaseSerializationSpec extends FreeSpec with Matchers with ScalaCheckPropertyChecks with TransactionGen {

  "Set serialization" in {
    forAll(Gen.listOf(Gen.posNum[Int]).map(_.toSet)) { ints =>
      val bytes = writeSet(Ints.toByteArray)(ints)
      val read  = readSet(Ints.fromByteArray)(bytes)
      read shouldBe ints
    }
  }

  "PrivacyDataId serialization" in {
    forAll(privacyDataIdGen) { id =>
      val bytes = writePrivacyDataId(id)
      val read  = readPrivacyDataId(bytes)
      read shouldBe id
    }
  }
}
