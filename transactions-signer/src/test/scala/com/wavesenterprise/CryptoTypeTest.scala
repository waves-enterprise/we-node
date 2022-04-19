package com.wavesenterprise

import org.scalatest.{EitherValues, FreeSpec, Matchers}

class CryptoTypeTest extends FreeSpec with Matchers with EitherValues {

  "parse case insensitive 'waves'" in {
    CryptoType.fromStr("waves") shouldBe CryptoType.Waves
    CryptoType.fromStr("waVes") shouldBe CryptoType.Waves
    CryptoType.fromStr("WaVes") shouldBe CryptoType.Waves
    CryptoType.fromStr("WaVeS") shouldBe CryptoType.Waves
  }

  "parse all unknown values to Unknown type" in {
    CryptoType.fromStr("xxx") shouldBe CryptoType.Unknown
    CryptoType.fromStr("uknown") shouldBe CryptoType.Unknown
    CryptoType.fromStr("123") shouldBe CryptoType.Unknown
  }
}
