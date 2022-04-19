package com.wavesenterprise.state.diffs.smart.predef

import com.wavesenterprise.account.Address
import com.wavesenterprise.lang.Testing._
import com.wavesenterprise.state._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalatest.{Matchers, PropSpec}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import scodec.bits.ByteVector

class AddressTest extends PropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {
  property("should calculate address from public key") {
    forAll(accountGen) { acc =>
      val script =
        s"""
           | let pk = base58'${ByteStr(acc.publicKey.getEncoded).base58}'
           | let address = addressFromPublicKey(pk)
           | address.bytes
        """.stripMargin
      runScript(script) shouldBe evaluated(ByteVector(Address.fromPublicKey(acc.publicKey.getEncoded, chainId).bytes.arr))
    }
  }

  property("should calculate address from bytes") {
    forAll(accountGen) { acc =>
      val addressBytes = Address.fromPublicKey(acc.publicKey.getEncoded, chainId).bytes
      val script =
        s"""
           | let addressString = "${addressBytes.base58}"
           | let maybeAddress = addressFromString(addressString)
           | let address = extract(maybeAddress)
           | address.bytes
        """.stripMargin
      runScript(script) shouldBe evaluated(ByteVector(Address.fromBytes(addressBytes.arr, chainId).explicitGet().bytes.arr))
    }
  }

  property("should calculate address and return bytes without intermediate ref") {
    forAll(accountGen) { acc =>
      val addressBytes = Address.fromPublicKey(acc.publicKey.getEncoded, chainId).bytes
      val script =
        s"""
           | let addressString = "${addressBytes.base58}"
           | let maybeAddress = addressFromString(addressString)
           | extract(maybeAddress).bytes
        """.stripMargin
      runScript(script) shouldBe evaluated(ByteVector(Address.fromBytes(addressBytes.arr, chainId).explicitGet().bytes.arr))
    }
  }
}
