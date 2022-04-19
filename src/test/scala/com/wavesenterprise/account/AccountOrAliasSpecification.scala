package com.wavesenterprise.account

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.utils.EitherUtils.EitherExt
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.{Matchers, PropSpec}

class AccountOrAliasSpecification extends PropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen {

  property("Account serialization round trip") {
    forAll(addressGen) { account =>
      val bytes   = account.bytes.arr
      val address = Address.fromBytes(bytes).explicitGet()
      address.stringRepr shouldBe account.stringRepr
    }
  }

  property("Alias serialization round trip") {
    forAll(aliasGen) { alias: Alias =>
      val bytes          = alias.bytes.arr
      val representation = Alias.fromBytes(bytes).explicitGet()
      representation.stringRepr shouldBe representation.stringRepr
    }
  }

  property("AccountOrAlias serialization round trip") {
    forAll(accountOrAliasGen) { aoa: AddressOrAlias =>
      val bytes          = aoa.bytes.arr
      val addressOrAlias = AddressOrAlias.fromBytes(bytes, 0).explicitGet()
      addressOrAlias._1.stringRepr shouldBe aoa.stringRepr
    }
  }
}
