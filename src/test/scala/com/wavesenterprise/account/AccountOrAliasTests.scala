package com.wavesenterprise.account

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.utils.EitherUtils.EitherExt
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.{Matchers, PropSpec}

class AccountOrAliasTests extends PropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen {

  property("Account should get parsed correctly") {
    forAll(addressGen) { address =>
      AddressOrAlias.fromString(address.address).explicitGet() shouldBe an[Address]
      AddressOrAlias.fromString(s"address:$address").explicitGet() shouldBe an[Address]

      Address.fromString(address.address).explicitGet() shouldBe an[Address]
      Address.fromString(s"address:$address").explicitGet() shouldBe an[Address]
    }

  }

  property("Alias should get parsed correctly") {
    val alias = AddressOrAlias.fromString("alias:T:sasha").explicitGet().asInstanceOf[Alias]
    alias.name shouldBe "sasha"
    alias.chainId shouldBe 'T'

    val alias2 = Alias.fromString("alias:T:sasha").explicitGet()
    alias2.name shouldBe "sasha"
    alias2.chainId shouldBe 'T'

  }
  property("Alias cannot be from other network") {
    AddressOrAlias.fromString("alias:Q:sasha") shouldBe 'left
  }

  property("Malformed aliases cannot be reconstructed") {
    AddressOrAlias.fromString("alias::sasha") shouldBe 'left
    AddressOrAlias.fromString("alias:T: sasha") shouldBe 'left
    AddressOrAlias.fromString("alias:T:sasha\nivanov") shouldBe 'left
    AddressOrAlias.fromString("alias:T:s") shouldBe 'left
    AddressOrAlias.fromString("alias:TTT:sasha") shouldBe 'left

    Alias.fromString("alias:T: sasha") shouldBe 'left
    Alias.fromString("alias:T:sasha\nivanov") shouldBe 'left
    Alias.fromString("alias::sasha") shouldBe 'left
    Alias.fromString("alias:T:s") shouldBe 'left
    Alias.fromString("alias:TTT:sasha") shouldBe 'left

    Alias.fromString("aliaaas:W:sasha") shouldBe 'left
  }

  property("Unknown address schemes cannot be parsed") {
    AddressOrAlias.fromString("postcode:119072") shouldBe 'left
  }

  property("Address from PublicKeyAccount for different chainId") {
    forAll(accountGen, Gen.alphaUpperChar.map(_.toByte)) {
      case (acc, externalChainId) =>
        if (AddressScheme.getAddressSchema.chainId != externalChainId) {
          acc.toAddress shouldNot be(acc.toAddress(externalChainId))
        } else {
          acc.toAddress shouldBe acc.toAddress(externalChainId)
        }
    }
  }

  property("Address.fromString for different chainId") {
    forAll(accountGen, Gen.alphaUpperChar.map(_.toByte)) {
      case (acc, externalChainId) =>
        val addressWithExternalChainId = Address.fromString(acc.toAddress(externalChainId).address, externalChainId).right.get
        addressWithExternalChainId shouldBe acc.toAddress(externalChainId)

        val addressWithGlobalChainId = Address.fromString(acc.toAddress.address).right.get
        addressWithGlobalChainId shouldBe acc.toAddress

        if (AddressScheme.getAddressSchema.chainId != externalChainId) {
          addressWithExternalChainId shouldNot be(addressWithGlobalChainId)
        } else {
          addressWithExternalChainId shouldBe addressWithGlobalChainId
        }
    }
  }

}
