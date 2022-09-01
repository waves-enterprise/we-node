package com.wavesenterprise.settings

import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FeeSettingsSpecification extends AnyFlatSpec with Matchers {

  "FeeSettings" should "correctly read disabled state" in {
    val config = ConfigSource.string {
      """{
      |   enabled = false
      |}""".stripMargin
    }

    val feeSettings = config.loadOrThrow[FeeSettings]
    feeSettings should be(FeeSettings.FeesDisabled)
  }

  it should "read a well built config without specified enabled key" in {

    val configSource = ConfigSource.string {
      """{
        | base {
        |    burn = 0.00000006 WEST
        |    reissue = 0.00000005 WEST
        |    create-alias = 0.00000010 WEST
        |    data = 0.00000012 WEST
        |    sponsor-fee = 0.00000014 WEST
        |    lease = 0.00000008 WEST
        |    issue = 0.00000003 WEST
        |    exchange = 0.00000007 WEST
        |    transfer = 0.00000004 WEST
        |    set-script = 0.00000013 WEST
        |    set-asset-script = 0.00000015 WEST
        |    mass-transfer = 0.00000011 WEST
        |    lease-cancel = 0.00000009 WEST
        |    permit = 0.00000102 WEST
        |    create-contract = 0.00000103 WEST
        |    call-contract = 0.00000104 WEST
        |    disable-contract = 0.00000106 WEST
        |    update-contract = 0.00000107 WEST
        |    register-node = 0.00000111 WEST
        |    create-policy = 0.00000112 WEST
        |    update-policy = 0.00000113 WEST
        |    policy-data-hash = 0.00000114 WEST
        | }
        |
        | additional {
        |    mass-transfer = 0.00000011 WEST
        |    data = 0.00000012 WEST
        | }
        |}""".stripMargin
    }

    val feeSettings = configSource.loadOrThrow[FeeSettings]

    val expectedMap = FeeSettings.txTypesRequiringFees.map {
      case (_, txParser) =>
        txParser.typeId -> WestAmount(txParser.typeId.toLong)
    }
    val expectedAdditional = FeeSettings.txTypesRequiringAdditionalFees.map {
      case (_, txParser) =>
        txParser.typeId -> WestAmount(txParser.typeId.toLong)
    }

    feeSettings shouldBe FeeSettings.FeesEnabled(expectedMap, expectedAdditional)
  }

  it should "read a well built config with enabled key" in {
    val config = ConfigSource.string("""{
      | enabled = true
      | base {
      |    burn = 0.00000006 WEST
      |    reissue = 0.00000005 WEST
      |    create-alias = 0.00000010 WEST
      |    data = 0.00000012 WEST
      |    sponsor-fee = 0.00000014 WEST
      |    lease = 0.00000008 WEST
      |    issue = 0.00000003 WEST
      |    exchange = 0.00000007 WEST
      |    transfer = 0.00000004 WEST
      |    set-script = 0.00000013 WEST
      |    set-asset-script = 0.00000015 WEST
      |    mass-transfer = 0.00000011 WEST
      |    lease-cancel = 0.00000009 WEST
      |    permit = 0.00000102 WEST
      |    create-contract = 0.00000103 WEST
      |    call-contract = 0.00000104 WEST
      |    disable-contract = 0.00000106 WEST
      |    update-contract = 0.00000107 WEST
      |    register-node = 0.00000111 WEST
      |    create-policy = 0.00000112 WEST
      |    update-policy = 0.00000113 WEST
      |    policy-data-hash = 0.00000114 WEST
      | }
      |
      | additional {
      |   mass-transfer = 0.00000011 WEST
      |   data = 0.00000012 WEST
      | }
      |}""".stripMargin)

    val feeSettings = config.loadOrThrow[FeeSettings]
    val expectedMap = FeeSettings.txTypesRequiringFees.map {
      case (_, txParser) =>
        txParser.typeId -> WestAmount(txParser.typeId.toLong)
    }
    val expectedAdditional = FeeSettings.txTypesRequiringAdditionalFees.map {
      case (_, txParser) =>
        txParser.typeId -> WestAmount(txParser.typeId.toLong)
    }

    feeSettings should be(FeeSettings.FeesEnabled(expectedMap, expectedAdditional))
  }

  it should "ignore settings if enabled = false" in {
    val configSource = ConfigSource.string("""{
      | enabled = false
      | base {
      |    burn = 0.00000006 WEST
      |    reissue = 0.00000005 WEST
      |    data = 0.00000012 WEST
      |    sponsor-fee = 0.00000014 WEST
      |    lease = 0.00000008 WEST
      |    issue = 0.00000003 WEST
      |    exchange = 0.00000007 WEST
      |    transfer = 0.00000004 WEST
      |    set-script = 0.00000013 WEST
      |    set-asset-script = 0.00000015 WEST
      |    mass-transfer = 0.00000011 WEST
      |    lease-cancel = 0.00000009 WEST
      |    create-contract = 0.00000103 WEST
      |    call-contract = 0.00000104 WEST
      |    executed-contract = 0.00000105 WEST
      |    disable-contract = 0.00000106 WEST
      |    update-contract = 0.00000107 WEST
      |    create-policy = 0.00000112 WEST
      |    update-policy = 0.00000113 WEST
      |    policy-data-hash = 0.00000114 WEST
      | }
      |
      | additional {
      |    mass-transfer = 0.00000011 WEST
      |    data = 0.00000012 WEST
      | }
      |}""".stripMargin)

    val feeSettings = configSource.loadOrThrow[FeeSettings]
    feeSettings shouldBe FeeSettings.FeesDisabled
  }

  it should "throw requirement exceptions if one transaction fee is not specified" in {
    val config = ConfigSource.string("""{
      | base {
      |    burn = 0.00000006 WEST
      |    reissue = 0.00000005 WEST
      |    create-alias = 0.00000010 WEST
      |    data = 0.00000012 WEST
      |    sponsor-fee = 0.00000014 WEST
      |    lease = 0.00000008 WEST
      |    issue = 0.00000003 WEST
      |    exchange = 0.00000007 WEST
      |    transfer = 0.00000004 WEST
      |    set-script = 0.00000013 WEST
      |    set-asset-script = 0.00000015 WEST
      |    mass-transfer = 0.00000011 WEST
      |    permit = 0.00000102 WEST
      |    create-contract = 0.00000103 WEST
      |    call-contract = 0.00000104 WEST
      |    disable-contract = 0.00000106 WEST
      |    update-contract = 0.00000107 WEST
      |    create-policy = 0.00000112 WEST
      |    update-policy = 0.00000113 WEST
      |    register-node = 0.00000111 WEST
      |    policy-data-hash = 0.00000114 WEST
      | }
      |
      | additional {
      |    mass-transfer = 0.00000011 WEST
      |    data = 0.00000012 WEST
      | }
      |}""".stripMargin)

    val caughtException = intercept[IllegalArgumentException] {
      config.loadOrThrow[FeeSettings]
    }
    caughtException.getMessage should include(s"Unspecified fees for transaction types: [lease-cancel]")
  }

  it should "throw requirement exceptions if one transaction fee has invalid notation" in {
    val configSource = ConfigSource.string {
      """{
        | base {
        |    burn = 0.00000006 WEST
        |    reissue = 0.00000005 VTS
        |    create-alias = 0.00000010 WEST
        |    data = 0.00000012 WEST
        |    sponsor-fee = 0.00000014 WEST
        |    lease = 0.00000008 WEST
        |    issue = 0.00000003 WEST
        |    exchange = 0.00000007 WEST
        |    transfer = 0.00000004 WEST
        |    set-script = 0.00000013 WEST
        |    set-asset-script = 0.00000015 WEST
        |    mass-transfer = 0.00000011 WEST
        |    lease-cancel = 0.00000009 WEST
        |    permit = 0.00000102 WEST
        |    create-contract = 0.00000103 WEST
        |    call-contract = 0.00000104 WEST
        |    disable-contract = 0.00000106 WEST
        |    update-contract = 0.00000107 WEST
        |    register-node = 0.00000111 WEST
        |    create-policy = 0.00000112 WEST
        |    update-policy = 0.00000113 WEST
        |    policy-data-hash = 0.00000114 WEST
        | }
        |
        | additional {
        |   mass-transfer = 0.00000011 WEST
        |   data = 0.00000012 WEST
        | }
        |}""".stripMargin
    }

    val caughtException = intercept[ConfigReaderException[_]] {
      configSource.loadOrThrow[FeeSettings]
    }

    caughtException.getMessage should (include("reissue") and include("Failed to parse") and include("as WEST Amount"))
  }

  it should "list all missing transaction names from invalid config" in {
    val configSource = ConfigSource.string {
      """{
        | base {
        |    burn = 0.00000006 WEST
        |    reissue = 0.00000005 WEST
        |    data = 0.00000012 WEST
        |    sponsor-fee = 0.00000014 WEST
        |    lease = 0.00000008 WEST
        |    exchange = 0.00000007 WEST
        |    transfer = 0.00000004 WEST
        |    set-script = 0.00000013 WEST
        |    set-asset-script = 0.00000015 WEST
        |    mass-transfer = 0.00000011 WEST
        |    create-contract = 0.00000103 WEST
        |    call-contract = 0.00000104 WEST
        |    disable-contract = 0.00000106 WEST
        |    update-contract = 0.00000107 WEST
        |    create-policy = 0.00000112 WEST
        |    update-policy = 0.00000113 WEST
        |    policy-data-hash = 0.00000114 WEST
        | }
        |}""".stripMargin
    }

    val caughtException = intercept[IllegalArgumentException] {
      configSource.loadOrThrow[FeeSettings]
    }

    caughtException.getMessage should (include("lease-cancel") and include("issue"))
  }
}
