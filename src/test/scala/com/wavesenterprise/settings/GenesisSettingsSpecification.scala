package com.wavesenterprise.settings

import com.wavesenterprise.TransactionGen
import org.scalacheck.Gen
import org.scalatest.{FreeSpec, Matchers}
import pureconfig.error.{ConfigReaderException, ConfigReaderFailure}
import pureconfig.{ConfigObjectSource, ConfigSource}

class GenesisSettingsSpecification extends FreeSpec with Matchers with TransactionGen {

  "PlainGenesisSettings" - {
    def configStringGen(timestamp: Long = GenesisSettings.minTimestamp): Gen[String] =
      for {
        acc1 <- accountGen
        acc2 <- accountGen
        acc3 <- accountGen
        acc4 <- accountGen
        acc5 <- accountGen
        acc6 <- accountGen
        configString = s"""{
                    | average-block-delay: 10000ms
                    | initial-base-target: 200000
                    | block-timestamp: $timestamp
                    | initial-balance: 10000000 WEST
                    | genesis-public-key-base-58: "HQLZLbtQsEPyHLSjyptHuhqhiGoPz3nVYz2j891PRym9ZuiGC6hwKHRktC9Ws8fLic7pvPpZ2DzWZ7UvXMyeKM5"
                    | signature: "5WepTySe3ab5k6WprLJUaFdDZLu71ecLirGN7WnfunDeixeZ8TBJcoYtnPtaksEBPQpRByZrNkKSp9xZrouPhc9K"
                    | transactions = [
                    |   {recipient: "${acc1.toAddress.stringRepr}", amount: 40000000000000},
                    |   {recipient: "${acc2.toAddress.stringRepr}", amount: 60000000000000},
                    |   {recipient: "${acc3.toAddress.stringRepr}", amount: 4000000000000},
                    |   {recipient: "${acc4.toAddress.stringRepr}", amount: 1600000000000},
                    |   {recipient: "${acc5.toAddress.stringRepr}", amount: 6000000000000},
                    |   {recipient: "${acc6.toAddress.stringRepr}", amount: 5000000000000}
                    | ]
                    | network-participants = [
                    |  {public-key: "${acc1.publicKeyBase58}", roles: [miner, permissioner]},
                    |  {public-key: "${acc2.publicKeyBase58}", roles: [miner]},
                    |  {public-key: "${acc3.publicKeyBase58}", roles: [permissioner]},
                    |  {public-key: "${acc4.publicKeyBase58}"},
                    |  {public-key: "${acc5.publicKeyBase58}", roles: [connection_manager]}
                    | ]
                    |}
      """.stripMargin
      } yield configString

    val testingGenesisConfig: ConfigObjectSource = ConfigSource.string {
      configStringGen().sample.get
    }

    "GenesisTransactionSettings should parse WEST amounts in units and support the older plain format" in {
      val account = accountGen.sample.get
      val olderConfig =
        ConfigSource.string(s"""{recipient: "${account.toAddress.stringRepr}", amount: 40000000000000}""").loadOrThrow[GenesisTransactionSettings]
      val newerConfig =
        ConfigSource.string(s"""{recipient: "${account.toAddress.stringRepr}", amount: 400000 WEST}""").loadOrThrow[GenesisTransactionSettings]
      olderConfig shouldBe newerConfig
    }

    "PlainGenesisSettings with network participants should be read as expected" in {
      val settings = testingGenesisConfig.loadOrThrow[GenesisSettings]
      settings shouldBe a[PlainGenesisSettings]
      settings.`type` shouldBe GenesisType.Plain
    }

    "should throw IllegalArgumentException when validating and if genesis timestamp less than mainnet start date" in {
      val badTimestamp    = GenesisSettings.minTimestamp - 1
      val badConfigString = configStringGen(badTimestamp).sample.get
      val badConfig       = ConfigSource.string(badConfigString)

      the[IllegalArgumentException] thrownBy {
        badConfig.loadOrThrow[GenesisSettings]
      } should have message {
        s"requirement failed: Genesis block timestamp '$badTimestamp' should be no less than '${GenesisSettings.minTimestamp}'"
      }
    }
  }

  "SnapshotBasedGenesisSettings" - {
    "should read as expected" in {
      val settings = ConfigSource
        .string {
          s"""{
             | type: "snapshot-based"
             | block-timestamp: ${GenesisSettings.minTimestamp}
             | genesis-public-key-base-58: "HQLZLbtQsEPyHLSjyptHuhqhiGoPz3nVYz2j891PRym9ZuiGC6hwKHRktC9Ws8fLic7pvPpZ2DzWZ7UvXMyeKM5"
             | signature: "5WepTySe3ab5k6WprLJUaFdDZLu71ecLirGN7WnfunDeixeZ8TBJcoYtnPtaksEBPQpRByZrNkKSp9xZrouPhc9K"
             |}
           """.stripMargin
        }
        .loadOrThrow[GenesisSettings]

      settings shouldBe a[SnapshotBasedGenesisSettings]
      settings.`type` shouldBe GenesisType.SnapshotBased
    }
  }

  "Unknown type should trow expected error" in {
    the[ConfigReaderException[ConfigReaderFailure]] thrownBy {
      ConfigSource
        .string {
          s"""{
             | type: "unknown-type"
             | block-timestamp: ${GenesisSettings.minTimestamp}
             | genesis-public-key-base-58: "HQLZLbtQsEPyHLSjyptHuhqhiGoPz3nVYz2j891PRym9ZuiGC6hwKHRktC9Ws8fLic7pvPpZ2DzW"
             | signature: "5WepTySe3ab5k6WprLJUaFdDZLu71ecLirGN7WnfunDeixeZ8TBJcoYtnPtaksEBPQpRByZrNkKSp"
             |}
           """.stripMargin
        }
        .loadOrThrow[GenesisSettings]
    } should have message {
      """Cannot convert configuration to a com.wavesenterprise.settings.GenesisSettings. Failures are:
        |  - Unable to parse the configuration: Unsupported Genesis type.
        |""".stripMargin
    }
  }
}
