package com.wavesenterprise.database.snapshot

import com.wavesenterprise.api.http.snapshot.EnabledSnapshotApiRoute.GenesisSettingsFormat
import com.wavesenterprise.block.Block
import com.wavesenterprise.settings.{ConsensusType, GenesisSettingsVersion, GenesisType, PlainGenesisSettings, WestAmount}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.CommonGen
import com.wavesenterprise.utils.EitherUtils.EitherExt
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import play.api.libs.json.Json
import pureconfig.ConfigSource

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class SnapshotGenesisConfigSpec extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with CommonGen {

  private val miner = accountGen.sample.get

  property("Snapshot genesis block to GenesisSettings serialization") {
    val genesis  = SnapshotGenesis.buildGenesis(ConsensusType.CFT, miner, System.currentTimeMillis(), senderRoleEnabled = false).explicitGet()
    val settings = SnapshotGenesis.mapToSettings(genesis)

    val genesisEither = Block.genesis(settings, ConsensusType.CFT)
    genesisEither shouldBe 'right
    genesisEither.explicitGet() shouldBe genesis
  }

  property("JSON format validation for GenesisSettings") {
    val blockTimestamp  = System.currentTimeMillis()
    val genesisPKBase58 = "HQLZLbtQsEPyHLSjyptHuhqhiGoPz3nVYz2j891PRym9ZuiGC6hwKHRktC9Ws8fLic7pvPpZ2DzWZ7UvXMyeKM5"
    val signatureStr    = "5WepTySe3ab5k6WprLJUaFdDZLu71ecLirGN7WnfunDeixeZ8TBJcoYtnPtaksEBPQpRByZrNkKSp9xZrouPhc9K"

    val settings = PlainGenesisSettings(
      blockTimestamp = blockTimestamp,
      initialBalance = WestAmount(0),
      genesisPublicKeyBase58 = genesisPKBase58,
      signature = Some(ByteStr.decodeBase58(signatureStr).get),
      transactions = Seq.empty,
      networkParticipants = Seq.empty,
      initialBaseTarget = 0,
      averageBlockDelay = FiniteDuration(0, TimeUnit.SECONDS),
      version = GenesisSettingsVersion.ModernVersion,
      senderRoleEnabled = true
    )

    val json = Json.parse(s"""{
                       "block-timestamp": $blockTimestamp,
                       "initial-balance": 0,
                       "genesis-public-key-base-58": "$genesisPKBase58",
                       "signature": "$signatureStr",
                       "transactions": [],
                       "network-participants": [],
                       "initial-base-target": 0,
                       "average-block-delay": 0,
                       "version": ${GenesisSettingsVersion.ModernVersion.value},
                       "sender-role-enabled": true,
                       "type": "${GenesisType.Plain.entryName}"
                    }
    """)

    Json.toJson(settings) shouldBe (json)
  }

  property("Parse GenesisSettings from JSON config") {
    val genesis  = SnapshotGenesis.buildGenesis(ConsensusType.CFT, miner, System.currentTimeMillis(), senderRoleEnabled = false).explicitGet()
    val settings = SnapshotGenesis.mapToSettings(genesis)

    val json   = Json.toJson(settings)
    val config = ConfigSource.string(json.toString())
    config.loadOrThrow[PlainGenesisSettings] shouldBe settings
  }
}
