package com.wavesenterprise.api.http.consensus

import akka.http.scaladsl.server.Route
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.ValidInt._
import com.wavesenterprise.api.http._
import com.wavesenterprise.api.http.auth.ApiProtectionLevel.ApiKeyProtection
import com.wavesenterprise.api.http.auth.AuthRole.Administrator
import com.wavesenterprise.consensus.{GeneratingBalanceProvider, MinerBanlistEntry}
import com.wavesenterprise.settings.{ApiSettings, ConsensusSettings, FunctionalitySettings}
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.utils.Time
import monix.execution.schedulers.SchedulerService
import play.api.libs.json.{Format, Json}

class ConsensusApiRoute(val settings: ApiSettings,
                        val time: Time,
                        blockchain: Blockchain,
                        fs: FunctionalitySettings,
                        consensusSettings: ConsensusSettings,
                        val nodeOwner: Address,
                        val scheduler: SchedulerService)
    extends ApiRoute
    with CommonApiFunctions {

  override val route: Route =
    pathPrefix("consensus") {
      consensusSettingsRoute ~
        withAuth() {
          algo ~ basetarget ~ baseTargetId ~ generationSignature ~ generationSignatureId ~
            generatingBalance ~ minersAtHeight ~ minersAtTimestamp ~ bannedMiners ~ minersWarningsAndBans
        }
    }

  import ConsensusApiRoute._

  /**
    * GET /consensus/generatingbalance/{address}
    *
    * "Account's generating balance(the same as balance atm)
    **/
  def generatingBalance: Route = (path("generatingbalance" / Segment) & get) { addressStr =>
    withExecutionContext(scheduler) {
      complete {
        Address
          .fromString(addressStr)
          .map { address =>
            val balance = GeneratingBalanceProvider.balance(blockchain, fs, blockchain.height, address)
            Json.obj("address" -> address.address, "balance" -> balance)
          }
          .left
          .map(ApiError.fromCryptoError)
      }
    }
  }

  /**
    * GET /consensus/generationsignature/{blockSignature}
    *
    * Generation signature of a block with specified id
    **/
  def generationSignatureId: Route = (path("generationsignature" / Segment) & get) { encodedSignature =>
    withExecutionContext(scheduler) {
      withBlock(blockchain, encodedSignature) { block =>
        complete {
          block.consensusData
            .asPoSMaybe()
            .fold(err => ApiError.fromValidationError(err), posData => GenerationSignatureResult(posData.generationSignature.base58))
        }
      }
    }
  }

  /**
    * GET /consensus/generationsignature
    *
    * Generation signature of a last block
    **/
  def generationSignature: Route = (path("generationsignature") & get) {
    withExecutionContext(scheduler) {
      complete {
        blockchain.lastBlock.get.consensusData
          .asPoSMaybe()
          .fold(err => ApiError.fromValidationError(err), posData => GenerationSignatureResult(posData.generationSignature.base58))
      }
    }
  }

  /**
    * GET /consensus/basetarget/{blockSignature}
    *
    * Base target of a block with specified signature
    **/
  def baseTargetId: Route = (path("basetarget" / Segment) & get) { encodedSignature =>
    withExecutionContext(scheduler) {
      withBlock(blockchain, encodedSignature) { block =>
        complete {
          block.consensusData
            .asPoSMaybe()
            .fold(
              err => ApiError.fromValidationError(err),
              posData => BaseTargetResult(posData.baseTarget, block.blockScore.value.toString())
            )
        }
      }
    }
  }

  /**
    * GET /consensus/basetarget
    *
    * Base target of a last block
    **/
  def basetarget: Route = (path("basetarget") & get) {
    withExecutionContext(scheduler) {
      complete {
        blockchain.lastBlock.get.consensusData
          .asPoSMaybe()
          .fold(
            err => ApiError.fromValidationError(err),
            posData => BaseTargetResult(posData.baseTarget, blockchain.score.toString())
          )
      }
    }
  }

  /**
    * GET /consensus/algo
    *
    * Shows which consensus algo are being used
    **/
  def algo: Route = (path("algo") & get) {
    complete(
      consensusSettings match {
        case _: ConsensusSettings.PoASettings =>
          Json.obj("consensusAlgo" -> "Proof-of-Authority (PoA)")
        case ConsensusSettings.PoSSettings =>
          Json.obj("consensusAlgo" -> "Fair Proof-of-Stake (FairPoS)")
        case _: ConsensusSettings.CftSettings =>
          Json.obj("consensusAlgo" -> "Crash Fault Tolerance (CFT)")
      }
    )
  }

  /**
    * GET /consensus/settings
    *
    * Shows active consensus settings from node config
    **/
  def consensusSettingsRoute: Route = (path("settings") & get & withAuth(ApiKeyProtection, Administrator)) {
    complete {
      consensusSettings match {
        case ConsensusSettings.PoASettings(roundDuration, syncDuration, banDurationBlocks, warningsForBan, maxBansPercentage) =>
          Json.obj(
            "consensusAlgo"     -> "Proof-of-Authority (PoA)",
            "roundDuration"     -> s"$roundDuration",
            "syncDuration"      -> s"$syncDuration",
            "banDurationBlocks" -> banDurationBlocks,
            "warningsForBan"    -> warningsForBan,
            "maxBansPercentage" -> maxBansPercentage
          )
        case _ =>
          Json.obj("consensusAlgo" -> "Fair Proof-of-Stake (FairPoS)")
      }
    }
  }

  /**
    * GET /consensus/minersAtHeight/{height}
    *
    * Retrieves list of miners for timestamp of a block at given height or
    * error with negative height
    **/
  def minersAtHeight: Route = (path("minersAtHeight" / Segment) & get)(heightStr => {
    PositiveInt(heightStr).processRoute {
      height =>
        withExecutionContext(scheduler) {
          complete {
            for {
              blockHeader <- blockchain.blockHeaderAt(height)
              requestedTimestamp = blockHeader.timestamp
              minerAddresses     = blockchain.miners.currentMinersSet(requestedTimestamp).map(_.toString)
            } yield MinersAtHeight(minerAddresses.toSeq, height)
          }
        }
    }
  })

  /**
    * GET /consensus/miners/{timestamp}
    *
    * Retrieves list of miners at given timestamp
    **/
  def minersAtTimestamp: Route = (path("miners" / LongNumber) & get) { atTimestamp =>
    withExecutionContext(scheduler) {
      val minerAddresses = blockchain.miners.currentMinersSet(atTimestamp).map(_.toString)
      complete {
        MinersAtTimestamp(minerAddresses.toSeq, atTimestamp)
      }
    }
  }

  /**
    * GET /consensus/bannedMiners/{height}
    *
    * Retrieves list of miner addresses, that are banned at specified height
    **/
  def bannedMiners: Route = (path("bannedMiners" / Segment) & get) { heightStr =>
    PositiveInt(heightStr).processRoute { height =>
      withExecutionContext(scheduler) {
        complete {
          val miners = blockchain.bannedMiners(height).map(_.toString)
          BannedMiners(miners, height)
        }
      }
    }
  }

  // private api, used only by tests, so don't have documentation
  def minersWarningsAndBans: Route = (path("minersWarningsAndBans") & get) {
    complete {
      val value =
        blockchain.warningsAndBans
          .map(addressWithBanlist => banlistEntryToInt(addressWithBanlist._1, addressWithBanlist._2))
          .toSeq
      MinersWarningsAndBans(value)
    }
  }

  private def banlistEntryToInt(address: Address, minersBanlistEntry: Seq[MinerBanlistEntry]): MinerBanlist = {
    val (internalWarnings, internalBans) = minersBanlistEntry.partition {
      case MinerBanlistEntry.Warning(_)   => true
      case MinerBanlistEntry.Ban(_, _, _) => false
    }

    val warnings = internalWarnings.collect { case MinerBanlistEntry.Warning(timestamp) => ConsensusApiRoute.Warning(timestamp) }
    val bans     = internalBans.collect { case MinerBanlistEntry.Ban(timestamp, beginHeight, _) => ConsensusApiRoute.Ban(timestamp, beginHeight) }

    MinerBanlist(address.toString, bans, warnings)
  }

}

object ConsensusApiRoute {

  case class MinersAtTimestamp(miners: Seq[String], timestamp: Long)

  case class MinersAtHeight(miners: Seq[String], height: Int)

  case class BannedMiners(bannedMiners: Seq[String], height: Int)

  case class BaseTargetResult(baseTarget: Long, score: String)

  case class GenerationSignatureResult(generationSignature: String)

  case class Ban(timestamp: Long, beginHeight: Int)

  case class Warning(timestamp: Long)

  case class MinerBanlist(address: String, bans: Seq[Ban], warnings: Seq[Warning])

  case class MinersWarningsAndBans(warningsAndBans: Seq[MinerBanlist])

  implicit val minersAtTimestampFormat: Format[MinersAtTimestamp]           = Json.format
  implicit val minersAtHeightFormat: Format[MinersAtHeight]                 = Json.format
  implicit val bannedMinersFormat: Format[BannedMiners]                     = Json.format
  implicit val warningFormat: Format[Warning]                               = Json.format
  implicit val banFormat: Format[Ban]                                       = Json.format
  implicit val minerBanlistFormat: Format[MinerBanlist]                     = Json.format
  implicit val minersWarningsAndBansFormat: Format[MinersWarningsAndBans]   = Json.format
  implicit val baseTargetFormat: Format[BaseTargetResult]                   = Json.format
  implicit val generationSignatureFormat: Format[GenerationSignatureResult] = Json.format
}
