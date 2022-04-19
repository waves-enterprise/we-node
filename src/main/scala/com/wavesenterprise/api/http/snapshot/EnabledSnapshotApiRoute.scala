package com.wavesenterprise.api.http.snapshot

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.server.Route
import cats.data.{EitherT, OptionT}
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.api.http.ApiError.{SnapshotAlreadySwapped, SnapshotGenesisNotFound, SnapshotNotVerified}
import com.wavesenterprise.block.Block
import com.wavesenterprise.database.snapshot.{SnapshotGenesis, SnapshotStatusHolder, Swapped, Verified}
import com.wavesenterprise.settings._
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.utils.Time
import monix.eval.Task
import monix.execution.Scheduler
import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._
import play.api.libs.json.Writes._
import play.api.libs.json.{Format, JsPath, JsResult, JsValue, Json, OFormat, _}

import scala.concurrent.duration.FiniteDuration

class EnabledSnapshotApiRoute(statusHolder: SnapshotStatusHolder,
                              snapshotGenesis: Task[Option[Block]],
                              snapshotSwapTask: Boolean => Task[Either[ApiError, Unit]],
                              val settings: ApiSettings,
                              val time: Time,
                              val nodeOwner: Address,
                              freezeApp: () => Unit)(implicit scheduler: Scheduler)
    extends SnapshotApiRoute {

  import EnabledSnapshotApiRoute._

  /**
    * GET /snapshot/status
    *
    * Return node's consensual snapshot status
    */
  override def statusRoute: Route = complete {
    statusHolder.status.toJson
  }

  /**
    * GET /snapshot/genesisConfig
    *
    * Return snapshot's genesis block as genesis-settings for config
    */
  override def genesisConfigRoute: Route = complete {
    EitherT
      .rightT[Task, ApiError](statusHolder.status)
      .ensure(SnapshotAlreadySwapped)(_ != Swapped)
      .ensureOr(status => SnapshotNotVerified(status))(_ == Verified)
      .flatMap { _ =>
        OptionT(snapshotGenesis).toRight[ApiError](SnapshotGenesisNotFound)
      }
      .map { block =>
        GenesisSettingsFormat.writes(SnapshotGenesis.mapToSnapshotBasedSettings(block))
      }
      .value
      .runToFuture(scheduler)
  }

  /**
    * POST /snapshot/swapState
    *
    * Swap data and snapshot in data-directory, optionally backup old state
    */
  override def swapStateRoute(backupOldState: Boolean): Route = complete {
    EitherT
      .rightT[Task, ApiError](statusHolder.status)
      .ensure(SnapshotAlreadySwapped)(_ != Swapped)
      .ensureOr(status => SnapshotNotVerified(status))(_ == Verified)
      .map { _ =>
        freezeApp()
      }
      .flatMap { _ =>
        EitherT(snapshotSwapTask(backupOldState))
      }
      .map { _ =>
        Json.obj("message" -> "Finished swapping old state for snapshot")
      }
      .value
      .runToFuture(scheduler)
  }
}

object EnabledSnapshotApiRoute {

  implicit val GenesisTransactionSettingsFormat: OFormat[GenesisTransactionSettings]       = Json.format
  implicit val NetworkParticipantDescriptionFormat: OFormat[NetworkParticipantDescription] = Json.format

  implicit val FiniteDurationFormat: Format[FiniteDuration] = new Format[FiniteDuration] {
    override def writes(o: FiniteDuration): JsValue = JsNumber(o.toSeconds)
    override def reads(json: JsValue): JsResult[FiniteDuration] = json match {
      case JsNumber(v) => JsSuccess(FiniteDuration(v.toLongExact, TimeUnit.SECONDS))
      case _           => JsError("Expected JsNumber")
    }
  }

  implicit val GenesisSettingsVersionFormat: Format[GenesisSettingsVersion] = new Format[GenesisSettingsVersion] {
    override def writes(o: GenesisSettingsVersion): JsValue = JsNumber(o.value.toInt)
    override def reads(json: JsValue): JsResult[GenesisSettingsVersion] = json match {
      case JsNumber(v) => JsSuccess(GenesisSettingsVersion.withValue(v.toByteExact))
      case _           => JsError("Expected JsNumber")
    }
  }

  // for Writes use GenesisSettingsFormat
  private val PlainGenesisSettingsFormat: Format[PlainGenesisSettings] = {
    ((JsPath \ "block-timestamp").format[Long] and
      (JsPath \ "initial-balance").format[WestAmount] and
      (JsPath \ "genesis-public-key-base-58").format[String] and
      (JsPath \ "signature").formatNullable[ByteStr] and
      (JsPath \ "transactions").format[Seq[GenesisTransactionSettings]] and
      (JsPath \ "network-participants").format[Seq[NetworkParticipantDescription]] and
      (JsPath \ "initial-base-target").format[Long] and
      (JsPath \ "average-block-delay").format[FiniteDuration](FiniteDurationFormat) and
      (JsPath \ "version").format[GenesisSettingsVersion](GenesisSettingsVersionFormat) and
      (JsPath \ "sender-role-enabled").format[Boolean])(PlainGenesisSettings.apply, unlift(PlainGenesisSettings.unapply))
  }

  // for Writes use GenesisSettingsFormat
  private val SnapshotBasedGenesisSettingsFormat: Format[SnapshotBasedGenesisSettings] = {
    ((JsPath \ "block-timestamp").format[Long] and
      (JsPath \ "genesis-public-key-base-58").format[String] and
      (JsPath \ "signature").formatNullable[ByteStr] and
      (JsPath \ "sender-role-enabled").format[Boolean])(SnapshotBasedGenesisSettings.apply, unlift(SnapshotBasedGenesisSettings.unapply))
  }

  implicit val SnapshotBasedGenesisSettingsReads: Reads[SnapshotBasedGenesisSettings] = SnapshotBasedGenesisSettingsFormat

  implicit val GenesisSettingsFormat: Format[GenesisSettings] = Format(
    (JsPath \ "type").readWithDefault[GenesisType](GenesisType.Plain).flatMap {
      case GenesisType.Plain         => PlainGenesisSettingsFormat.reads(_)
      case GenesisType.SnapshotBased => SnapshotBasedGenesisSettingsFormat.reads(_)
      case _                         => Reads(_ => JsError("Unsupported genesis type"))
    },
    genesisSettings =>
      Json.obj("type" -> Json.toJson(genesisSettings.`type`)).deepMerge {
        genesisSettings match {
          case plain: PlainGenesisSettings                 => PlainGenesisSettingsFormat.writes(plain).as[JsObject]
          case snapshotBased: SnapshotBasedGenesisSettings => SnapshotBasedGenesisSettingsFormat.writes(snapshotBased).as[JsObject]
        }
    }
  )

}
