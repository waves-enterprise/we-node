package com.wavesenterprise.privacy.db

import akka.actor.ActorSystem
import cats.implicits._
import com.wavesenterprise.api.http.ApiError.CustomValidationError
import com.wavesenterprise.api.http.{ApiError, PolicyItem}
import com.wavesenterprise.privacy.{PolicyMetaData, PolicyStorage, PrivacyDataType}
import com.wavesenterprise.settings.privacy.PostgresPrivacyStorageSettings
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.utils.{ScorexLogging, Time}
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import scala.util.{Failure, Success}

class PolicyPostgresStorageService(
    policyDao: PostgresPolicyDao,
    val time: Time,
    settings: PostgresPrivacyStorageSettings
)(implicit actorSystem: ActorSystem)
    extends PolicyStorage
    with ScorexLogging {

  override def policyItemType(policyId: String, policyItemHash: String): Task[Either[ApiError, Option[PrivacyDataType]]] =
    policyDao.isLargeObject(policyId, policyItemHash).map { isLargeOrError =>
      isLargeOrError.bimap(
        ApiError.fromDBError,
        _.map {
          case true  => PrivacyDataType.Large
          case false => PrivacyDataType.Default
        }
      )
    }

  override def policyItemData(policyId: String, policyItemHash: String): Task[Either[ApiError, Option[ByteStr]]] = {
    policyDao
      .findItemData(policyId, policyItemHash)
      .map { dbRes =>
        dbRes.leftMap(ApiError.fromDBError)
      }
  }

  override def policyItemExists(policyId: String, policyItemHash: String): Task[Either[ApiError, Boolean]] =
    policyDao.itemMetaDataExists(policyId, policyItemHash).map(_.leftMap(ApiError.fromDBError))

  override def policyItemMeta(policyId: String, policyItemHash: String): Task[Either[ApiError, Option[PolicyMetaData]]] = {
    policyDao
      .policyItemMeta(policyId, policyItemHash)
      .map(_.leftMap(ApiError.fromDBError))
  }

  override def policyItemsMetas(policyIdsWithHashes: Map[String, Set[String]]): Task[Either[ApiError, Seq[PolicyMetaData]]] = {
    val (policyIds, policyDataHashes) = policyIdsWithHashes.toList
      .flatMap {
        case (id, hashes) => hashes.map((id, _))
      }
      .toSet
      .unzip

    policyDao
      .policyItemsMetas(policyIds, policyDataHashes)
      .map(_.leftMap(ApiError.fromDBError))
  }

  override def internalSavePolicyItem(policyItem: PolicyItem)(implicit s: Scheduler): Task[Either[ApiError, Unit]] = {
    val sendDataMetaInfo = s"policyId: '${policyItem.policyId}', policyHash: '${policyItem.hash}'"
    log.debug(s"Saving policyItem for $sendDataMetaInfo ...")
    val data = policyItem.data match {
      case Right(d) => Success(d)
      case Left(d)  => ByteStr.decodeBase64(d)
    }
    data match {
      case Failure(ex) =>
        log.error(s"Failed to decode policy data for $sendDataMetaInfo: ${ex.getMessage}")
        Task.pure(Left(CustomValidationError("Failed to decode policy data from Base64")))
      case Success(policyData) =>
        log.debug(s"Decoded policy data for $sendDataMetaInfo ...")
        val policyMeta = PolicyMetaData.fromPolicyItem(policyItem, PrivacyDataType.Default)

        policyDao
          .insertPolicyData(policyData, policyMeta)
          .map { eitherResult =>
            eitherResult.bimap(
              ApiError.fromDBError,
              _ => log.debug(s"Successfully saved policy data to DB for $sendDataMetaInfo")
            )
          }
    }
  }

  override def healthCheck: Task[Either[ApiError, Unit]] = policyDao.healthCheck.map(_.leftMap(ApiError.fromDBError))

  override def close(): Unit = {
    policyDao.close()
    super.close()
  }

  override def internalSavePolicyDataWithMeta(
      policyData: Either[ByteStr, Observable[Byte]],
      policyMeta: PolicyMetaData
  )(implicit s: Scheduler): Task[Either[ApiError, Unit]] = {
    policyData match {
      case Left(bytes) => policyDao.insertPolicyData(bytes, policyMeta).map(_.map(_ => {}).leftMap(ApiError.fromDBError))
      case Right(dataStream) =>
        val chunkedStream = dataStream.bufferTumbling(settings.uploadChunkSize.toBytes.toInt).map(_.toArray)
        policyDao
          .insertPolicyDataStream(chunkedStream, policyMeta)
          .map { dbResult =>
            dbResult.leftMap(ApiError.fromDBError).void
          }
    }
  }

  override def policyItemDataStream(
      policyId: String,
      policyItemHash: String
  )(implicit s: Scheduler): Task[Either[ApiError, Option[Observable[Array[Byte]]]]] =
    policyDao
      .findItemDataAsSource(policyId, policyItemHash)
      .map(_.leftMap(ApiError.fromDBError))
}
