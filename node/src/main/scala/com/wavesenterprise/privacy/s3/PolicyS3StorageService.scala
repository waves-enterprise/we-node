package com.wavesenterprise.privacy.s3

import cats.data.EitherT
import cats.implicits._
import com.wavesenterprise.api.http.ApiError.CustomValidationError
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.api.http.privacy.PolicyItem
import com.wavesenterprise.privacy.{PolicyMetaData, PolicyStorage, PrivacyDataType}
import com.wavesenterprise.settings.privacy.S3PrivacyStorageSettings
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.utils.{ScorexLogging, Time}
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import scala.util.{Failure, Success}

class PolicyS3StorageService(dao: S3PolicyDao, val time: Time, settings: S3PrivacyStorageSettings) extends PolicyStorage with ScorexLogging {

  override def policyItemType(policyId: String, policyItemHash: String): Task[Either[ApiError, Option[PrivacyDataType]]] =
    dao
      .policyItemType(policyId, policyItemHash)
      .map(_.leftMap(ApiError.fromS3Error))

  override def policyItemExists(policyId: String, policyItemHash: String): Task[Either[ApiError, Boolean]] = {
    dao
      .exists(policyId, policyItemHash)
      .map(_.leftMap(ApiError.fromS3Error))
  }

  override def policyItemData(policyId: String, policyItemHash: String): Task[Either[ApiError, Option[ByteStr]]] = {
    dao
      .findData(policyId, policyItemHash)
      .map { s3Res =>
        s3Res.leftMap(ApiError.fromS3Error)
      }
  }

  override def policyItemMeta(policyId: String, policyItemHash: String): Task[Either[ApiError, Option[PolicyMetaData]]] =
    dao
      .findPolicyMetaData(policyId, policyItemHash)
      .map(_.leftMap(ApiError.fromS3Error))

  override def policyItemsMetas(policyIdsWithHashes: Map[String, Set[String]]): Task[Either[ApiError, Seq[PolicyMetaData]]] =
    dao
      .findPolicyMetaData(policyIdsWithHashes)
      .map(_.leftMap(ApiError.fromS3Error))

  override def internalSavePolicyItem(policyItem: PolicyItem)(implicit s: Scheduler): Task[Either[ApiError, Unit]] = {
    val sendDataMetaInfo = s"policyId: '${policyItem.policyId}', policyHash: '${policyItem.hash}'"
    log.debug(s"Decoding policyItem for '$sendDataMetaInfo' ...")
    val data = policyItem.data match {
      case Right(d) => Success(d)
      case Left(d)  => ByteStr.decodeBase64(d)
    }
    data match {
      case Failure(ex) =>
        log.error(s"Failed to decode policy data for $sendDataMetaInfo: ${ex.getMessage}")
        Task.pure(Left(CustomValidationError("Failed to decode policy data from Base64")))
      case Success(policyData) =>
        log.debug(s"Successfully decoded. Saving policyItem for $sendDataMetaInfo ...")
        val policyMeta = PolicyMetaData.fromPolicyItem(policyItem, PrivacyDataType.Default)
        dao
          .save(policyData, policyMeta)
          .map(_.map(_ => {}).leftMap(ApiError.fromS3Error))
    }
  }

  override def healthCheck: Task[Either[ApiError, Unit]] = dao.healthCheck.map(_.leftMap(ApiError.fromS3Error))

  override def close(): Unit = {
    dao.close()
    super.close()
  }

  override def internalSavePolicyDataWithMeta(
      policyData: Either[ByteStr, Observable[Byte]],
      policyMeta: PolicyMetaData
  )(implicit s: Scheduler): Task[Either[ApiError, Unit]] = {
    policyData match {
      case Left(bytes) => dao.save(bytes, policyMeta).map(_.map(_ => {}).leftMap(ApiError.fromS3Error))
      case Right(dataStream) =>
        val chunkedStream = dataStream.bufferTumbling(settings.uploadChunkSize.toBytes.toInt).map(_.toArray)
        dao
          .saveDataViaObservable(policyMeta, chunkedStream)
          .map { dbResult =>
            dbResult.leftMap(ApiError.fromS3Error).void
          }
    }

  }

  override def policyItemDataStream(
      policyId: String,
      policyItemHash: String
  )(implicit s: Scheduler): Task[Either[ApiError, Option[Observable[Array[Byte]]]]] =
    EitherT(dao.findDataAsPublisher(policyId, policyItemHash))
      .leftMap(ApiError.fromS3Error)
      .map(_.map { publisher =>
        Observable
          .fromReactivePublisher(publisher)
          .map(bb => bb.array())
      })
      .value
}
