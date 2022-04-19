package com.wavesenterprise.privacy.s3.multipartUpload

import cats.data.EitherT
import com.wavesenterprise.crypto.util.Sha256Hash
import com.wavesenterprise.privacy.PolicyMetaData
import com.wavesenterprise.privacy.s3.{InvalidHash, S3Error}
import com.wavesenterprise.utils.Base58
import monix.eval.Task
import monix.reactive.{Consumer, Observable}
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model._

import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

trait RequestBuilder {

  def createMultiPartUploadRequest(bucket: String, key: String, metaData: Map[String, String]): CreateMultipartUploadRequest =
    CreateMultipartUploadRequest.builder().bucket(bucket).key(key).metadata(metaData.asJava).build()

  def uploadPartRequest(bucket: String, key: String, uploadId: String, partNumber: Int): UploadPartRequest =
    UploadPartRequest.builder().bucket(bucket).key(key).uploadId(uploadId).partNumber(partNumber).build()

  val completedPart: (String, Int) => CompletedPart = (eTag: String, partNumber: Int) =>
    CompletedPart.builder().eTag(eTag).partNumber(partNumber).build()

  def completedMultipartUpload(parts: CompletedPart*): CompletedMultipartUpload =
    CompletedMultipartUpload.builder().parts(parts: _*).build()

  def completeMultiPartUploadReq(bucket: String,
                                 key: String,
                                 uploadId: String,
                                 completedMPU: CompletedMultipartUpload): CompleteMultipartUploadRequest =
    CompleteMultipartUploadRequest
      .builder()
      .bucket(bucket)
      .key(key)
      .uploadId(uploadId)
      .multipartUpload(completedMPU)
      .build()

  def abortMultiPartUploadReq(bucket: String, key: String, uploadId: String): AbortMultipartUploadRequest =
    AbortMultipartUploadRequest.builder().bucket(bucket).key(key).uploadId(uploadId).build()

  def listUploadParts(bucket: String, key: String, uploadId: String): ListPartsRequest =
    ListPartsRequest.builder().bucket(bucket).key(key).uploadId(uploadId).build()
}

final case class MultiPartUploader(
    asyncS3Client: S3AsyncClient,
    bucket: String,
) extends RequestBuilder {

  private implicit def compFutureToTask[T](cf: => CompletableFuture[T]): Task[T] =
    Task.defer(Task.from(cf))

  def upload(metaData: PolicyMetaData, content: Observable[Array[Byte]]): Task[Either[S3Error, CompleteMultipartUploadResponse]] = {
    val key = metaData.policyId + metaData.hash
    (for {
      uploadId        <- EitherT.right(createMultiPartUpload(bucket, key, metaData.toMap))
      hashWithResults <- EitherT.right(uploadParts(content, key, uploadId))
      (actualHash, partResults) = hashWithResults
      _    <- EitherT(Task(validateHashes(Base58.encode(actualHash), metaData.hash)))
      resp <- EitherT.right[S3Error](completeUpload(key, partResults, uploadId))
    } yield resp).value
  }

  private def createMultiPartUpload(bucket: String, key: String, metaData: Map[String, String]): Task[String] = {
    val createMultipartUploadReq = createMultiPartUploadRequest(bucket, key, metaData)
    asyncS3Client
      .createMultipartUpload(createMultipartUploadReq)
      .map(_.uploadId())
  }

  private def validateHashes(actual: String, expected: String): Either[S3Error, Unit] = {
    Either.cond(actual == expected, (), InvalidHash(actual, expected))
  }

  private def uploadPart(
      content: Array[Byte],
      key: String,
      uploadId: String,
      partNumber: Int
  ): Task[(String, Int)] = {
    asyncS3Client
      .uploadPart(
        uploadPartRequest(bucket, key, uploadId, partNumber),
        AsyncRequestBody.fromBytes(content)
      )
      .map(resp => (resp.eTag(), partNumber))
      .onErrorRecoverWith {
        case NonFatal(ex) => abortUntilAllRemoved(bucket, key, uploadId) *> Task.raiseError(ex)
      }

  }

  private def uploadParts(content: Observable[Array[Byte]], key: String, uploadId: String): Task[(Array[Byte], List[(String, Int)])] = {
    val consumer = Consumer.foldLeftTask[(Sha256Hash, List[(String, Int)]), (Array[Byte], Long)]((Sha256Hash(), List.empty)) {
      case ((hash, acc), (chunk, index)) =>
        uploadPart(chunk, key, uploadId, index.toInt).map {
          case (str, i) => hash.update(chunk) -> (acc :+ (str -> i))
        }
    }

    content.zipWithIndex.consumeWith(consumer).map {
      case (hash, acc) => hash.result() -> acc
    }
  }

  private def completeUpload(
      key: String,
      eTagPartNumbers: List[(String, Int)],
      uploadId: String
  ): Task[CompleteMultipartUploadResponse] = {
    val completedParts          = eTagPartNumbers.map(completedPart.tupled)
    val completeMultiPartUpload = completedMultipartUpload(completedParts: _*)
    val completeMultiPartUploadRequest = completeMultiPartUploadReq(
      bucket,
      key,
      uploadId,
      completeMultiPartUpload
    )
    asyncS3Client
      .completeMultipartUpload(completeMultiPartUploadRequest)
      .onErrorRecoverWith {
        case NonFatal(ex) => abortUntilAllRemoved(bucket, key, uploadId) *> Task.raiseError(ex)
      }
  }

  private def abortMultiPartUpload(bucket: String, key: String, uploadId: String): Task[AbortMultipartUploadResponse] =
    asyncS3Client.abortMultipartUpload(abortMultiPartUploadReq(bucket, key, uploadId))

  private def abortUntilAllRemoved(
      bucket: String,
      key: String,
      uploadId: String
  ): Task[Unit] =
    abortMultiPartUpload(bucket, key, uploadId)
      .flatMap(_ => listParts(bucket, key, uploadId))
      .restartUntil(_.hasParts())
      .void
      .onErrorRecoverWith {
        case _: NoSuchUploadException => Task.unit
      }

  private def listParts(
      bucket: String,
      key: String,
      uploadId: String
  ): Task[ListPartsResponse] =
    asyncS3Client.listParts(listUploadParts(bucket, key, uploadId))
}
