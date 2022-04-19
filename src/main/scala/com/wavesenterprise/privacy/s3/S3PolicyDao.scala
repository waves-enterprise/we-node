package com.wavesenterprise.privacy.s3

import cats.data.EitherT
import cats.implicits._
import com.wavesenterprise.privacy.{PolicyDataHash, PolicyMetaData, PrivacyDataType}
import com.wavesenterprise.privacy.aws.S3Result
import com.wavesenterprise.privacy.s3.multipartUpload.MultiPartUploader
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.utils.ScorexLogging
import monix.eval.Task
import monix.reactive.Observable
import org.reactivestreams.Publisher
import software.amazon.awssdk.core.async.{AsyncRequestBody, AsyncResponseTransformer, SdkPublisher}
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model._

import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import scala.collection.JavaConverters._
import scala.language.implicitConversions

class S3PolicyDao(s3Client: S3AsyncClient, bucket: String) extends ScorexLogging with AutoCloseable {

  private[this] val multipartUploader = MultiPartUploader(s3Client, bucket)

  private implicit def compFutureToTask[T](cf: => CompletableFuture[T]): Task[S3Result[T]] =
    Task.defer(Task.from(cf)).map(Right(_))

  private def catchUnhandled[T](t: => Task[S3Result[T]]): Task[S3Result[T]] =
    t.onErrorRecover {
      case e =>
        log.error("S3 error: ", e)
        Left(UnmappedError)
    }

  def createBucket: Task[S3Result[CreateBucketResponse]] = catchUnhandled {
    s3Client
      .createBucket(CreateBucketRequest.builder().bucket(bucket).build())
      .onErrorRecover {
        case _: BucketAlreadyExistsException | _: BucketAlreadyOwnedByYouException => Right(CreateBucketResponse.builder().build())
      }
  }

  def policyItemType(policyId: String, dataHash: String): Task[Either[S3Error, Option[PrivacyDataType]]] = {
    def extractItemType(info: HeadObjectResponse): PrivacyDataType = {
      val isLarge = info
        .metadata()
        .asScala
        .get("isLarge")
        .exists(_.toBoolean)

      if (isLarge) {
        PrivacyDataType.Large
      } else {
        PrivacyDataType.Default
      }
    }

    s3Client
      .headObject(HeadObjectRequest.builder().bucket(bucket).key(policyId + dataHash).build())
      .map(value => value.map(info => Some(extractItemType(info))))
      .onErrorRecover {
        case _: NoSuchKeyException                 => Right(None)
        case s: S3Exception if s.statusCode == 404 => Right(None)
      }
  }

  def exists(policyId: String, dataHash: String): Task[S3Result[Boolean]] = catchUnhandled {
    s3Client
      .headObject(HeadObjectRequest.builder().bucket(bucket).key(policyId + dataHash).build())
      .map(_.map(_ => true))
      .onErrorRecover {
        case _: NoSuchKeyException                 => Right(false)
        case s: S3Exception if s.statusCode == 404 => Right(false)
      }
  }

  def findPolicyMetaData(policyId: String, dataHash: String): Task[S3Result[Option[PolicyMetaData]]] = catchUnhandled {
    s3Client
      .headObject(HeadObjectRequest.builder().bucket(bucket).key(policyId + dataHash).build())
      .map(_.flatMap { headObj =>
        PolicyMetaData
          .fromMap(headObj.metadata().asScala.toMap.map { case (k, v) => k.toLowerCase -> v })
          .toEither
          .leftMap(e => {
            log.error("PolicyMetaData parse error: ", e)
            ParseError(e.getLocalizedMessage)
          })
          .map(Some(_))
      })
      .onErrorRecover {
        case _: NoSuchKeyException => Right(None)
      }
  }

  def findPolicyMetaData(policyIdsWithHashes: Map[String, Set[String]]): Task[S3Result[Seq[PolicyMetaData]]] = catchUnhandled {
    policyIdsWithHashes.toList
      .flatMap {
        case (id, hashes) => hashes.map((id, _))
      }
      .map {
        case (id, hash) => findPolicyMetaData(id, hash)
      }
      .sequence
      .map(_.sequence.map(_.flatten))
  }

  def findData(policyId: String, dataHash: String): Task[S3Result[Option[ByteStr]]] = catchUnhandled {
    s3Client
      .getObject(
        GetObjectRequest.builder().bucket(bucket).key(policyId + dataHash).build(),
        AsyncResponseTransformer.toBytes[GetObjectResponse]
      )
      .map(_.map(data => Some(ByteStr(data.asByteArray))))
      .onErrorRecover {
        case _: NoSuchKeyException                 => Right(None)
        case s: S3Exception if s.statusCode == 404 => Right(None)
      }
  }

  def findData(policyIdHashes: Map[String, String]): Task[S3Result[List[ByteStr]]] = catchUnhandled {
    policyIdHashes
      .map { case (policyId, hash) => findData(policyId, hash) }
      .toList
      .sequence
      .map(_.sequence.map(_.flatten))
  }

  def findDataAsPublisher(policyId: String, dataHash: String): Task[S3Result[Option[Publisher[ByteBuffer]]]] = catchUnhandled {
    s3Client
      .getObject(
        GetObjectRequest.builder().bucket(bucket).key(policyId + dataHash).build(),
        buildPublisherTransformer()
      )
      .map(_.map(pub => Some(pub)))
      .onErrorRecover {
        case _: NoSuchKeyException                 => Right(None)
        case s: S3Exception if s.statusCode == 404 => Right(None)
      }
  }

  def save(policyData: ByteStr, policyMeta: PolicyMetaData): Task[S3Result[S3Response]] =
    catchUnhandled[PutObjectResponse] {
      checkHash(policyData, policyMeta).flatMapF { _ =>
        s3Client
          .putObject(
            PutObjectRequest.builder().bucket(bucket).key(policyMeta.policyId + policyMeta.hash).metadata(policyMeta.toMap.asJava).build(),
            AsyncRequestBody.fromBytes(policyData.arr)
          )
      }.value
    }

  private def checkHash(policyData: ByteStr, policyMeta: PolicyMetaData): EitherT[Task, InvalidHash, Unit] =
    EitherT {
      Task {
        val actualHash = PolicyDataHash.fromDataBytes(policyData.arr).toString
        Either.cond(actualHash == policyMeta.hash, (), InvalidHash(actualHash, policyMeta.hash))
      }
    }

  def saveDataViaObservable(metaData: PolicyMetaData, obs: Observable[Array[Byte]]): Task[S3Result[S3Response]] =
    catchUnhandled[CompleteMultipartUploadResponse] {
      multipartUploader.upload(metaData, obs)
    }

  def saveAll(dataMetas: Seq[(ByteStr, PolicyMetaData)]): Task[S3Result[S3Response]] = catchUnhandled {
    dataMetas
      .map { case (data, meta) => save(data, meta) }
      .toList
      .sequence
      .map(_.foldLeft[S3Result[S3Response]](Right(PutObjectResponse.builder().build())) {
        case (accumulator, currentMaybe) =>
          for {
            _       <- accumulator
            current <- currentMaybe
          } yield current
      })
  }

  def delete(key: String): Task[S3Result[S3Response]] = catchUnhandled[DeleteObjectResponse] {
    s3Client
      .deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build())
  }

  def clearBucket: Task[S3Result[S3Response]] =
    (for {
      bucketObjects <- EitherT(catchUnhandled(s3Client.listObjects(ListObjectsRequest.builder().bucket(bucket).build())))
      response <- EitherT {
        Task
          .sequence(bucketObjects.contents().asScala.toList.map(i => delete(i.key())))
          .map(_.foldLeft[S3Result[S3Response]](Right(DeleteBucketResponse.builder().build())) {
            case (accumulator, currentMaybe) =>
              for {
                _       <- accumulator
                current <- currentMaybe
              } yield current
          })
      }
    } yield response).value

  def healthCheck: Task[S3Result[Unit]] =
    catchUnhandled[HeadBucketResponse] {
      s3Client.headBucket(HeadBucketRequest.builder().bucket(bucket).build())
    }.map(_.flatMap { response =>
      Either.cond(
        response.sdkHttpResponse().isSuccessful,
        (),
        BucketError(s"HeadBucketResponse status code is '${response.sdkHttpResponse().statusCode()}' instead of 200")
      )
    })

  override def close(): Unit = s3Client.close()

  private def buildPublisherTransformer(): AsyncResponseTransformer[GetObjectResponse, Publisher[ByteBuffer]] =
    new AsyncResponseTransformer[GetObjectResponse, Publisher[ByteBuffer]]() {

      @volatile
      private[this] var future: CompletableFuture[Publisher[ByteBuffer]] = _

      override def prepare(): CompletableFuture[Publisher[ByteBuffer]] = {
        future = new CompletableFuture[Publisher[ByteBuffer]]
        future
      }

      override def onResponse(response: GetObjectResponse): Unit       = ()
      override def exceptionOccurred(throwable: Throwable): Unit       = future.completeExceptionally(throwable)
      override def onStream(publisher: SdkPublisher[ByteBuffer]): Unit = future.complete(publisher)
    }
}
