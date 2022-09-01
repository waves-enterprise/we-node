package com.wavesenterprise.privacy

import akka.actor.ActorSystem
import cats.data.EitherT
import com.wavesenterprise.api.http.{ApiError, PolicyItem}
import com.wavesenterprise.privacy.db._
import com.wavesenterprise.privacy.s3.{PolicyS3StorageService, S3PolicyDao}
import com.wavesenterprise.settings.WESettings
import com.wavesenterprise.settings.privacy.{DisabledPrivacyStorage, PostgresPrivacyStorageSettings, S3PrivacyStorageSettings}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.utils.{ScorexLogging, Time}
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.subjects.ConcurrentSubject
import monix.reactive.{Observable, OverflowStrategy}
import slick.jdbc.JdbcBackend
import slick.jdbc.JdbcBackend.Database
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.client.config.{ClientAsyncConfiguration, SdkAdvancedAsyncClientOption}
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.s3.{S3AsyncClient, S3Configuration}

import java.net.URI
import scala.concurrent.Await
import scala.concurrent.duration._

trait PolicyStorage extends AutoCloseable with ScorexLogging {

  private val internalLastPolicyEvent: ConcurrentSubject[PrivacyEvent, PrivacyEvent] =
    ConcurrentSubject.publish[PrivacyEvent](monix.execution.Scheduler.global)
  val lastPolicyEvent: Observable[PrivacyEvent] = internalLastPolicyEvent.asyncBoundary(OverflowStrategy.Default)

  def policyItemType(policyId: String, policyItemHash: String): Task[Either[ApiError, Option[PrivacyDataType]]]
  def policyItemExists(policyId: String, policyItemHash: String): Task[Either[ApiError, Boolean]]
  def policyItemData(policyId: String, policyItemHash: String): Task[Either[ApiError, Option[ByteStr]]]
  def policyItemMeta(policyId: String, policyItemHash: String): Task[Either[ApiError, Option[PolicyMetaData]]]
  def policyItemsMetas(policyIdsWithHashes: Map[String, Set[String]]): Task[Either[ApiError, Seq[PolicyMetaData]]]

  final def savePolicyItem(sendDataReq: PolicyItem)(implicit s: Scheduler): Task[Either[ApiError, Unit]] = {
    (for {
      privacyEvent <- EitherT.right[ApiError](Task(DataAcquiredEvent(sendDataReq.policyId, sendDataReq.hash, time.getTimestamp())))
      _            <- EitherT(internalSavePolicyItem(sendDataReq))
      _            <- EitherT.right[ApiError](Task.deferFuture(internalLastPolicyEvent.onNext(privacyEvent)))
    } yield ()).value
  }

  final def savePolicyDataWithMeta(policyData: ByteStr, policyMeta: PolicyMetaData)(implicit s: Scheduler): Task[Either[ApiError, Unit]] = {
    (for {
      privacyEvent <- EitherT.right[ApiError](Task(DataAcquiredEvent(policyMeta.policyId, policyMeta.hash, time.getTimestamp())))
      _            <- EitherT(internalSavePolicyDataWithMeta(Left(policyData), policyMeta))
      _            <- EitherT.right[ApiError](Task.deferFuture(internalLastPolicyEvent.onNext(privacyEvent)))
    } yield ()).value
  }

  def savePolicyDataWithMeta(
      policyData: Either[ByteStr, Observable[Byte]],
      policyMeta: PolicyMetaData
  )(implicit s: Scheduler): Task[Either[ApiError, Unit]] = {
    (for {
      privacyEvent <- EitherT.right[ApiError](Task(DataAcquiredEvent(policyMeta.policyId, policyMeta.hash, time.getTimestamp())))
      _            <- EitherT(internalSavePolicyDataWithMeta(policyData, policyMeta))
      _            <- EitherT.right[ApiError](Task.deferFuture(internalLastPolicyEvent.onNext(privacyEvent)))
    } yield ()).value
  }

  def policyItemDataStream(
      policyId: String,
      policyItemHash: String
  )(implicit s: Scheduler): Task[Either[ApiError, Option[Observable[Array[Byte]]]]]

  def close(): Unit = {
    internalLastPolicyEvent.onComplete()
  }

  protected def internalSavePolicyItem(sendDataReq: PolicyItem)(implicit s: Scheduler): Task[Either[ApiError, Unit]]

  protected def internalSavePolicyDataWithMeta(
      policyData: Either[ByteStr, Observable[Byte]],
      policyMeta: PolicyMetaData
  )(implicit s: Scheduler): Task[Either[ApiError, Unit]]

  def healthCheck: Task[Either[ApiError, Unit]]
  def time: Time
}

class EmptyPolicyStorage(val time: Time) extends PolicyStorage {
  override def policyItemData(
      policyId: String,
      policyItemHash: String
  ): Task[Either[ApiError, Option[ByteStr]]] =
    Task.pure(Right(None))

  override def policyItemMeta(
      policyId: String,
      policyItemHash: String
  ): Task[Either[ApiError, Option[PolicyMetaData]]] =
    Task.pure(Right(None))

  override def policyItemsMetas(
      policyIdsWithHashes: Map[String, Set[String]]
  ): Task[Either[ApiError, Seq[PolicyMetaData]]] =
    Task.pure(Right(Seq.empty))

  override def internalSavePolicyItem(sendDataReq: PolicyItem)(implicit s: Scheduler): Task[Either[ApiError, Unit]] =
    Task.pure(Right({}))

  override def policyItemExists(
      policyId: String,
      policyItemHash: String
  ): Task[Either[ApiError, Boolean]] =
    Task.pure(Right(false))

  override def healthCheck: Task[Either[ApiError, Unit]] = Task.pure(Right({}))

  override def policyItemType(policyId: String, policyItemHash: String): Task[Either[ApiError, Option[PrivacyDataType]]] = Task.pure(Right(None))

  override protected def internalSavePolicyDataWithMeta(
      policyData: Either[ByteStr, Observable[Byte]],
      policyMeta: PolicyMetaData
  )(implicit s: Scheduler): Task[Either[ApiError, Unit]] =
    Task.pure(Right({}))

  override def policyItemDataStream(
      policyId: String,
      policyItemHash: String
  )(implicit s: Scheduler): Task[Either[ApiError, Option[Observable[Array[Byte]]]]] =
    Task.pure(Right(None))
}

object PolicyStorage extends ScorexLogging {
  def create(
      settings: WESettings,
      time: Time,
      shutdownFunction: () => Unit
  )(implicit actorSystem: ActorSystem): PolicyStorage = settings.privacy.storage match {
    case DisabledPrivacyStorage =>
      log.trace("Policy storage disabled")
      new EmptyPolicyStorage(time)
    case pss: PostgresPrivacyStorageSettings =>
      val jdbcBackend: JdbcBackend.Database = Database.forConfig(path = "", config = pss.jdbcConfig)

      log.trace("Found privacy storage settings, starting SchemaMigration")
      SchemaMigration.migrate(
        slickDatasource = jdbcBackend.source,
        migrationDir = pss.migrationDir,
        schema = pss.schema,
        baseline = true,
        shutdown = {
          shutdownFunction(); sys.exit(1)
        }
      )

      val policyMetaDataDBIO = new PolicyMetaDataDBIO(pss.jdbcProfile)
      val policyDataDBIO     = new PolicyDataDBIO(pss.jdbcProfile)

      val policyDao = new PostgresPolicyDao(
        policyDataDBIO,
        policyMetaDataDBIO,
        jdbcBackend,
        pss.jdbcProfile
      )

      new PolicyPostgresStorageService(policyDao, time, pss)(actorSystem)
    case pss: S3PrivacyStorageSettings =>
      log.trace("Found privacy storage settings, creating S3 Client")
      val httpClient = NettyNioAsyncHttpClient
        .builder()
        .connectionAcquisitionTimeout(pss.connectionAcquisitionTimeout)
        .connectionTimeout(pss.connectionTimeout)
        .maxConcurrency(pss.maxConcurrency)
        .readTimeout(pss.readTimeout)
        .build()

      val asyncConfig = ClientAsyncConfiguration
        .builder()
        .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, monix.execution.Scheduler.global)
        .build()

      val s3Client = S3AsyncClient
        .builder()
        .asyncConfiguration(asyncConfig)
        .httpClient(httpClient)
        .endpointOverride(URI.create(pss.url))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(pss.accessKeyId, pss.secretAccessKey)))
        .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(pss.pathStyleAccessEnabled).build())
        .region(pss.region)
        .build()

      val dao            = new S3PolicyDao(s3Client, pss.bucket)
      val bucketCreation = dao.createBucket.runToFuture(monix.execution.Scheduler.global)
      Await.result(bucketCreation, 10 seconds)
      new PolicyS3StorageService(dao, time, pss)
  }
}
