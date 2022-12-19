package com.wavesenterprise.http

import cats.data.EitherT
import cats.syntax.either._
import com.github.dockerjava.api.DockerClient
import com.wavesenterprise.anchoring.TargetnetAuthTokenProvider
import com.wavesenterprise.api.http.ApiError.{CustomValidationError, HealthCheckError, ServiceIsDisabled}
import com.wavesenterprise.api.http.{ApiError, ExternalStatusResponse, FrozenStatusResponse, NodeStatusResponse, StatusResponse}
import com.wavesenterprise.crypto
import com.wavesenterprise.database.Keys
import com.wavesenterprise.database.rocksdb.RocksDBOperations
import com.wavesenterprise.privacy.{EmptyPolicyStorage, PolicyStorage}
import com.wavesenterprise.settings.{HealthCheckDisabledSettings, HealthCheckEnabledSettings, HealthCheckSettings}
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.utils.ScorexLogging
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.cancelables.SerialCancelable
import org.apache.commons.io.FileUtils

import java.io.File
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Future, TimeoutException}
import scala.util.control.NonFatal

sealed trait HealthChecker extends AutoCloseable with ScorexLogging {
  def blockchain: Blockchain
  def roundDuration: FiniteDuration
  def maxBlockSizeInBytes: Long
  def storage: RocksDBOperations
  def dataDirectory: String
  def policyStorage: PolicyStorage
  def maybeAuthProvider: Option[TargetnetAuthTokenProvider]
  def maybeDockerClient: Option[DockerClient]
  def nodeIsFrozenFlag: AtomicBoolean
  def nodeStatus: Future[Either[ApiError, StatusResponse]] = {
    if (nodeIsFrozenFlag.get()) {
      Future.successful(Right(FrozenStatusResponse))
    } else {
      innerNodeStatus
    }
  }

  def externalStatuses: Future[Either[ApiError, ExternalStatuses]] = {
    if (nodeIsFrozenFlag.get()) {
      Future.successful(Right(ExternalStatuses.frozen))
    } else {
      innerExternalStatuses
    }
  }

  def externalStatusByName(serviceName: String): Future[Either[ApiError, ExternalStatusResponse]] = {
    ExternalStatusResponse.fromStr(serviceName) match {
      case ExternalStatusResponse.Unknown =>
        Future.successful[Either[ApiError, ExternalStatusResponse]](Left(CustomValidationError(s"Unknown service '$serviceName'")))
      case s =>
        externalStatuses.map(_.flatMap { externalStatuses =>
          s match {
            case ExternalStatusResponse.Docker         => externalStatuses.docker
            case ExternalStatusResponse.PrivacyStorage => externalStatuses.privacyStorage
            case ExternalStatusResponse.AnchoringAuth  => externalStatuses.anchoring
            case _                                     => Left(HealthCheckError("Unexpected status response"))
          }
        })(scheduler)
    }
  }

  // for tests
  def fileStorageService: FileStorageService

  protected val scheduler: Scheduler
  protected def innerNodeStatus: Future[Either[ApiError, StatusResponse]]
  protected def innerExternalStatuses: Future[Either[ApiError, ExternalStatuses]]

  private val storageFreeSpaceWarnThreshold: Long  = (0.75 * maxBlockSizeInBytes * (3.days / roundDuration)).toLong
  private val storageFreeSpaceFatalThreshold: Long = 500 * FileUtils.ONE_MB

  protected def healthCheckTask: Task[Either[ApiError, StatusResponse]] = {
    (for {
      startTime       <- EitherT.right[ApiError](Task(System.currentTimeMillis()))
      bcHeight        <- EitherT.right[ApiError](Task(blockchain.height))
      lastUpdated     <- EitherT.right[ApiError](Task(blockchain.blockHeaderAndSize(bcHeight).get._1.timestamp))
      _               <- EitherT(checkRocksDB)
      stateTime       <- EitherT(checkStateStorage)
      cryptoCheckTime <- EitherT(checkCrypto)
    } yield {
      val endTime         = System.currentTimeMillis()
      val stateTook       = stateTime - startTime
      val cryptoCheckTook = cryptoCheckTime - startTime - stateTook

      log.debug {
        s"Health check finished. State: OK (took $stateTook), Crypto: OK (took $cryptoCheckTook ms). Overall took ${endTime - startTime}"
      }

      NodeStatusResponse(bcHeight, bcHeight, lastUpdated, Instant.ofEpochMilli(lastUpdated).toString, endTime)
    }).leftMap { err =>
      log.error(s"Health check failed: '$err'")
      err
    }.value
  }

  protected def externalHealthCheckTask: Task[Either[ApiError, ExternalStatuses]] = {
    Task.parMap3(checkDocker, checkPolicyStorage, checkAnchoringAuthService) {
      case (dockerResult, policyResult, anchoringResult) =>
        log.debug {
          s"External health check finished. Docker: ${extractLogMessage(dockerResult)}, Policy storage: ${extractLogMessage(
              policyResult)}, Anchoring: ${extractLogMessage(anchoringResult)}"
        }

        Right {
          ExternalStatuses(
            dockerResult.map(_ => ExternalStatusResponse.Docker),
            policyResult.map(_ => ExternalStatusResponse.PrivacyStorage),
            anchoringResult.map(_ => ExternalStatusResponse.AnchoringAuth)
          )
        }
    }
  }

  private def extractLogMessage(e: Either[ApiError, Long]): String = e match {
    case Right(took)                => s"OK (took $took ms)"
    case Left(ServiceIsDisabled(_)) => "DISABLED"
    case Left(_)                    => "FAILED"
  }

  private def checkCrypto: Task[Either[ApiError, Long]] = Task {
    crypto.cryptoSettings.checkEnvironment
      .bimap(
        ApiError.fromCryptoError,
        _ => System.currentTimeMillis()
      )
  }

  private def checkRocksDB: Task[Either[ApiError, Long]] = Task.eval {
    Either
      .catchNonFatal {
        storage.get(Keys.schemaVersion)
      }
      .leftMap { e =>
        log.error("Unable to retrieve RocksDB schema version", e)
        HealthCheckError(s"Unable to retrieve RocksDB schema version because of error: '${e.getMessage}'")
      }
      .flatMap { maybeSchemaVersion =>
        maybeSchemaVersion
          .map(_ => System.currentTimeMillis())
          .toRight(HealthCheckError("Unable to retrieve RocksDB schema version"))
      }
  }

  private def checkStateStorage: Task[Either[ApiError, Long]] = Task.eval {
    val freeSpace = fileStorageService.freeSpace(dataDirectory)

    if (freeSpace >= storageFreeSpaceFatalThreshold && freeSpace < storageFreeSpaceWarnThreshold) {
      log.warn(s"The remaining storage free space is sufficient for less than 3 days")
    }

    Either.cond(
      freeSpace >= storageFreeSpaceFatalThreshold,
      System.currentTimeMillis(),
      HealthCheckError(s"Storage free space is below 500MB")
    )
  }

  protected def checkPolicyStorage: Task[Either[ApiError, Long]] = {
    policyStorage match {
      case _: EmptyPolicyStorage => Task(Left(ServiceIsDisabled("Privacy Storage")))
      case enabledPolicyStorage =>
        (for {
          startTime <- EitherT.right(Task.eval(System.currentTimeMillis()))
          _         <- EitherT(enabledPolicyStorage.healthCheck)
        } yield System.currentTimeMillis() - startTime).leftMap { err =>
          log.error(s"Policy storage health check failed: '$err'")
          err
        }.value
    }
  }

  protected def checkDocker: Task[Either[ApiError, Long]] = Task.eval {
    maybeDockerClient.fold[Either[ApiError, Long]] {
      Left(ServiceIsDisabled("Docker"))
    } { client =>
      val startTime = System.currentTimeMillis()
      val isReachable =
        try {
          client.pingCmd().exec()
          true
        } catch {
          case NonFatal(_) => false
        }
      Either
        .cond(isReachable, System.currentTimeMillis() - startTime, HealthCheckError(s"Docker host is not available"))
        .leftMap { err =>
          log.error(s"Docker health check failed: $err")
          err
        }
    }
  }

  protected def checkAnchoringAuthService: Task[Either[ApiError, Long]] = Task.eval {
    maybeAuthProvider.fold[Either[ApiError, Long]] {
      Left(ServiceIsDisabled("Anchoring"))
    } { authProvider =>
      val startTime = System.currentTimeMillis()
      Either
        .cond(authProvider.isAlive, System.currentTimeMillis() - startTime, HealthCheckError(s"Targetnet auth service is not available"))
        .leftMap { err =>
          log.error(s"Anchoring health check failed: '$err'")
          err
        }
    }
  }
}

object HealthChecker {
  def apply(settings: HealthCheckSettings,
            blockchain: Blockchain,
            roundDuration: FiniteDuration,
            maxBlockSizeInBytes: Long,
            storage: RocksDBOperations,
            dataDirectory: String,
            policyStorage: PolicyStorage,
            maybeAuthProvider: Option[TargetnetAuthTokenProvider],
            maybeDockerClient: Option[DockerClient],
            nodeIsFrozenFlag: AtomicBoolean,
            fileStorageService: FileStorageService = FileStorageServiceImpl)(implicit s: Scheduler): HealthChecker = settings match {
    case HealthCheckDisabledSettings =>
      HealthCheckerStateless(
        blockchain,
        roundDuration,
        maxBlockSizeInBytes,
        storage,
        dataDirectory,
        policyStorage,
        maybeAuthProvider,
        maybeDockerClient,
        nodeIsFrozenFlag,
        fileStorageService
      )
    case es: HealthCheckEnabledSettings =>
      HealthCheckerStateful(
        es,
        blockchain,
        roundDuration,
        maxBlockSizeInBytes,
        storage,
        dataDirectory,
        policyStorage,
        maybeAuthProvider,
        maybeDockerClient,
        nodeIsFrozenFlag,
        fileStorageService
      )
  }
}

case class HealthCheckerStateless private (blockchain: Blockchain,
                                           roundDuration: FiniteDuration,
                                           maxBlockSizeInBytes: Long,
                                           storage: RocksDBOperations,
                                           dataDirectory: String,
                                           policyStorage: PolicyStorage,
                                           maybeAuthProvider: Option[TargetnetAuthTokenProvider],
                                           maybeDockerClient: Option[DockerClient],
                                           nodeIsFrozenFlag: AtomicBoolean,
                                           fileStorageService: FileStorageService)(implicit val scheduler: Scheduler)
    extends HealthChecker {
  override protected def innerNodeStatus: Future[Either[ApiError, StatusResponse]]         = healthCheckTask.runToFuture
  override protected def innerExternalStatuses: Future[Either[ApiError, ExternalStatuses]] = externalHealthCheckTask.runToFuture
  override def close(): Unit                                                               = ()
}

case class HealthCheckerStateful private (healthCheckSettings: HealthCheckEnabledSettings,
                                          blockchain: Blockchain,
                                          roundDuration: FiniteDuration,
                                          maxBlockSizeInBytes: Long,
                                          storage: RocksDBOperations,
                                          dataDirectory: String,
                                          policyStorage: PolicyStorage,
                                          maybeAuthProvider: Option[TargetnetAuthTokenProvider],
                                          maybeDockerClient: Option[DockerClient],
                                          nodeIsFrozenFlag: AtomicBoolean,
                                          fileStorageService: FileStorageService)(implicit val scheduler: Scheduler)
    extends HealthChecker {
  @volatile private var state: Option[Either[ApiError, StatusResponse]]           = None
  @volatile private var externalState: Option[Either[ApiError, ExternalStatuses]] = None

  private val pipe = SerialCancelable()

  sys.addShutdownHook {
    close()
  }

  {
    pipe := Task
      .parMap2(healthCheckTask, externalHealthCheckTask) {
        case (nodeStatus, externalStatus) =>
          state = Some(nodeStatus)
          externalState = Some(externalStatus)
      }
      .delayExecution(healthCheckSettings.interval)
      .loopForever
      .executeAsync
      .runToFuture
  }

  override protected def healthCheckTask: Task[Either[ApiError, StatusResponse]] =
    super.healthCheckTask
      .timeout(healthCheckSettings.timeout)
      .onErrorRecover {
        case _: TimeoutException => Left(HealthCheckError("Health check timeout exceeded"))
      }

  override protected def externalHealthCheckTask: Task[Either[ApiError, ExternalStatuses]] =
    super.externalHealthCheckTask
      .timeout(healthCheckSettings.timeout)
      .onErrorRecover {
        case _: TimeoutException => Left(HealthCheckError("External health check timeout exceeded"))
      }

  override def innerNodeStatus: Future[Either[ApiError, StatusResponse]] = {
    state.fold[Future[Either[ApiError, StatusResponse]]](healthCheckTask.runToFuture)(Future.successful)
  }

  override protected def innerExternalStatuses: Future[Either[ApiError, ExternalStatuses]] = {
    externalState.fold[Future[Either[ApiError, ExternalStatuses]]](externalHealthCheckTask.runToFuture)(Future.successful)
  }

  override def close(): Unit = {
    if (!pipe.isCanceled) {
      pipe.cancel()
    }
  }
}

case class ExternalStatuses(docker: Either[ApiError, ExternalStatusResponse.Docker.type],
                            privacyStorage: Either[ApiError, ExternalStatusResponse.PrivacyStorage.type],
                            anchoring: Either[ApiError, ExternalStatusResponse.AnchoringAuth.type])

object ExternalStatuses {
  val frozen: ExternalStatuses = ExternalStatuses(
    Right(ExternalStatusResponse.Docker),
    Right(ExternalStatusResponse.PrivacyStorage),
    Right(ExternalStatusResponse.AnchoringAuth)
  )
}

trait FileStorageService {
  def freeSpace(path: String): Long
}

case object FileStorageServiceImpl extends FileStorageService {
  override def freeSpace(path: String): Long = new File(path).getFreeSpace
}
