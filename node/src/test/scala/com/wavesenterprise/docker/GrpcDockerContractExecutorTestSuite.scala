package com.wavesenterprise.docker

import akka.actor.ActorSystem
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.wavesenterprise.TestSchedulers
import com.wavesenterprise.account.Address
import com.wavesenterprise.docker.CircuitBreakerSupport.CircuitBreakerError.{ContractOpeningLimitError, OpenedCircuitBreakersLimitError}
import com.wavesenterprise.docker.ContractExecutionError.RecoverableErrorCode
import com.wavesenterprise.docker.DockerContractExecutor.ContainerKey
import com.wavesenterprise.docker.grpc.GrpcDockerContractExecutor.ConnectionId
import com.wavesenterprise.docker.grpc.{GrpcDockerContractExecutor, NodeGrpcApiSettings}
import com.wavesenterprise.metrics.Metrics.CircuitBreakerCacheSettings
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.metrics.docker.ContractExecutionMetrics
import com.wavesenterprise.protobuf.service.contract.ContractTransactionResponse
import com.wavesenterprise.settings.dockerengine._
import com.wavesenterprise.settings.{PositiveInt, dockerengine}
import com.wavesenterprise.state.NG
import com.wavesenterprise.transaction.docker.{CreateContractTransaction, CreateContractTransactionV2}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.wallet.Wallet
import monix.execution.Scheduler
import monix.execution.exceptions.ExecutionRejectedException
import org.apache.commons.codec.digest.DigestUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class GrpcDockerContractExecutorTestSuite
    extends AnyFreeSpec
    with Matchers
    with MockFactory
    with ScalaCheckPropertyChecks
    with ScalaFutures
    with BeforeAndAfterAll {

  implicit val as: ActorSystem      = ActorSystem()
  implicit val scheduler: Scheduler = TestSchedulers.dockerExecutorScheduler

  private val blockchain = stub[NG]

  private val sourceQueue = Source
    .queue[ContractTransactionResponse](100, OverflowStrategy.backpressure)
    .toMat(Sink.ignore)(Keep.left)
    .run()

  private val contractAuthService = new ContractAuthTokenService()

  private val dockerEngineSettings = DockerEngineSettings(
    enable = true,
    None,
    None,
    useNodeDockerHost = false,
    None,
    ContractExecutionLimitsSettings(4.seconds, 2.seconds, 512, 0),
    10.minutes,
    List.empty,
    checkRegistryAuthOnStartup = true,
    None,
    ContractExecutionMessagesCacheSettings(60.minutes, 100000, 10, 100.millis, 5.minutes, PositiveInt(3)),
    1.minute,
    GrpcServerSettings(None, 6865),
    removeContainerOnFail = true,
    CircuitBreakerSettings(PositiveInt(2), 5, 1000, 4.hours, 5.seconds, 1.0, 1.minute),
    PositiveInt(8)
  )

  private val connectionTime = dockerEngineSettings.executionLimits.startupTimeout / 2
  private val startupTimeout = dockerEngineSettings.executionLimits.startupTimeout + 1.second
  private val executeTimeout = dockerEngineSettings.executionLimits.timeout + 1.second
  private val executionDelay = dockerEngineSettings.executionLimits.timeout / 2

  private val grpcApiSettings = NodeGrpcApiSettings("localhost", 6865)

  private val defaultCircuitBreakerCacheSettings = CircuitBreakerCacheSettings(10, 10.seconds)

  private def createGrpcContractExecutor(
      dockerEngineStub: DockerEngine,
      contractReusedContainers: ContractReusedContainers,
      settingsMapper: DockerEngineSettings => DockerEngineSettings
  ): GrpcDockerContractExecutor =
    new GrpcDockerContractExecutor(
      dockerEngineStub,
      settingsMapper(dockerEngineSettings),
      grpcApiSettings,
      defaultCircuitBreakerCacheSettings,
      contractAuthService,
      contractReusedContainers,
      blockchain,
      scheduler
    )

  private val sender      = Wallet.generateNewAccount()
  private val containerId = "some_container_id"

  private def createTx(): CreateContractTransaction = {
    CreateContractTransactionV2
      .selfSigned(sender,
                  "localhost:5000/smart-kv",
                  DigestUtils.sha256Hex("hash"),
                  "contract",
                  List.empty,
                  Random.nextInt(),
                  System.currentTimeMillis(),
                  None)
      .explicitGet()
  }

  case class FixtureParams(startupCount: AtomicInteger, removeCount: AtomicInteger, contractExecutor: GrpcDockerContractExecutor)

  override protected def afterAll(): Unit = {
    sourceQueue.complete()
  }

  def fixture(settingsMapper: DockerEngineSettings => DockerEngineSettings = identity)(test: FixtureParams => Unit): Unit = {
    val startupCount = new AtomicInteger(0)
    val removeCount  = new AtomicInteger(0)

    val dockerEngine = stub[DockerEngine]
    (dockerEngine
      .removeContainer(_: String))
      .when(containerId)
      .onCall((_: String) => {
        removeCount.incrementAndGet(); ()
      })
      .anyNumberOfTimes()
    (dockerEngine
      .createAndStartContainer(_: ContractInfo, _: ContractExecutionMetrics, _: List[String]))
      .when(*, *, *)
      .onCall((_: ContractInfo, _: ContractExecutionMetrics, _: List[String]) => {
        startupCount.incrementAndGet()
        Right(containerId)
      })
      .anyNumberOfTimes()
    val contractReusedContainers = new ContractReusedContainers(10.minutes)

    (blockchain.lastBlock _)
      .when()
      .returning(Option(TestBlock.create(Seq.empty)))
      .anyNumberOfTimes()
    (blockchain.currentMiner _)
      .when()
      .returning(Option(Address.fromString("3MqQQYcdzYRGB1Dv5rsMU1DWRCbjNz1zTAs").right.get))
      .anyNumberOfTimes()

    val fixtureParams = FixtureParams(startupCount, removeCount, createGrpcContractExecutor(dockerEngine, contractReusedContainers, settingsMapper))
    try {
      test(fixtureParams)
    } finally {
      contractReusedContainers.close()
    }
  }

  private def checkInvalidation(containerKey: ContainerKey, fixtureParams: FixtureParams): Unit = {
    fixtureParams.contractExecutor.contractReusedContainers.invalidate(containerKey)
    Thread.sleep(1.second.toMillis)

    fixtureParams.startupCount.get() shouldBe 1 // container must be created just one time
    fixtureParams.removeCount.get() shouldBe 1  // container must be removed just one time
  }

  "test successful contract startup" - {
    "must be one startup for few start calls" in fixture() {
      case fixtureParams @ FixtureParams(_, _, contractExecutor) =>
        val contract = ContractInfo(createTx())
        contractExecutor.contractStarted(contract).foreach(_ shouldBe false)

        val futures = (1 to 3).map { _ =>
          contractExecutor.startContract(contract, ContractExecutionMetrics(createTx()))
        }

        deferEither {
          /* Make contract started (connected) */
          val containerKey = ContainerKey(contract)
          val connectionId = ConnectionId(containerKey)
          contractExecutor.addConnection(connectionId, sourceQueue)
        }.delayExecution(connectionTime)
          .executeAsync
          .executeOn(scheduler)
          .runToFuture

        futures.foreach { f =>
          Await.result(f, startupTimeout) shouldBe containerId
        }
        contractExecutor.contractStarted(contract).foreach(_ shouldBe true)

        checkInvalidation(ContainerKey(contract), fixtureParams)
    }
  }

  "test contract startup timeout" - {
    "contract must fail for invalid container" in fixture() {
      case fixtureParams @ FixtureParams(_, _, contractExecutor) =>
        val tx       = createTx()
        val contract = ContractInfo(tx)
        val metrics  = ContractExecutionMetrics(tx)
        contractExecutor.contractStarted(contract).foreach(_ shouldBe false)

        val future = contractExecutor.startContract(contract, metrics)

        val throwable = future.failed.futureValue(Timeout(startupTimeout))
        throwable shouldBe a[ContractExecutionException]
        throwable.getMessage should include regex s"Container '$containerId' startup timeout"

        checkInvalidation(ContainerKey(ContractInfo(tx)), fixtureParams)
    }
  }

  "test circuit breaker" - {
    "must switch between state for contract start" in fixture() {
      case FixtureParams(_, _, contractExecutor) =>
        val tx       = createTx()
        val contract = ContractInfo(tx)
        val metrics  = ContractExecutionMetrics(tx)

        val maxFailures = dockerEngineSettings.circuitBreaker.maxFailures.value

        /* Imitating $maxFailures unsuccessful attempts */
        (1 to maxFailures).foreach { _ =>
          val failure = contractExecutor.startContract(contract, metrics)

          val throwable = failure.failed.futureValue(Timeout(startupTimeout))
          throwable shouldBe a[ContractExecutionException]
        }

        /* After unsuccessful attempts circuit breaker must switch to Open state and reject all executions */
        val rejected = contractExecutor.startContract(contract, metrics)

        val throwable = rejected.failed.futureValue(Timeout(startupTimeout))
        throwable shouldBe a[ExecutionRejectedException]

        /* Waiting for reset timeout expire and circuit breaker switch to HalfOpen state */
        Thread.sleep(dockerEngineSettings.circuitBreaker.resetTimeout.toMillis)

        val successful = contractExecutor.startContract(contract, metrics)

        deferEither {
          /* Make contract started (connected) */
          val containerKey = ContainerKey(contract)
          val connectionId = ConnectionId(containerKey)
          contractExecutor.addConnection(connectionId, sourceQueue)
        }.delayExecution(connectionTime)
          .executeAsync
          .executeOn(scheduler)
          .runToFuture

        Await.result(successful, startupTimeout) shouldBe containerId
        contractExecutor.contractStarted(contract).foreach(_ shouldBe true)
    }

    "must switch between state for contract system errors" in fixture() {
      case FixtureParams(_, _, contractExecutor) =>
        val tx       = createTx()
        val contract = ContractInfo(tx)
        val metrics  = ContractExecutionMetrics(tx)

        val start = contractExecutor.startContract(contract, metrics)

        deferEither {
          /* Make contract started (connected) */
          val containerKey = ContainerKey(contract)
          val connectionId = ConnectionId(containerKey)
          contractExecutor.addConnection(connectionId, sourceQueue)
        }.delayExecution(connectionTime)
          .executeAsync
          .executeOn(scheduler)
          .runToFuture

        Await.result(start, startupTimeout) shouldBe containerId

        val maxFailures = dockerEngineSettings.circuitBreaker.maxFailures.value

        /* Imitating $maxFailures unsuccessful attempts */
        (1 to maxFailures).foreach { i =>
          val failure = contractExecutor.executeTransaction(contract, tx, None, metrics).executeAsync.executeOn(scheduler).runToFuture

          deferEither {
            contractExecutor.commitExecutionError(i.toString, tx.id(), "Error: service unavailable", RecoverableErrorCode)
          }.delayExecution(executionDelay)
            .executeAsync
            .executeOn(scheduler)
            .runToFuture

          val throwable = failure.failed.futureValue(Timeout(executeTimeout))
          throwable shouldBe a[ContractExecutionException]
          throwable.asInstanceOf[ContractExecutionException].code shouldBe Some(RecoverableErrorCode)
        }

        /* After unsuccessful attempts circuit breaker must switch to Open state and reject all executions */
        val rejected = contractExecutor.executeTransaction(contract, tx, None, metrics).executeAsync.executeOn(scheduler).runToFuture

        val throwable = rejected.failed.futureValue(Timeout(executeTimeout))
        throwable shouldBe a[ExecutionRejectedException]

        /* Waiting for reset timeout expire and circuit breaker switch to HalfOpen state */
        Thread.sleep(dockerEngineSettings.circuitBreaker.resetTimeout.toMillis)

        val successful = contractExecutor.executeTransaction(contract, tx, None, metrics).executeAsync.executeOn(scheduler).runToFuture

        deferEither {
          contractExecutor.commitExecutionResults((maxFailures + 1).toString, tx.id(), List.empty, List.empty)
        }.delayExecution(executionDelay)
          .executeAsync
          .executeOn(scheduler)
          .runToFuture

        Await.result(successful, executeTimeout) shouldBe ContractExecutionSuccess(List.empty)
    }

    "should throw OpenedCircuitBreakersLimitError" in fixture(
      { settings =>
        settings.copy(
          circuitBreaker = dockerengine.CircuitBreakerSettings(
            maxFailures = PositiveInt(3),
            contractOpeningLimit = 10,
            openedBreakersLimit = 0,
            expireAfter = 4.hours,
            resetTimeout = 5.seconds,
            exponentialBackoffFactor = 1.0,
            maxResetTimeout = 1.minute
          )
        )
      }
    ) {
      case fixtureParams @ FixtureParams(_, _, contractExecutor) =>
        val tx       = createTx()
        val contract = ContractInfo(tx)
        val metrics  = ContractExecutionMetrics(tx)

        val maxFailures = dockerEngineSettings.circuitBreaker.maxFailures.value

        /* Imitating unsuccessful attempt to open circuit-breaker */
        (1 to maxFailures).foreach { _ =>
          val failure = contractExecutor.startContract(contract, metrics)

          val throwable = failure.failed.futureValue(Timeout(startupTimeout))
          throwable shouldBe a[ContractExecutionException]
        }

        /* The next attempt should throw circuit-breakers limit error */
        val limitFailure = contractExecutor.startContract(contract, metrics)
        limitFailure.failed.futureValue(Timeout(startupTimeout)) shouldBe a[OpenedCircuitBreakersLimitError]

        contractExecutor.contractStarted(contract).foreach(_ shouldBe false)

        /* Waiting for reset timeout expire and circuit breaker switch to HalfOpen state */
        Thread.sleep(dockerEngineSettings.circuitBreaker.resetTimeout.toMillis)

        val successful = contractExecutor.startContract(contract, metrics)

        val containerKey = ContainerKey(contract)
        val connectionId = ConnectionId(containerKey)

        /* Make contract started (connected) */
        deferEither {
          contractExecutor.addConnection(connectionId, sourceQueue)
        }.delayExecution(connectionTime)
          .executeAsync
          .executeOn(scheduler)
          .runToFuture

        Await.result(successful, startupTimeout) shouldBe containerId
        contractExecutor.contractStarted(contract).foreach(_ shouldBe true)

        fixtureParams.contractExecutor.contractReusedContainers.invalidate(containerKey)
        Thread.sleep(1.second.toMillis)

        /* Imitating unsuccessful attempt again to open circuit-breaker */
        (1 to maxFailures).foreach { _ =>
          val failure = contractExecutor.startContract(contract, metrics)

          val throwable = failure.failed.futureValue(Timeout(startupTimeout))
          throwable shouldBe a[ContractExecutionException]
        }

        /* The next attempt should again throw circuit-breakers limit error */
        val limitFailure2 = contractExecutor.startContract(contract, metrics)
        limitFailure2.failed.futureValue(Timeout(startupTimeout)) shouldBe a[OpenedCircuitBreakersLimitError]

        contractExecutor.contractStarted(contract).foreach(_ shouldBe false)
    }

    "should throw ContractOpeningLimitError" in fixture(
      { settings =>
        settings.copy(
          circuitBreaker = dockerengine.CircuitBreakerSettings(
            maxFailures = PositiveInt(3),
            contractOpeningLimit = 0,
            openedBreakersLimit = 1000,
            expireAfter = 4.hours,
            resetTimeout = 5.seconds,
            exponentialBackoffFactor = 1.0,
            maxResetTimeout = 1.minute
          )
        )
      }
    ) {
      case fixtureParams @ FixtureParams(_, _, contractExecutor) =>
        val tx       = createTx()
        val contract = ContractInfo(tx)
        val metrics  = ContractExecutionMetrics(tx)

        val maxFailures = dockerEngineSettings.circuitBreaker.maxFailures.value

        /* Imitating unsuccessful attempt to open circuit-breaker */
        (1 to maxFailures).foreach { _ =>
          val failure = contractExecutor.startContract(contract, metrics)

          val throwable = failure.failed.futureValue(Timeout(startupTimeout))
          throwable shouldBe a[ContractExecutionException]
        }

        /* The next attempt should throw contract opening limit error */
        val limitFailure = contractExecutor.startContract(contract, metrics)
        limitFailure.failed.futureValue(Timeout(startupTimeout)) shouldBe a[ContractOpeningLimitError]
        contractExecutor.contractStarted(contract).foreach(_ shouldBe false)

        /* Waiting for reset timeout expire and circuit breaker switch to HalfOpen state */
        Thread.sleep(dockerEngineSettings.circuitBreaker.resetTimeout.toMillis)

        val successful = contractExecutor.startContract(contract, metrics)

        val containerKey = ContainerKey(contract)
        val connectionId = ConnectionId(containerKey)

        /* Make contract started (connected) */
        deferEither {
          contractExecutor.addConnection(connectionId, sourceQueue)
        }.delayExecution(connectionTime)
          .executeAsync
          .executeOn(scheduler)
          .runToFuture

        Await.result(successful, startupTimeout) shouldBe containerId
        contractExecutor.contractStarted(contract).foreach(_ shouldBe true)

        fixtureParams.contractExecutor.contractReusedContainers.invalidate(containerKey)
        Thread.sleep(1.second.toMillis)

        /* Imitating unsuccessful attempt again to increase counter */
        (1 to maxFailures).foreach { _ =>
          val failure = contractExecutor.startContract(contract, metrics)

          val throwable = failure.failed.futureValue(Timeout(startupTimeout))
          throwable shouldBe a[ContractExecutionException]
        }

        /* The next attempt should again throw contract opening limit error*/
        val secondLimitFailure = contractExecutor.startContract(contract, metrics)
        secondLimitFailure.failed.futureValue(Timeout(startupTimeout)) shouldBe a[ContractOpeningLimitError]

        contractExecutor.contractStarted(contract).foreach(_ shouldBe false)
    }
  }
}
