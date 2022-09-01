package com.wavesenterprise.http

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.command.PingCmd
import com.wavesenterprise.anchoring.TargetnetAuthTokenProvider
import com.wavesenterprise.api.http.ApiError.HealthCheckError
import com.wavesenterprise.api.http.{ApiError, ExternalStatusResponse, FrozenStatusResponse, NodeStatusResponse, StatusResponse}
import com.wavesenterprise.database.rocksdb.RocksDBOperations
import com.wavesenterprise.database.{Key, Keys}
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.privacy.PolicyStorage
import com.wavesenterprise.settings.HealthCheckEnabledSettings
import com.wavesenterprise.state.Blockchain
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.apache.commons.io.FileUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterEach

import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class HealthCheckerSpec extends AnyFreeSpec with Matchers with MockFactory with BeforeAndAfterEach {

  private val settings      = HealthCheckEnabledSettings(7.seconds, 5.seconds)
  private val nodeIsFrozen  = new AtomicBoolean(false)
  private val timestamp     = System.currentTimeMillis()
  private val blockHeader   = TestBlock.create(timestamp, Seq.empty).blockHeader
  private val roundDuration = 15.seconds
  private val maxBlockSize  = 2 * FileUtils.ONE_MB

  private val blockchain    = mock[Blockchain]
  private val policyStorage = mock[PolicyStorage]
  private val authProvider  = mock[TargetnetAuthTokenProvider]
  private val dockerClient  = mock[DockerClient]
  private val pingCmd       = mock[PingCmd]
  private val rocksDB       = mock[RocksDBOperations]
  private val fileStorage   = mock[FileStorageService]
  private val dataDirectory = "."

  "HealthCheckerStateful" - {
    "Node status" - {
      "should pass with valid statuses" in {
        (blockchain.height _).expects().returns(10)
        (blockchain.blockHeaderAndSize(_: Int)).expects(10).returns(Some(blockHeader -> 50))
        (rocksDB.get(_: Key[Option[Int]])).expects(Keys.schemaVersion).returns(Some(1))
        (fileStorage.freeSpace _).expects(".").returns(550 * FileUtils.ONE_MB)

        val healthChecker = HealthCheckerStateful(
          settings,
          blockchain,
          roundDuration,
          maxBlockSize,
          rocksDB,
          dataDirectory,
          policyStorage,
          Some(authProvider),
          Some(dockerClient),
          nodeIsFrozen,
          fileStorage
        )
        val result = healthChecker.nodeStatusAndCloseUnsafe

        result shouldBe 'right
        val response = result.right.get
        response shouldBe a[NodeStatusResponse]
        val activeNodeResponse = response.asInstanceOf[NodeStatusResponse]
        activeNodeResponse.blockchainHeight shouldBe 10
        activeNodeResponse.stateHeight shouldBe 10
        activeNodeResponse.updatedDate shouldBe Instant.ofEpochMilli(timestamp).toString
        activeNodeResponse.updatedTimestamp shouldBe timestamp
        activeNodeResponse.isFrozen shouldBe None
      }

      "should fail when free disk space is below 300MB" in {
        (blockchain.height _).expects().returns(10)
        (blockchain.blockHeaderAndSize(_: Int)).expects(10).returns(Some(blockHeader -> 50))
        (rocksDB.get(_: Key[Option[Int]])).expects(Keys.schemaVersion).returns(Some(1))
        (fileStorage.freeSpace _).expects(".").returns(300 * FileUtils.ONE_MB)

        val healthChecker = HealthCheckerStateful(
          settings,
          blockchain,
          roundDuration,
          maxBlockSize,
          rocksDB,
          dataDirectory,
          policyStorage,
          Some(authProvider),
          Some(dockerClient),
          nodeIsFrozen,
          fileStorage
        )
        val result = healthChecker.nodeStatusAndCloseUnsafe

        result shouldBe 'left
        result.left.get shouldBe HealthCheckError("Storage free space is below 500MB")
      }

      "should fail when RocksDB returns None" in {
        (blockchain.height _).expects().returns(10)
        (blockchain.blockHeaderAndSize(_: Int)).expects(10).returns(Some(blockHeader -> 50))
        (rocksDB.get(_: Key[Option[Int]])).expects(Keys.schemaVersion).returns(None)

        val healthChecker = HealthCheckerStateful(
          settings,
          blockchain,
          roundDuration,
          maxBlockSize,
          rocksDB,
          dataDirectory,
          policyStorage,
          Some(authProvider),
          Some(dockerClient),
          nodeIsFrozen,
          fileStorage
        )
        val result = healthChecker.nodeStatusAndCloseUnsafe

        result shouldBe 'left
        result.left.get shouldBe HealthCheckError("Unable to retrieve RocksDB schema version")
      }

      "should fail when RocksDB throws exception" in {
        (blockchain.height _).expects().returns(10)
        (blockchain.blockHeaderAndSize(_: Int)).expects(10).returns(Some(blockHeader -> 50))
        (rocksDB.get(_: Key[Option[Int]])).expects(Keys.schemaVersion).throws(new RuntimeException("Some RocksDB exception"))

        val healthChecker = HealthCheckerStateful(
          settings,
          blockchain,
          roundDuration,
          maxBlockSize,
          rocksDB,
          dataDirectory,
          policyStorage,
          Some(authProvider),
          Some(dockerClient),
          nodeIsFrozen,
          fileStorage
        )
        val result = healthChecker.nodeStatusAndCloseUnsafe

        result shouldBe 'left
        result.left.get shouldBe HealthCheckError("Unable to retrieve RocksDB schema version because of error: 'Some RocksDB exception'")
      }

      "should fail by timeout" in {
        (blockchain.height _).expects().returns(10)
        (blockchain.blockHeaderAndSize(_: Int)).expects(10).onCall { _ =>
          Thread.sleep(9.seconds.toMillis)
          Some(blockHeader -> 50)
        }

        val newSettings = HealthCheckEnabledSettings(30.seconds, 5.seconds)
        val healthChecker = HealthCheckerStateful(
          newSettings,
          blockchain,
          roundDuration,
          maxBlockSize,
          rocksDB,
          dataDirectory,
          policyStorage,
          Some(authProvider),
          Some(dockerClient),
          nodeIsFrozen,
          fileStorage
        )
        val result = healthChecker.nodeStatusAndCloseUnsafe

        result shouldBe 'left
        result.left.get shouldBe HealthCheckError("Health check timeout exceeded")
      }
    }

    "External statuses" - {
      "should pass with valid statuses" in {
        (dockerClient.pingCmd _).expects().returning(pingCmd)
        (pingCmd.exec _).expects().returns(null)
        (policyStorage.healthCheck _).expects().returns(Task.pure(Right(())))
        (authProvider.isAlive _).expects().returns(true)

        val healthChecker = HealthCheckerStateful(
          settings,
          blockchain,
          roundDuration,
          maxBlockSize,
          rocksDB,
          dataDirectory,
          policyStorage,
          Some(authProvider),
          Some(dockerClient),
          nodeIsFrozen,
          fileStorage
        )
        val result = healthChecker.externalStatusesAndCloseUnsafe

        result shouldBe Right {
          ExternalStatuses(Right(ExternalStatusResponse.Docker),
                           Right(ExternalStatusResponse.PrivacyStorage),
                           Right(ExternalStatusResponse.AnchoringAuth))
        }
      }

      "should fail when docker is not responding" in {
        (pingCmd.exec _).expects().throws(new RuntimeException("Docker is not available"))
        (dockerClient.pingCmd _).expects().returning(pingCmd)
        (policyStorage.healthCheck _).expects().returns(Task.pure(Right(())))
        (authProvider.isAlive _).expects().returns(true)

        val healthChecker = HealthCheckerStateful(
          settings,
          blockchain,
          roundDuration,
          maxBlockSize,
          rocksDB,
          dataDirectory,
          policyStorage,
          Some(authProvider),
          Some(dockerClient),
          nodeIsFrozen,
          fileStorage
        )
        val result = healthChecker.externalStatusesAndCloseUnsafe

        result shouldBe 'right
        result.right.get.privacyStorage shouldBe 'right
        result.right.get.anchoring shouldBe 'right
        result.right.get.docker shouldBe 'left
        result.right.get.docker.left.get shouldBe HealthCheckError("Docker host is not available")
      }

      "should fail if Policy Storage is not responding" in {
        (policyStorage.healthCheck _).expects().returns(Task.pure(Left(HealthCheckError("DB is not available"))))
        (dockerClient.pingCmd _).expects().returning(pingCmd)
        (pingCmd.exec _).expects().returning(null)
        (authProvider.isAlive _).expects().returns(true)

        val healthChecker = HealthCheckerStateful(
          settings,
          blockchain,
          roundDuration,
          maxBlockSize,
          rocksDB,
          dataDirectory,
          policyStorage,
          Some(authProvider),
          Some(dockerClient),
          nodeIsFrozen,
          fileStorage
        )
        val result = healthChecker.externalStatusesAndCloseUnsafe

        result shouldBe 'right
        result.right.get.docker shouldBe 'right
        result.right.get.anchoring shouldBe 'right
        result.right.get.privacyStorage shouldBe 'left
        result.right.get.privacyStorage.left.get shouldBe HealthCheckError("DB is not available")
      }

      "should fail if Auth Provider is not responding" in {
        (authProvider.isAlive _).expects().returns(false)
        (dockerClient.pingCmd _).expects().returning(pingCmd)
        (pingCmd.exec _).expects().returning(null)
        (policyStorage.healthCheck _).expects().returns(Task.pure(Right(())))

        val healthChecker = HealthCheckerStateful(
          settings,
          blockchain,
          roundDuration,
          maxBlockSize,
          rocksDB,
          dataDirectory,
          policyStorage,
          Some(authProvider),
          Some(dockerClient),
          nodeIsFrozen,
          fileStorage
        )
        val result = healthChecker.externalStatusesAndCloseUnsafe

        result shouldBe 'right
        result.right.get.docker shouldBe 'right
        result.right.get.privacyStorage shouldBe 'right
        result.right.get.anchoring shouldBe 'left
        result.right.get.anchoring.left.get shouldBe HealthCheckError("Targetnet auth service is not available")
      }

      "should fail by timeout" in {
        (dockerClient.pingCmd _).expects().returning(pingCmd)
        (pingCmd.exec _).expects().returning(null)
        (policyStorage.healthCheck _).expects().returns(Task.sleep(9.seconds) >> Task.pure(Right(())))
        (authProvider.isAlive _).expects().returns(true)

        val newSettings = HealthCheckEnabledSettings(30.seconds, 5.seconds)
        val healthChecker = HealthCheckerStateful(
          newSettings,
          blockchain,
          roundDuration,
          maxBlockSize,
          rocksDB,
          dataDirectory,
          policyStorage,
          Some(authProvider),
          Some(dockerClient),
          nodeIsFrozen,
          fileStorage
        )
        val result = healthChecker.externalStatusesAndCloseUnsafe

        result shouldBe 'left
        result.left.get shouldBe HealthCheckError("External health check timeout exceeded")
      }
    }

    "should save status after checker is closed" in {
      (dockerClient.pingCmd _).expects().returning(pingCmd)
      (pingCmd.exec _).expects().returning(null)
      (blockchain.height _).expects().returns(10)
      (blockchain.blockHeaderAndSize(_: Int)).expects(10).returns(Some(blockHeader -> 50))
      (rocksDB.get(_: Key[Option[Int]])).expects(Keys.schemaVersion).returns(Some(1))
      (fileStorage.freeSpace _).expects(".").returns(550 * FileUtils.ONE_MB)
      (policyStorage.healthCheck _).expects().returns(Task.pure(Right(())))
      (authProvider.isAlive _).expects().returns(true)

      val newSettings = HealthCheckEnabledSettings(8.seconds, 5.seconds)
      val healthChecker = HealthCheckerStateful(
        newSettings,
        blockchain,
        roundDuration,
        maxBlockSize,
        rocksDB,
        dataDirectory,
        policyStorage,
        Some(authProvider),
        Some(dockerClient),
        nodeIsFrozen,
        fileStorage
      )
      Thread.sleep(10000) // make sure the state is set up
      healthChecker.close()

      val nodeStatusA = Await.result(healthChecker.nodeStatus, 10.seconds)
      val nodeStatusB = Await.result(healthChecker.nodeStatus, 10.seconds)
      nodeStatusA shouldBe nodeStatusB

      val externalStatusesA = Await.result(healthChecker.nodeStatus, 10.seconds)
      val externalStatusesB = Await.result(healthChecker.nodeStatus, 10.seconds)
      externalStatusesA shouldBe externalStatusesB
    }

    "should pass if node is frozen" in {
      val healthChecker = HealthCheckerStateful(
        settings,
        blockchain,
        roundDuration,
        maxBlockSize,
        rocksDB,
        dataDirectory,
        policyStorage,
        Some(authProvider),
        Some(dockerClient),
        new AtomicBoolean(true),
        fileStorage
      )

      val nodeStatusA = healthChecker.nodeStatusAndCloseUnsafe
      val nodeStatusB = healthChecker.nodeStatusAndCloseUnsafe
      nodeStatusA shouldBe Right(FrozenStatusResponse)
      nodeStatusA shouldBe nodeStatusB

      val externalStatusesA = healthChecker.externalStatusesAndCloseUnsafe
      val externalStatusesB = healthChecker.externalStatusesAndCloseUnsafe
      externalStatusesA shouldBe Right(ExternalStatuses.frozen)
      externalStatusesA shouldBe externalStatusesB
    }
  }

  "HealthCheckerStateless" - {
    "Node status" - {
      "should pass with valid statuses" in {
        (blockchain.height _).expects().returns(10)
        (blockchain.blockHeaderAndSize(_: Int)).expects(10).returns(Some(blockHeader -> 50))
        (rocksDB.get(_: Key[Option[Int]])).expects(Keys.schemaVersion).returns(Some(1))
        (fileStorage.freeSpace _).expects(".").returns(550 * FileUtils.ONE_MB)

        val healthChecker = HealthCheckerStateless(
          blockchain,
          roundDuration,
          maxBlockSize,
          rocksDB,
          dataDirectory,
          policyStorage,
          Some(authProvider),
          Some(dockerClient),
          nodeIsFrozen,
          fileStorage
        )
        val result = healthChecker.nodeStatusAndCloseUnsafe

        result shouldBe 'right
        val response = result.right.get
        response shouldBe a[NodeStatusResponse]
        val activeNodeResponse = response.asInstanceOf[NodeStatusResponse]
        activeNodeResponse.blockchainHeight shouldBe 10
        activeNodeResponse.stateHeight shouldBe 10
        activeNodeResponse.updatedDate shouldBe Instant.ofEpochMilli(timestamp).toString
        activeNodeResponse.updatedTimestamp shouldBe timestamp
        activeNodeResponse.isFrozen shouldBe None
      }
    }

    "External statuses" - {
      "should pass with valid statuses" in {
        (dockerClient.pingCmd _).expects().returning(pingCmd)
        (pingCmd.exec _).expects().returns(null)
        (policyStorage.healthCheck _).expects().returns(Task.pure(Right(())))
        (authProvider.isAlive _).expects().returns(true)

        val healthChecker = HealthCheckerStateless(
          blockchain,
          roundDuration,
          maxBlockSize,
          rocksDB,
          dataDirectory,
          policyStorage,
          Some(authProvider),
          Some(dockerClient),
          nodeIsFrozen,
          fileStorage
        )
        val result = healthChecker.externalStatusesAndCloseUnsafe

        result shouldBe Right {
          ExternalStatuses(Right(ExternalStatusResponse.Docker),
                           Right(ExternalStatusResponse.PrivacyStorage),
                           Right(ExternalStatusResponse.AnchoringAuth))
        }
      }
    }
  }

  implicit class HealthCheckerExt(healthChecker: HealthChecker) {
    def nodeStatusAndCloseUnsafe: Either[ApiError, StatusResponse] = {
      try {
        Await.result(healthChecker.nodeStatus, 10.seconds)
      } finally {
        healthChecker.close()
      }
    }

    def externalStatusesAndCloseUnsafe: Either[ApiError, ExternalStatuses] = {
      try {
        Await.result(healthChecker.externalStatuses, 10.seconds)
      } finally {
        healthChecker.close()
      }
    }
  }
}
