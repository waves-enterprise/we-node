package com.wavesenterprise.settings.dockerengine

import cats.Show
import cats.implicits.{catsStdShowForOption => _, _}
import com.github.dockerjava.api.model.AuthConfig
import com.wavesenterprise.settings.{PositiveInt, WEConfigReaders}
import com.wavesenterprise.settings.WEConfigReaders
import com.wavesenterprise.utils.StringUtils._
import scala.util.chaining.scalaUtilChainingOps
import okhttp3.HttpUrl
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

import java.net.URI
import scala.concurrent.duration.FiniteDuration

case class DockerEngineSettings(
    enable: Boolean,
    dockerAuth: Option[DockerAuth],
    dockerHost: Option[URI],
    useNodeDockerHost: Boolean,
    nodeRestApi: Option[HttpUrl],
    executionLimits: ContractExecutionLimitsSettings,
    removeContainerAfter: FiniteDuration,
    remoteRegistries: List[RegistryAuth],
    checkRegistryAuthOnStartup: Boolean,
    defaultRegistryDomain: Option[String],
    contractExecutionMessagesCache: ContractExecutionMessagesCacheSettings,
    contractAuthExpiresIn: FiniteDuration,
    grpcServer: GrpcServerSettings,
    removeContainerOnFail: Boolean,
    circuitBreaker: CircuitBreakerSettings,
    contractsParallelism: PositiveInt
) {
  val registriesAuthByAddress: Map[String, AuthConfig] = remoteRegistries.map(auth => auth.native.getRegistryAddress -> auth.native).toMap
}

object DockerEngineSettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[DockerEngineSettings] = deriveReader

  implicit val toPrintable: Show[DockerEngineSettings] = { x =>
    import x._

    s"""
       |enable:                      $enable
       |dockerHost:                  $dockerHost
       |nodeRestApi:                 $nodeRestApi
       |removeContainerAfter:        $removeContainerAfter
       |checkRegistryAuthOnStartup:  $checkRegistryAuthOnStartup
       |defaultRegistryDomain:       $defaultRegistryDomain
       |useNodeDockerHost:           $useNodeDockerHost
       |dockerAuth:
       |  ${dockerAuth.show pipe dashes}
       |contractExecutionLimits:
       |  ${executionLimits.show pipe dashes}
       |contractAuthExpiresIn:       $contractAuthExpiresIn
       |grpcServer:
       |  ${grpcServer.show pipe dashes}
       |contractExecutionMessagesCache:
       |  ${contractExecutionMessagesCache.show pipe dashes}
       |removeContainerOnFail:       $removeContainerOnFail
       |circuitBreaker:
       |  ${circuitBreaker.show pipe dashes}
       |contractsParallelism: ${contractsParallelism.value}
       """.stripMargin
  }
}

case class DockerAuth(username: String, password: String)

object DockerAuth extends WEConfigReaders {

  implicit val configReader: ConfigReader[DockerAuth] = deriveReader

  implicit val toPrintable: Show[DockerAuth] = { x =>
    import x._

    s"""
       |username: $username
       |password: $password
       """.stripMargin
  }
}

case class RegistryAuth(username: String, password: String, domain: String) {
  val native: AuthConfig = new AuthConfig()
    .withUsername(username)
    .withPassword(password)
    .withRegistryAddress(domain)
}

object RegistryAuth extends WEConfigReaders {
  implicit val configReader: ConfigReader[RegistryAuth] = deriveReader
}

case class GrpcServerSettings(host: Option[String], port: Int)

object GrpcServerSettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[GrpcServerSettings] = deriveReader

  implicit val toPrintable: Show[GrpcServerSettings] = { x =>
    import x._

    s"""
       |host: $host
       |port: $port
       """.stripMargin
  }
}
