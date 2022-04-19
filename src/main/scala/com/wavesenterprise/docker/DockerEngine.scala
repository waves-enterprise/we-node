package com.wavesenterprise.docker

import cats.implicits._
import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.async.ResultCallback.Adapter
import com.github.dockerjava.api.command.{CreateContainerCmd, InspectImageResponse, PullImageResultCallback}
import com.github.dockerjava.api.exception.NotFoundException
import com.github.dockerjava.api.model._
import com.github.dockerjava.core.{DefaultDockerClientConfig, DockerClientImpl}
import com.github.dockerjava.jaxrs.JerseyDockerHttpClient
import com.wavesenterprise.docker.ExecuteCommandInDocker.ProcessSuccessCode
import com.wavesenterprise.metrics.docker._
import com.wavesenterprise.settings.dockerengine.{DockerAuth, DockerEngineSettings}
import com.wavesenterprise.utils.Base64
import monix.eval.Coeval
import org.apache.commons.io.FileUtils
import org.glassfish.jersey.client.{ClientProperties, RequestEntityProcessing}

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Paths
import java.util.{Collections, concurrent => juc}
import javax.ws.rs.client.{ClientRequestContext, ClientRequestFilter}
import javax.ws.rs.core.HttpHeaders
import scala.collection.JavaConverters._
import scala.concurrent.blocking
import scala.io.Source
import scala.util.Try

trait DockerEngine extends ExecuteCommandInDocker {

  def inspectContractImage(contract: ContractInfo, metrics: ContractExecutionMetrics): Either[ContractExecutionException, InspectImageResponse]

  def imageExists(contract: ContractInfo): Either[ContractExecutionException, Boolean]

  def executeBashCommandInContainer(containerId: String,
                                    command: String,
                                    privileged: Boolean = true): Either[ContractExecutionException, (Int, String)]

  def createAndStartContainer(contract: ContractInfo,
                              metrics: ContractExecutionMetrics,
                              envParams: List[String]): Either[ContractExecutionException, String]

  def executeRunScript(containerId: String,
                       params: Map[String, String],
                       metrics: ContractExecutionMetrics): Either[ContractExecutionException, (Int, String)]

  def removeContainer(containerId: String): Unit

  def stopAllRunningContainers(): Unit
}

object DockerEngine {

  case class HostnameMapping(ip: String, hostname: String)

  class BasicAuthenticationRequestFilter(auth: DockerAuth) extends ClientRequestFilter {

    private[this] val credentials = Base64.encode(s"${auth.username}:${auth.password}".getBytes(UTF_8))

    override def filter(requestContext: ClientRequestContext): Unit = {
      requestContext.getHeaders.add(HttpHeaders.AUTHORIZATION, s"Basic $credentials")
      requestContext.setProperty(ClientProperties.REQUEST_ENTITY_PROCESSING, RequestEntityProcessing.BUFFERED)
    }
  }

  class ImageDigestValidationException(imageId: String, contract: ContractInfo)
      extends ContractExecutionException(s"Id '$imageId' of local image '${contract.image}' isn't equal to its digest '${contract.imageHash}'")

  class ImageNameValidationException(imageName: DockerImageName, imageInfo: InspectImageResponse)
      extends ContractExecutionException(
        s"Invalid image name '${imageName.image}' for image with id '${imageInfo.getId}'. RepoTags and RepoDigests don't contain its full name '${imageName.fullName}'")

  private[docker] val ContractSuccessCode: Int = ProcessSuccessCode
  private[docker] val ContractErrorCode: Int   = 3

  private def quotify(s: String): String = s.replace("'", "'\\''")

  private[docker] def envMapToString(params: Map[String, String]): String = params.map(p => s"${p._1}='${quotify(p._2)}'").mkString(" ")

  def readContainerHostnameMapping(): Coeval[HostnameMapping] = {
    val acquire = Coeval.now(Source.fromFile(Paths.get("/etc/hosts").toFile))
    acquire.bracket { source =>
      Coeval.now(source.getLines().toList.last)
    } { in =>
      Coeval.eval(in.close())
    } map { string =>
      val values = string.split("\\t").map(_.trim)
      HostnameMapping(values(0), values(1))
    }
  }

  def extractImageDigest(imageId: String): String = {
    imageId.replaceFirst("sha256:", "")
  }

  private[docker] def checkImageDigest(contract: ContractInfo, imageInfo: InspectImageResponse): Either[ImageDigestValidationException, Unit] = {
    val imageId = extractImageDigest(imageInfo.getId)
    Either.cond(imageId == contract.imageHash, (), new ImageDigestValidationException(imageId, contract))
  }

  private[docker] def checkImageName(imageName: DockerImageName, imageInfo: InspectImageResponse): Either[ImageNameValidationException, Unit] = {
    val availableNames = imageInfo.getRepoDigests.asScala ++ imageInfo.getRepoTags.asScala
    Either.cond(availableNames.contains(imageName.fullName), (), new ImageNameValidationException(imageName, imageInfo))
  }

  private[docker] def checkImage(contract: ContractInfo,
                                 imageName: DockerImageName,
                                 imageInfo: InspectImageResponse): Either[ContractExecutionException, InspectImageResponse] = {
    for {
      _ <- checkImageDigest(contract, imageInfo)
      _ <- checkImageName(imageName, imageInfo)
    } yield imageInfo
  }

  def apply(dockerEngineSettings: DockerEngineSettings): Either[Throwable, DockerEngine] = {
    for {
      dockerClient <- createDockerClient(dockerEngineSettings)
      _            <- pingDockerDaemon(dockerClient)
      _            <- checkRegistryAuthOnStartup(dockerEngineSettings, dockerClient)
    } yield new DockerEngineImpl(dockerClient, dockerEngineSettings)
  }

  private def createDockerClient(settings: DockerEngineSettings): Either[Throwable, DockerClient] = Either.catchNonFatal {
    val configBuilder = DefaultDockerClientConfig.createDefaultConfigBuilder
    val clientBuilder = new JerseyDockerHttpClient.Builder()

    settings.dockerHost.foreach(dockerHost => configBuilder.withDockerHost(dockerHost.toString))
    settings.dockerAuth.foreach(dockerAuth => clientBuilder.clientRequestFilters(Array(new BasicAuthenticationRequestFilter(dockerAuth))))

    val config           = configBuilder.build()
    val dockerHttpClient = clientBuilder.dockerHost(config.getDockerHost).sslConfig(config.getSSLConfig).build()

    DockerClientImpl.getInstance(config, dockerHttpClient)
  }

  private def pingDockerDaemon(dockerClient: DockerClient): Either[Throwable, Void] = {
    Either
      .catchNonFatal(dockerClient.pingCmd().exec())
      .leftMap(e => new RuntimeException("Can't connect to docker daemon. Install docker or disable docker-engine or check auth credentials", e))
  }

  private def checkRegistryAuthOnStartup(dockerEngineSettings: DockerEngineSettings, dockerClient: DockerClient): Either[Throwable, Unit] = {
    if (dockerEngineSettings.checkRegistryAuthOnStartup) {
      val registriesAuth = dockerEngineSettings.registriesAuthByAddress.values.toList
      registriesAuth.traverse { auth =>
        Either
          .catchNonFatal {
            dockerClient.authCmd().withAuthConfig(auth).exec()
          }
          .leftMap(e => new RuntimeException(s"Docker auth request failed for '$auth'", e))
      } map { _ =>
        ()
      }
    } else {
      Right(())
    }
  }
}

private class DockerEngineImpl(val docker: DockerClient, dockerEngineSettings: DockerEngineSettings) extends DockerEngine {

  import DockerEngine._

  private[this] val containerMemory: Long     = dockerEngineSettings.executionLimits.memory * FileUtils.ONE_MB
  private[this] val containerMemorySwap: Long = dockerEngineSettings.executionLimits.memorySwap * FileUtils.ONE_MB

  private[this] val containersIdSet: java.util.Set[String] = Collections.newSetFromMap[String](new juc.ConcurrentHashMap)

  private def createContainerHostConfig(): HostConfig = {
    new HostConfig().withMemory(containerMemory).withMemorySwap(containerMemorySwap)
  }

  private def setContainerNetwork(containerCreation: CreateContainerCmd): Either[ContractExecutionException, Unit] =
    resolveNodeNetworkName().map(nodeNetworkName => containerCreation.getHostConfig.withNetworkMode(nodeNetworkName))

  private def deployContainer(contract: ContractInfo,
                              metrics: ContractExecutionMetrics,
                              envParams: List[String]): Either[ContractExecutionException, String] = {
    for {
      imageInfo <- inspectContractImage(contract, metrics)
      containerCreation = docker.createContainerCmd(imageInfo.getId).withHostConfig(createContainerHostConfig()).withEnv(envParams: _*)
      _ <- if (dockerEngineSettings.useNodeDockerHost) setContainerNetwork(containerCreation) else Right(())
      containerId = metrics.measure(CreateContainer, blocking(containerCreation.exec().getId))
    } yield containerId
  }

  def inspectContractImage(contract: ContractInfo, metrics: ContractExecutionMetrics): Either[ContractExecutionException, InspectImageResponse] = {
    for {
      imageName <- DockerImageNameNormalizer.normalize(contract.image, dockerEngineSettings.defaultRegistryDomain)
      imageInfo <- inspectOrPullImage(contract, imageName, metrics)
      checkedImageInfo <- checkImage(contract, imageName, imageInfo)
        .recoverWith {
          case _: ImageNameValidationException =>
            log.warn(
              s"Image name check failed: tags and digests don't contain its full name '${imageName.fullName}'. Pulling image from registry to update its tags and digests...")
            pullAndInspectImage(imageName, metrics).flatMap(checkImage(contract, imageName, _))
        }
    } yield checkedImageInfo
  }

  private def inspectOrPullImage(contract: ContractInfo,
                                 imageName: DockerImageName,
                                 metrics: ContractExecutionMetrics): Either[ContractExecutionException, InspectImageResponse] = {
    val ContractInfo(_, _, image, imageId, _, _, _, _) = contract
    Either
      .catchNonFatal(blocking(docker.inspectImageCmd(imageId).exec()))
      .leftFlatMap {
        case _: NotFoundException =>
          log.warn(s"Image '$image' with id '$imageId' is not found. Pulling image by name '${imageName.fullName}' from registry...")
          pullAndInspectImage(imageName, metrics)
        case t: Throwable => Left(new ContractExecutionException(s"Can't inspect image with id '$imageId'", t))
      }
  }

  private def pullAndInspectImage(imageName: DockerImageName,
                                  metrics: ContractExecutionMetrics): Either[ContractExecutionException, InspectImageResponse] = {
    metrics.measureEither(PullImage, pullImageFromRegistry(imageName)).map { _ =>
      blocking(docker.inspectImageCmd(imageName.fullName).exec())
    }
  }

  private def pullImageFromRegistry(imageName: DockerImageName): Either[ContractExecutionException, Adapter[PullResponseItem]] = {
    val maybeAuth    = extractImageRegistryAuth(imageName)
    val pullImageCmd = docker.pullImageCmd(imageName.fullName)
    maybeAuth.foreach(pullImageCmd.withAuthConfig)

    Either
      .catchNonFatal(blocking(pullImageCmd.exec(new PullImageResultCallback).awaitCompletion()))
      .leftMap {
        case e: NotFoundException =>
          new ContractExecutionException(s"Can't pull image '${imageName.fullName}', image is not found in registry", e)
        case t: Throwable => new ContractExecutionException(s"Can't pull image '${imageName.fullName}' from registry", t)
      }
  }

  def imageExists(contractInfo: ContractInfo): Either[ContractExecutionException, Boolean] =
    Either
      .catchNonFatal(
        blocking(
          docker
            .inspectImageCmd(contractInfo.imageHash)
            .exec()
            .getRepoTags
            .asScala
            .toList
        )
      )
      .map(repoTags => repoTags.exists(_.contains(contractInfo.image)))
      .recover {
        case _: NotFoundException => false
      }
      .leftMap { t =>
        new ContractExecutionException(s"Can't inspect image with id '${contractInfo.imageHash}'", t)
      }

  def executeBashCommandInContainer(containerId: String,
                                    command: String,
                                    privileged: Boolean = true): Either[ContractExecutionException, (Int, String)] = {
    executeCommandInContainer(containerId, Array[String]("/bin/sh", "-c", command), Set(ContractSuccessCode, ContractErrorCode), privileged)
      .leftMap(e => new ContractExecutionException(e.getMessage, Some(e.exitCode)))
  }

  def createAndStartContainer(contract: ContractInfo,
                              metrics: ContractExecutionMetrics,
                              envParams: List[String]): Either[ContractExecutionException, String] = {
    for {
      containerId <- deployContainer(contract, metrics, envParams)
      _ = containersIdSet.add(containerId)
      _ <- metrics.measureEither(
        StartContainer,
        Either
          .catchNonFatal(blocking(docker.startContainerCmd(containerId).exec()))
          .leftMap(new ContractExecutionException(s"Can't start container '$containerId'", _))
      )
    } yield containerId
  }

  def executeRunScript(containerId: String,
                       params: Map[String, String],
                       metrics: ContractExecutionMetrics): Either[ContractExecutionException, (Int, String)] = {
    val paramsString = envMapToString(params)
    metrics.measureEither(RunInContainer, executeBashCommandInContainer(containerId, s"$paramsString /run.sh", privileged = false))
  }

  def removeContainer(containerId: String): Unit =
    Try {
      if (containersIdSet.remove(containerId)) {
        log.trace(s"Removing the container '$containerId'")
        blocking(docker.removeContainerCmd(containerId).withForce(true).exec())
      }
    } fold (e => log.warn(s"Can't remove container '$containerId': ${e.getMessage}"), _ => ())

  private def resolveNodeNetworkName(): Either[ContractExecutionException, String] = {
    (for {
      nodeContainerId <- readContainerHostnameMapping().attempt
        .apply()
        .map(_.hostname)
        .leftMap(_ -> "Can't resolve node container id from /etc/hosts file")
      networkName <- Either
        .catchNonFatal {
          blocking(docker.inspectContainerCmd(nodeContainerId).exec().getNetworkSettings.getNetworks.keySet().iterator().next())
        }
        .leftMap(_ -> s"Can't resolve node network name for container '$nodeContainerId'")
    } yield networkName).leftMap(ContractExecutionException.apply)
  }

  private def extractImageRegistryAuth(imageName: DockerImageName): Option[AuthConfig] = {
    imageName.domain.flatMap { domain =>
      dockerEngineSettings.registriesAuthByAddress.get(domain)
    }
  }

  def stopAllRunningContainers(): Unit = {
    containersIdSet.forEach(removeContainer(_))
  }
}
