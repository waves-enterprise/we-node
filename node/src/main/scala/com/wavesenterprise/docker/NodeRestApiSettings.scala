package com.wavesenterprise.docker

import cats.implicits._
import com.wavesenterprise.docker.DockerEngine.readContainerHostnameMapping
import com.wavesenterprise.settings.dockerengine.DockerEngineSettings
import okhttp3.HttpUrl

case class NodeRestApiSettings(
    node: String,
    restApiPort: Int,
    nodeRestAPI: String,
)

object NodeRestApiSettings {

  private def apply(url: HttpUrl): NodeRestApiSettings =
    NodeRestApiSettings(url.host, url.port, url.toString)

  def createApiSettings(
      localDockerHostResolver: LocalDockerHostResolver,
      dockerEngineSettings: DockerEngineSettings,
      restAPIPort: Int,
  ): Either[Throwable, NodeRestApiSettings] =
    dockerEngineSettings.nodeRestApi match {
      case Some(nodeRestApi) => NodeRestApiSettings(nodeRestApi).asRight
      case None              => resolveNodeRestApiSettings(localDockerHostResolver, dockerEngineSettings, restAPIPort)
    }

  private def resolveNodeRestApiSettings(
      localDockerHostResolver: LocalDockerHostResolver,
      dockerEngineSettings: DockerEngineSettings,
      restAPIPort: Int,
  ): Either[Throwable, NodeRestApiSettings] =
    if (dockerEngineSettings.useNodeDockerHost) {
      resolveForNodeDockerHost(restAPIPort)
    } else {
      val host = localDockerHostResolver.localDockerHost()
      NodeRestApiSettings(
        node = host,
        restAPIPort,
        nodeRestAPI = s"http://$host:$restAPIPort",
      ).asRight
    }

  private def resolveForNodeDockerHost(restAPIPort: Int): Either[Throwable, NodeRestApiSettings] =
    (for {
      hostnameMapping <- readContainerHostnameMapping().attempt.apply()
      nodeIP = hostnameMapping.ip
    } yield NodeRestApiSettings(nodeIP, restAPIPort, s"http://$nodeIP:$restAPIPort")).leftMap(e =>
      new RuntimeException("Can't resolve node IP address", e))
}
