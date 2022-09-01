package com.wavesenterprise.docker.grpc

import cats.implicits._
import com.wavesenterprise.docker.DockerEngine.readContainerHostnameMapping
import com.wavesenterprise.docker.LocalDockerHostResolver
import com.wavesenterprise.settings.dockerengine.DockerEngineSettings

case class NodeGrpcApiSettings(
    node: String,
    grpcApiPort: Int
)

object NodeGrpcApiSettings {

  def createApiSettings(
      localDockerHostResolver: LocalDockerHostResolver,
      dockerEngineSettings: DockerEngineSettings
  ): Either[Throwable, NodeGrpcApiSettings] = {
    dockerEngineSettings.grpcServer.host match {
      case Some(node) => NodeGrpcApiSettings(node, dockerEngineSettings.grpcServer.port).asRight
      case None       => resolveNodeGrpcApiSettings(localDockerHostResolver, dockerEngineSettings)
    }
  }

  private def resolveNodeGrpcApiSettings(
      localDockerHostResolver: LocalDockerHostResolver,
      dockerEngineSettings: DockerEngineSettings,
  ): Either[Throwable, NodeGrpcApiSettings] = {
    val grpcApiPort = dockerEngineSettings.grpcServer.port
    if (dockerEngineSettings.useNodeDockerHost) {
      resolveForNodeDockerHost(grpcApiPort)
    } else {
      NodeGrpcApiSettings(localDockerHostResolver.localDockerHost(), grpcApiPort).asRight
    }
  }

  private def resolveForNodeDockerHost(grpcApiPort: Int): Either[Throwable, NodeGrpcApiSettings] = {
    (for {
      hostnameMapping <- readContainerHostnameMapping().attempt.apply()
      nodeIP = hostnameMapping.ip
    } yield NodeGrpcApiSettings(nodeIP, grpcApiPort)).leftMap(e => new RuntimeException("Can't resolve node IP address", e))
  }
}
