package com.wavesenterprise.docker

import cats.implicits._
import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.core.{DefaultDockerClientConfig, DockerClientImpl}
import com.wavesenterprise.settings.dockerengine.DockerEngineSettings
import com.github.dockerjava.jaxrs.JerseyDockerHttpClient
import com.wavesenterprise.docker.DockerEngine.BasicAuthenticationRequestFilter

object DockerClientBuilder {
  def createDockerClient(settings: DockerEngineSettings): Either[Throwable, DockerClient] = Either.catchNonFatal {
    val configBuilder = DefaultDockerClientConfig.createDefaultConfigBuilder
    val clientBuilder = new JerseyDockerHttpClient.Builder()

    settings.dockerHost.foreach(dockerHost => configBuilder.withDockerHost(dockerHost.toString))
    settings.dockerAuth.foreach(dockerAuth => clientBuilder.clientRequestFilters(Array(new BasicAuthenticationRequestFilter(dockerAuth))))

    val config           = configBuilder.build()
    val dockerHttpClient = clientBuilder.dockerHost(config.getDockerHost).sslConfig(config.getSSLConfig).build()

    DockerClientImpl.getInstance(config, dockerHttpClient)
  }
}
