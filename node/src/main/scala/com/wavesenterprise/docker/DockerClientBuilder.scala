package com.wavesenterprise.docker

import cats.implicits._
import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.core.{DefaultDockerClientConfig, DockerClientImpl}
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient
import com.wavesenterprise.docker.DockerEngine.BasicAuthenticationRequestFilter
import com.wavesenterprise.settings.dockerengine.DockerEngineSettings

object DockerClientBuilder {
  def createDockerClient(settings: DockerEngineSettings): Either[Throwable, DockerClient] = Either.catchNonFatal {
    val configBuilder = DefaultDockerClientConfig.createDefaultConfigBuilder
    val clientBuilder = new ApacheDockerHttpClient.Builder()

    settings.dockerHost.foreach(dockerHost => configBuilder.withDockerHost(dockerHost.toString))
    settings.dockerAuth.foreach(dockerAuth => {
      configBuilder.withRegistryUsername(dockerAuth.username)
      configBuilder.withRegistryUsername(dockerAuth.password)
    })

    val config           = configBuilder.build()
    val dockerHttpClient = clientBuilder.dockerHost(config.getDockerHost).sslConfig(config.getSSLConfig).build()

    DockerClientImpl.getInstance(config, dockerHttpClient)
  }

  def createDefaultApacheDockerClient() = {
    val configBuilder = DefaultDockerClientConfig.createDefaultConfigBuilder
    val clientBuilder = new ApacheDockerHttpClient.Builder()

    val config           = configBuilder.build()
    val dockerHttpClient = clientBuilder.dockerHost(config.getDockerHost).sslConfig(config.getSSLConfig).build()

    DockerClientImpl.getInstance(config, dockerHttpClient)
  }

}
