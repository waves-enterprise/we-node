package com.wavesenterprise.docker

import cats.implicits._
import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.command.PullImageResultCallback
import com.github.dockerjava.api.exception.NotFoundException
import com.wavesenterprise.docker.ExecuteCommandInDocker._
import monix.eval.Coeval
import org.apache.commons.lang3.SystemUtils

import scala.concurrent.blocking

class LocalDockerHostResolver(val docker: DockerClient) extends ExecuteCommandInDocker {

  import LocalDockerHostResolver._

  val localDockerHost: Coeval[String] = Coeval.defer {
    Coeval.fromEither(resolve())
  }.memoizeOnSuccess

  private def resolve(): Either[RuntimeException, String] = {
    (for {
      _           <- pullImageIfNotFound()
      containerId <- createAndStartContainer()
      host <- executeCommandInContainer(containerId, Array[String]("/bin/sh", "-c", createCommand()), Set(ProcessSuccessCode), privileged = true)
        .map(_._2)
      _ = log.info(s"Resolved local docker host: '$host'")
      _ <- removeContainer(containerId)
    } yield host).leftMap(e =>
      new RuntimeException(
        "Can't resolve local docker host. Check 'docker-engine' configuration, probably 'node-rest-api' and 'grpc-server.host' settings are not properly configured.",
        e
      ))
  }

  private def pullImageIfNotFound(): Either[Throwable, Any] = {
    Either
      .catchNonFatal(blocking(docker.inspectImageCmd("alpine:3.8").exec()))
      .recoverWith {
        case _: NotFoundException =>
          Either.catchNonFatal {
            blocking(docker.pullImageCmd("alpine").withTag("3.8").exec(new PullImageResultCallback).awaitCompletion())
          }
      }
  }

  private def createAndStartContainer(): Either[Throwable, String] = {
    for {
      containerId <- Either.catchNonFatal {
        blocking(docker.createContainerCmd("alpine:3.8").withCmd("sleep", "9999").exec().getId)
      }
      _ <- Either.catchNonFatal {
        blocking(docker.startContainerCmd(containerId).exec())
      }
    } yield containerId
  }

  private def createCommand(): String = {
    (if (SystemUtils.IS_OS_MAC || SystemUtils.IS_OS_WINDOWS) "" else s"$ResolveDockerHostForLinuxCmd && ") + ResolveNodeHostCmd
  }

  private def removeContainer(containerId: String): Either[Throwable, Any] = {
    Either
      .catchNonFatal {
        blocking(docker.removeContainerCmd(containerId).withForce(true).exec())
      }
      .recover {
        case e: Throwable => log.error(s"Can't remove container with id '$containerId'", e)
      }
  }
}

object LocalDockerHostResolver {

  private val ResolveNodeHostCmd = "nslookup host.docker.internal 2>/dev/null | grep Address | awk '{ print $3 }'"

  private val ResolveDockerHostForLinuxCmd = "echo -e $(/sbin/ip route|awk '/default/ { print $3 }')\\\\thost.docker.internal >> /etc/hosts"
}
