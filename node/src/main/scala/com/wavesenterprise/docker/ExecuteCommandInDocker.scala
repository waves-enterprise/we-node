package com.wavesenterprise.docker

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.core.command.ExecStartResultCallback
import com.github.ghik.silencer.silent
import com.wavesenterprise.utils.ScorexLogging

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.blocking

trait ExecuteCommandInDocker extends ScorexLogging {

  def docker: DockerClient

  @silent("deprecated")
  protected def executeCommandInContainer(containerId: String,
                                          command: Array[String],
                                          successCodes: Set[Int],
                                          privileged: Boolean = false): Either[ExecuteCommandException, (Int, String)] = {
    val execId = docker
      .execCreateCmd(containerId)
      .withCmd(command: _*)
      .withAttachStderr(true)
      .withAttachStdin(true)
      .withAttachStdout(true)
      .withPrivileged(privileged)
      .exec()
      .getId

    val stdout = new ByteArrayOutputStream()
    val stderr = new ByteArrayOutputStream()

    blocking(docker.execStartCmd(execId).exec(new ExecStartResultCallback(stdout, stderr)).awaitCompletion())

    val inspect  = blocking(docker.inspectExecCmd(execId).exec())
    val exitCode = inspect.getExitCodeLong.toInt
    val result   = new String(stdout.toByteArray, UTF_8)

    log.debug(s"Process was finished with code '$exitCode': $result")
    if (successCodes.contains(exitCode)) {
      val message = result
        .split("\n")
        .flatMap(_.split("\r"))
        .filter(_.nonEmpty)
        .lastOption
        .map(_.trim)
        .getOrElse("")
      Right((exitCode, message))
    } else {
      Left(new ExecuteCommandException(s"Contract process was failed with code '$exitCode': $result", exitCode))
    }
  }
}

object ExecuteCommandInDocker {
  val ProcessSuccessCode: Int = 0
}

class ExecuteCommandException(message: String, val exitCode: Int) extends RuntimeException(message)
