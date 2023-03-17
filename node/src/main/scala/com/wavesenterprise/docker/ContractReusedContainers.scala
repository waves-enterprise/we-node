package com.wavesenterprise.docker

import cats.data.OptionT
import com.google.common.cache.{Cache, CacheBuilder, RemovalNotification}
import com.wavesenterprise.docker.ContractExecutor.{ContainerKey, StartContractSetup}
import com.wavesenterprise.utils.ScorexLogging
import monix.eval.Task
import monix.execution.{CancelableFuture, Scheduler}

import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

class ContractReusedContainers(removeContainerAfter: FiniteDuration)(implicit val scheduler: Scheduler) extends ScorexLogging with AutoCloseable {

  private[this] val startedContainers: Cache[ContainerKey, ReusedContainer] = CacheBuilder
    .newBuilder()
    .expireAfterAccess(removeContainerAfter.toMillis, TimeUnit.MILLISECONDS)
    .removalListener((notification: RemovalNotification[ContainerKey, ReusedContainer]) => {
      val containerKey    = notification.getKey
      val reusedContainer = notification.getValue
      reusedContainer.future.foreach(containerId => reusedContainer.onInvalidate(containerKey, containerId))
    })
    .build()

  private[this] val cancelable = scheduler.scheduleWithFixedDelay(10, 10, TimeUnit.MINUTES, () => startedContainers.cleanUp())

  def isStarted(containerKey: ContainerKey): Boolean = {
    Option(startedContainers.getIfPresent(containerKey)).exists(_.isContractStarted)
  }

  def getStarted(containerKey: ContainerKey): Task[String] = Task.defer {
    OptionT
      .fromOption[Task](Option(startedContainers.getIfPresent(containerKey)))
      .filter(_.isContractStarted)
      .map(_.future)
      .semiflatMap(Task.fromFuture)
      .getOrElseF(Task.raiseError(new RuntimeException(s"Started container is not found for '$containerKey'")))
  }

  def startOrReuse(setup: StartContractSetup): Future[String] = {
    val StartContractSetup(containerKey, startContractTask, onInvalidate) = setup
    startedContainers
      .get(
        containerKey,
        () => {
          ReusedContainer(
            startTask = startContractTask.onErrorHandleWith(onStartContractError(containerKey)),
            onInvalidate = onInvalidate
          )
        }
      )
      .future
  }

  private def onStartContractError(containerKey: ContainerKey)(t: Throwable): Task[String] = {
    Task(invalidate(containerKey)) >> Task.raiseError(t)
  }

  def invalidate(containerKey: ContainerKey): Unit = {
    log.debug(s"Invalidating started container cache for image '${containerKey.image}'")
    startedContainers.invalidate(containerKey)
  }

  override def close(): Unit = {
    cancelable.cancel()
  }
}

private case class ReusedContainer(startTask: Task[String], onInvalidate: (ContainerKey, String) => Unit)(implicit val scheduler: Scheduler) {
  lazy val future: CancelableFuture[String] = startTask.executeAsync.runToFuture

  def isContractStarted: Boolean = future.value.exists(_.isSuccess)
}
