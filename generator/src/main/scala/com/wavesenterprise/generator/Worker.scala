package com.wavesenterprise.generator

import com.wavesenterprise.generator.config.WorkerMode

import java.net.{InetSocketAddress, URL}
import com.wavesenterprise.network.RawBytes
import com.wavesenterprise.network.client.NetworkSender
import com.wavesenterprise.transaction.Transaction
import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel.Channel
import monix.eval.Task
import monix.execution.Scheduler
import org.asynchttpclient.AsyncHttpClient
import play.api.libs.json.Json

import java.net.{InetSocketAddress, URL}
import scala.compat.java8.FutureConverters
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class Worker(settings: WorkerSettings,
             transactionSource: Iterator[Transaction],
             networkSender: NetworkSender,
             node: InetSocketAddress,
             nodeRestAddress: URL,
             currentMode: () => WorkerMode,
             initial: Iterator[Transaction],
             validationAfterInit: Seq[Transaction] => Task[_])(implicit httpClient: AsyncHttpClient, ec: ExecutionContext)
    extends ScorexLogging {

  def run(): Future[Unit] =
    pullAndWriteTask().map(_ => ()).runAsyncLogErr(Scheduler(ec))

  private[this] def pullAndWriteTask(channel: Option[Channel] = None): Task[Option[Channel]] = {
    currentMode() match {
      case WorkerMode.Stop   => Task.now(None)
      case WorkerMode.Freeze => Task.defer(pullAndWriteTask(channel)).delayExecution(5.seconds)
      case WorkerMode.Continue =>
        val nodeUTXTransactionsCount: Task[Int] = Task.defer {
          import org.asynchttpclient.Dsl._
          val request = get(s"$nodeRestAddress/transactions/unconfirmed/size").build()
          Task
            .fromFuture(FutureConverters.toScala(httpClient.executeRequest(request).toCompletableFuture))
            .map(r => (Json.parse(r.getResponseBody) \ "size").as[Int])
        }

        def writeInitTransactions(channel: Channel, initTxs: Seq[Transaction]): Task[Unit] =
          if (initTxs.isEmpty) {
            Task.unit
          } else {
            for {
              _ <- Task.eval(log.info(s"Sending ${initTxs.size} initial(s) to $channel"))
              _ <- Task.fromFuture(networkSender.send(channel, initTxs.map(RawBytes.from): _*))
              _ <- Task.eval(log.info(s"Validating after initials"))
              _ <- validationAfterInit(initTxs)
              _ <- Task.eval(log.info(s"Sent '${initTxs.size}' to '$channel'"))
            } yield ()
          }

        def writeTransactions(channel: Channel, txs: Seq[Transaction]): Task[Unit] =
          if (txs.isEmpty) {
            Task.unit
          } else {
            for {
              _ <- Task.eval(log.info(s"Sending ${txs.size} to $channel"))
              _ <- Task.fromFuture(networkSender.send(channel, txs.map(RawBytes.from): _*))
              _ <- Task.eval(log.info(s"Sent '${txs.size}' to '$channel'"))
            } yield ()
          }

        def writeAllTransactions(channel: Channel, initTxs: Seq[Transaction], txs: Seq[Transaction]): Task[Unit] =
          writeInitTransactions(channel, initTxs) >> writeTransactions(channel, txs)

        val baseTask = for {
          validChannel <- Task.defer(if (channel.exists(_.isOpen)) Task.now(channel.get) else Task.fromFuture(networkSender.connect(node)))
          txCount      <- nodeUTXTransactionsCount
          (initTxs, txs) = {
            val availableUtxCount = (settings.utxLimit - txCount).max(0)
            val initTxs           = initial.take(availableUtxCount).toStream
            val txs               = transactionSource.take(availableUtxCount - initTxs.size).toStream
            initTxs -> txs
          }
          _ <- if (initTxs.nonEmpty || txs.nonEmpty) writeAllTransactions(validChannel, initTxs, txs) else Task.unit
        } yield Option(validChannel)

        val withReconnect = baseTask.onErrorRecoverWith {
          case error =>
            channel.foreach(_.close())

            if (settings.autoReconnect) {
              log.error(s"[$node] An error during sending transations, reconnect", error)
              for {
                _       <- Task.sleep(settings.reconnectDelay)
                channel <- pullAndWriteTask()
              } yield channel
            } else {
              log.error("Stopping because autoReconnect is disabled", error)
              Task.raiseError(error)
            }
        }

        for {
          channel    <- withReconnect
          _          <- Task(log.info(s"Sleeping for ${settings.delay}"))
          _          <- Task.sleep(settings.delay)
          newChannel <- pullAndWriteTask(channel)
        } yield newChannel
    }
  }
}
