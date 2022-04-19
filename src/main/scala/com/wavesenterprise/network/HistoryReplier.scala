package com.wavesenterprise.network

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.common.util.concurrent.UncheckedExecutionException
import com.wavesenterprise.network.HistoryReplier.{BlockNotFoundException, CacheSizes}
import com.wavesenterprise.network.MicroBlockLoader.MicroBlockSignature
import com.wavesenterprise.network.handshake.SignedHandshake
import com.wavesenterprise.network.message.MessageSpec.{BlockSpec, MicroBlockResponseSpec, MissingBlockSpec}
import com.wavesenterprise.settings.SynchronizationSettings
import com.wavesenterprise.state.{ByteStr, NG}
import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import monix.eval.Task
import monix.execution.schedulers.SchedulerService

/**
  * History request handler. Generates responses with signatures, blocks, micro-blocks and scores.
  */
@Sharable
class HistoryReplier(
    ng: NG,
    microBlockLoaderStorage: MicroBlockLoaderStorage,
    synchronizationSettings: SynchronizationSettings
)(implicit scheduler: SchedulerService)
    extends ChannelInboundHandlerAdapter
    with ScorexLogging {

  private val historyReplierSettings = synchronizationSettings.historyReplier

  private val knownMicroBlocks: LoadingCache[MicroBlockSignature, Array[Byte]] = CacheBuilder
    .newBuilder()
    .maximumSize(historyReplierSettings.maxMicroBlockCacheSize)
    .build(new CacheLoader[MicroBlockSignature, Array[Byte]] {
      override def load(key: MicroBlockSignature): Array[Byte] = {
        (microBlockLoaderStorage.findMicroBlockByTotalSign(key) orElse ng.microBlock(key))
          .map(microBlock => MicroBlockResponseSpec.serializeData(MicroBlockResponse(microBlock)))
          .get
      }
    })

  private val knownBlocks: LoadingCache[ByteStr, Array[Byte]] = CacheBuilder
    .newBuilder()
    .maximumSize(historyReplierSettings.maxBlockCacheSize)
    .build(new CacheLoader[ByteStr, Array[Byte]] {
      override def load(key: ByteStr): Array[Byte] = {
        ng.blockBytes(key) match {
          case Some(blockBytes) => blockBytes
          case _                => throw new BlockNotFoundException(key)
        }
      }
    })

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit = msg match {
    case GetNewSignatures(oldSignatures) =>
      Task {
        val nextIds = oldSignatures.view
          .map(id => id -> ng.blockIdsAfter(id, synchronizationSettings.maxChainLength))
          .collectFirst { case (parent, Some(ids)) => parent +: ids }

        nextIds match {
          case Some(extension) =>
            log.debug(
              s"Got GetSignatures request from '${id(ctx)}' with '${oldSignatures.length}' old signatures," +
                s"found common parent '${extension.head}' and sending total of '${extension.length}' signatures")
            ctx.writeAndFlush(Signatures(extension))
          case None =>
            log.debug(
              s"Got GetSignatures request from '${id(ctx)}' with '${oldSignatures.length}' old signatures, but could not find a common parent")
        }
      }.runAsyncLogErr

    case GetBlock(signature) =>
      Task(knownBlocks.get(signature))
        .map { bytes =>
          log.debug(s"Sent requested block '${signature.trim}' to '${id(ctx)}'")
          ctx.writeAndFlush(RawBytes(BlockSpec.messageCode, bytes))
        }
        .void
        .onErrorHandleWith(handleBlockNotFoundException(_, ctx))
        .executeAsync
        .runToFuture

    case GetBlocks(signatures) =>
      Task
        .traverse(signatures) { signature =>
          Task(knownBlocks.get(signature)).map { bytes =>
            log.trace(s"Sent requested block '${signature.trim}' to '${id(ctx)}'")
            ctx.write(RawBytes(BlockSpec.messageCode, bytes))
          }
        }
        .void
        .onErrorHandleWith(handleBlockNotFoundException(_, ctx))
        .flatMap(_ => Task(ctx.flush()))
        .foreachL(_ => log.debug(s"Sent requested blocks ${formatSignatures(signatures)} to '${id(ctx)}'"))
        .executeAsync
        .runToFuture

    case MicroBlockRequest(totalResBlockSig) =>
      Task(knownMicroBlocks.get(totalResBlockSig))
        .map { bytes =>
          ctx.writeAndFlush(RawBytes(MicroBlockResponseSpec.messageCode, bytes))
          log.trace(s"Sent requested micro-block '${totalResBlockSig.trim}' to '${id(ctx)}'")
        }
        .logErrDiscardNoSuchElementException
        .executeAsync
        .runToFuture

    case _: SignedHandshake =>
      Task {
        if (ctx.channel().isOpen) ctx.writeAndFlush(LocalScoreChanged(ng.score))
      }.runAsyncLogErr

    case _ =>
      super.channelRead(ctx, msg)
  }

  def cacheSizes: CacheSizes = CacheSizes(knownBlocks.size, knownMicroBlocks.size)

  private def handleBlockNotFoundException(t: Throwable, ctx: ChannelHandlerContext): Task[Unit] = Task.defer {
    t match {
      case uee: UncheckedExecutionException =>
        Option(uee.getCause) match {
          case Some(bnfe: BlockNotFoundException) =>
            if (ctx.channel().hasAttr(Attributes.HistoryReplierExtensionV1Attribute)) {
              Task {
                log.debug(s"Sent missing block signature '${bnfe.signature.trim}' to '${id(ctx)}'")
                ctx.write(RawBytes(MissingBlockSpec.messageCode, bnfe.signature.arr))
              }
            } else {
              log.warn(s"Requested block '${bnfe.signature.trim}' not found", bnfe)
              Task.raiseError(uee)
            }
          case _ =>
            log.error("Error executing task", uee)
            Task.raiseError(uee)
        }
      case ex =>
        log.error("Error executing task", ex)
        Task.raiseError(ex)
    }
  }
}

object HistoryReplier {
  case class CacheSizes(blocks: Long, microBlocks: Long)
  class BlockNotFoundException(val signature: ByteStr) extends RuntimeException
}
