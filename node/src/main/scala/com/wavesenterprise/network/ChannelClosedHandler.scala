package com.wavesenterprise.network

import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel._
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.reactive.subjects.ConcurrentSubject

@Sharable
class ChannelClosedHandler private (scheduler: Scheduler) extends ChannelHandlerAdapter {

  // observable to notify outer world when channel is closing
  private val closedChannelsSubject = ConcurrentSubject.publish[Channel](scheduler)

  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    ctx.channel.closeFuture.addListener((cf: ChannelFuture) => closedChannelsSubject.onNext(cf.channel))
    super.handlerAdded(ctx)
  }

  def shutdown(): Unit = {
    closedChannelsSubject.onComplete()
  }
}

object ChannelClosedHandler {
  def apply(scheduler: Scheduler): (ChannelClosedHandler, Observable[Channel]) = {
    val h = new ChannelClosedHandler(scheduler)
    (h, h.closedChannelsSubject)
  }
}
