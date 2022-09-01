package com.wavesenterprise.network

import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageCodec

import java.util
import scala.util.{Failure, Success}

@Sharable
class MessageCodec() extends MessageToMessageCodec[RawBytes, Message] with ScorexLogging {

  import com.wavesenterprise.network.message.MessageSpec._

  override def encode(ctx: ChannelHandlerContext, msg: Message, out: util.List[AnyRef]): Unit = msg match {
    // Have no spec
    case r: RawBytes              => out.add(r)
    case LocalScoreChanged(score) => out.add(RawBytes(ScoreSpec.messageCode, ScoreSpec.serializeData(score)))
    case BlockForged(b)           => out.add(RawBytes(BlockSpec.messageCode, b.bytes()))

    // With a spec
    case GetPeersV2               => out.add(RawBytes(GetPeersV2Spec.messageCode, GetPeersV2Spec.serializeData(GetPeersV2)))
    case kv2: KnownPeersV2        => out.add(RawBytes(PeersV2Spec.messageCode, PeersV2Spec.serializeData(kv2)))
    case gs: GetNewSignatures     => out.add(RawBytes(GetSignaturesSpec.messageCode, GetSignaturesSpec.serializeData(gs)))
    case s: Signatures            => out.add(RawBytes(SignaturesSpec.messageCode, SignaturesSpec.serializeData(s)))
    case g: GetBlocks             => out.add(RawBytes(GetBlocksSpec.messageCode, GetBlocksSpec.serializeData(g)))
    case m: MicroBlockInventoryV1 => out.add(RawBytes(MicroBlockInventoryV1Spec.messageCode, MicroBlockInventoryV1Spec.serializeData(m)))
    case m: MicroBlockInventoryV2 => out.add(RawBytes(MicroBlockInventoryV2Spec.messageCode, MicroBlockInventoryV2Spec.serializeData(m)))
    case m: MicroBlockRequest     => out.add(RawBytes(MicroBlockRequestSpec.messageCode, MicroBlockRequestSpec.serializeData(m)))
    case m: MicroBlockResponseV1  => out.add(RawBytes(MicroBlockResponseV1Spec.messageCode, MicroBlockResponseV1Spec.serializeData(m)))
    case m: MicroBlockResponseV2  => out.add(RawBytes(MicroBlockResponseV2Spec.messageCode, MicroBlockResponseV2Spec.serializeData(m)))
    case m: NetworkContractExecutionMessage =>
      out.add(RawBytes(NetworkContractExecutionMessageSpec.messageCode, NetworkContractExecutionMessageSpec.serializeData(m)))
    case m: ContractValidatorResults => out.add(RawBytes(ContractValidatorResultsSpec.messageCode, ContractValidatorResultsSpec.serializeData(m)))
    case m: PrivateDataRequest       => out.add(RawBytes(PrivateDataRequestSpec.messageCode, PrivateDataRequestSpec.serializeData(m)))
    case m: PrivateDataResponse      => out.add(RawBytes(PrivateDataResponseSpec.messageCode, PrivateDataResponseSpec.serializeData(m)))
    case v: VoteMessage              => out.add(RawBytes(VoteMessageSpec.messageCode, VoteMessageSpec.serializeData(v)))
    case m: SnapshotNotification     => out.add(RawBytes(SnapshotNotificationSpec.messageCode, SnapshotNotificationSpec.serializeData(m)))
    case m: SnapshotRequest          => out.add(RawBytes(SnapshotRequestSpec.messageCode, SnapshotRequestSpec.serializeData(m)))
    case m: GenesisSnapshotRequest   => out.add(RawBytes(GenesisSnapshotRequestSpec.messageCode, GenesisSnapshotRequestSpec.serializeData(m)))
    case m: GenesisSnapshotError     => out.add(RawBytes(GenesisSnapshotErrorSpec.messageCode, GenesisSnapshotErrorSpec.serializeData(m)))
    case i: PrivacyInventoryV1       => out.add(RawBytes(PrivacyInventoryV1Spec.messageCode, PrivacyInventoryV1Spec.serializeData(i)))
    case i: PrivacyInventoryV2       => out.add(RawBytes(PrivacyInventoryV2Spec.messageCode, PrivacyInventoryV2Spec.serializeData(i)))
    case r: PrivacyInventoryRequest  => out.add(RawBytes(PrivacyInventoryRequestSpec.messageCode, PrivacyInventoryRequestSpec.serializeData(r)))
    case m: MissingBlock             => out.add(RawBytes(MissingBlockSpec.messageCode, MissingBlockSpec.serializeData(m)))
    case a: RawAttributes            => out.add(RawBytes(RawAttributesSpec.messageCode, RawAttributesSpec.serializeData(a)))
    case b: HistoryBlock             => out.add(RawBytes(HistoryBlockSpec.messageCode, HistoryBlockSpec.serializeData(b)))
    case t: BroadcastedTransaction   => out.add(RawBytes(BroadcastedTransactionSpec.messageCode, BroadcastedTransactionSpec.serializeData(t)))
  }

  override def decode(ctx: ChannelHandlerContext, msg: RawBytes, out: util.List[AnyRef]): Unit = {
    (specsByCodes(msg.code) match {
      case PrivateDataResponseSpec => PrivateDataResponseSpec.deserializeData(msg.data, !ctx.channel().hasAttr(Attributes.TlsAttribute))
      case otherSpec               => otherSpec.deserializeData(msg.data)
    }) match {
      case Success(x) => out.add(x)
      case Failure(e) =>
        log.error("Message decoding failed", e)
        block(ctx, e)
    }
  }

  protected def block(ctx: ChannelHandlerContext, e: Throwable): Unit = {
    closeChannel(ctx.channel(), s"Invalid message. ${e.getMessage}")
  }

}
