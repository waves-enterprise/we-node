package com.wavesenterprise.api.grpc.service

import akka.grpc.scaladsl.Metadata
import com.google.protobuf.empty.Empty
import com.wavesenterprise.account.{Address, PublicKeyAccount}
import com.wavesenterprise.api.grpc.auth.GrpcAuth
import com.wavesenterprise.http.NodeApiRoute.NodeConfigRawResponse
import com.wavesenterprise.protobuf.service.util.{AddressWithPubKeyResponse, NodeConfigResponse, NodeInfoServicePowerApi}
import com.wavesenterprise.settings.{AuthorizationSettings, WESettings}
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.utils.Time
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.Future

class NodeInfoServiceImpl(val time: Time, nodeSetting: WESettings, blockchain: Blockchain, ownerPublicAccount: PublicKeyAccount)(
    implicit val s: Scheduler)
    extends NodeInfoServicePowerApi
    with GrpcAuth {
  override val authSettings: AuthorizationSettings = nodeSetting.api.auth
  override val nodeOwner: Address                  = ownerPublicAccount.toAddress

  override def nodeConfig(request: Empty, metadata: Metadata): Future[NodeConfigResponse] =
    withAuthTask(metadata) {
      Task {
        NodeConfigRawResponse.build(nodeSetting, blockchain).toProto
      }
    }.runToFuture

  override def nodeOwner(in: Empty, metadata: Metadata): Future[AddressWithPubKeyResponse] =
    withAuthTask(metadata) {
      Task {
        AddressWithPubKeyResponse(ownerPublicAccount.address, ownerPublicAccount.publicKeyBase58)
      }
    }.runToFuture
}
