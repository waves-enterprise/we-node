package com.wavesenterprise.docker.grpc

import akka.grpc.GrpcServiceException
import cats.implicits._
import com.google.protobuf.ByteString
import com.wavesenterprise.api.grpc.utils._
import com.wavesenterprise.api.http.acl.PermissionsForAddressesReq
import com.wavesenterprise.api.http.service.PermissionApiService.{RolesForSeqResponse, RolesResponse}
import com.wavesenterprise.protobuf.service.contract._
import com.wavesenterprise.serialization.ProtoAdapter
import com.wavesenterprise.state.DataEntry
import com.wavesenterprise.transaction.PaymentsV1ToContract
import com.wavesenterprise.transaction.docker.assets.ContractAssetOperation
import com.wavesenterprise.transaction.docker.{CallContractTransaction, CreateContractTransaction, ExecutableTransaction}
import com.wavesenterprise.transaction.protobuf.{ContractAssetOperation => PbContractAssetOperation}

object ProtoObjectsMapper {

  def mapFromProto(operation: PbContractAssetOperation): Either[GrpcServiceException, ContractAssetOperation] = {
    ProtoAdapter
      .fromProto(operation)
      .leftMap(_.asGrpcServiceException)
  }

  @inline
  def mapToProto(data: DataEntry[_]): com.wavesenterprise.transaction.protobuf.DataEntry = ProtoAdapter.toProto(data)

  def mapFromProto(data: com.wavesenterprise.transaction.protobuf.DataEntry): Either[GrpcServiceException, DataEntry[_]] = {
    ProtoAdapter
      .fromProto(data)
      .leftMap(_.asGrpcServiceException)
  }

  def mapToProto(tx: ExecutableTransaction): com.wavesenterprise.protobuf.service.contract.ContractTransaction = {
    val basePb = ContractTransaction(
      id = tx.id().toString,
      `type` = tx.txType.toInt,
      sender = tx.sender.toAddress.stringRepr,
      senderPublicKey = tx.sender.publicKeyBase58,
      contractId = tx.contractId.toString,
      params = tx.params.map(mapToProto),
      fee = tx.fee,
      version = tx.version,
      proofs = ByteString.copyFrom(tx.proofs.bytes()),
      timestamp = tx.timestamp,
      feeAssetId = tx.feeAssetId.map(asset => AssetId(asset.base58)),
      data = mapTxDataToProto(tx),
    )

    tx match {
      case executableTx: ExecutableTransaction with PaymentsV1ToContract =>
        basePb.copy(
          payments = executableTx.payments.map(ProtoAdapter.toProto)
        )
      case _ =>
        basePb
    }
  }

  private def mapTxDataToProto(tx: ExecutableTransaction): ContractTransaction.Data = {
    tx match {
      case create: CreateContractTransaction =>
        ContractTransaction.Data.CreateData(
          CreateContractTransactionData(image = create.image, imageHash = create.imageHash, contractName = create.contractName))
      case call: CallContractTransaction => ContractTransaction.Data.CallData(CallContractTransactionData(contractVersion = call.contractVersion))
    }
  }

  def mapToProto(response: RolesResponse): PermissionsResponse = {
    PermissionsResponse(
      roles = response.roles.map(mapToProto),
      timestamp = response.timestamp
    )
  }

  def mapToProto(role: com.wavesenterprise.api.http.service.PermissionApiService.RoleInfo): RoleInfo = {
    RoleInfo(
      role = role.role,
      dueTimestamp = role.dueTimestamp
    )
  }

  def mapFromProto(request: AddressesPermissionsRequest): PermissionsForAddressesReq = {
    PermissionsForAddressesReq(
      addresses = request.addresses,
      timestamp = request.timestamp
    )
  }

  def mapToProto(response: RolesForSeqResponse): AddressesPermissionsResponse = {
    AddressesPermissionsResponse(
      addressToRoles = response.addressToRoles.map(mapToProto),
      timestamp = response.timestamp
    )
  }

  def mapToProto(response: com.wavesenterprise.api.http.service.PermissionApiService.RolesForAddressResponse): RolesForAddressResponse = {
    RolesForAddressResponse(
      address = response.address,
      roles = response.roles.map(mapToProto)
    )
  }

  def mapToProto(itemFileInfo: com.wavesenterprise.privacy.PolicyItemFileInfo): PolicyItemFileInfo = {
    PolicyItemFileInfo(
      filename = itemFileInfo.filename,
      size = itemFileInfo.size,
      timestamp = itemFileInfo.timestamp,
      author = itemFileInfo.author,
      comment = itemFileInfo.comment
    )
  }
}
