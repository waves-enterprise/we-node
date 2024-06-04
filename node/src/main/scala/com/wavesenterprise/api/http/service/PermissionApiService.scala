package com.wavesenterprise.api.http.service

import cats.implicits._
import com.wavesenterprise.acl.PermissionOp
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.api.http.ApiError.RequestedHeightDoesntExist
import com.wavesenterprise.api.http.ApiError.CustomValidationError
import com.wavesenterprise.api.http.acl.PermissionsForAddressesReq
import com.wavesenterprise.api.http.utils.AccountFromAddressOrAliasString
import com.wavesenterprise.state.Blockchain
import play.api.libs.json.{Format, Json}

class PermissionApiService(val blockchain: Blockchain) extends AccountFromAddressOrAliasString {

  import PermissionApiService._

  def forAddressAtTimestamp(addressStr: String, timestamp: Long): Either[ApiError, RolesResponse] = {
    (for {
      address <- accountFrom(addressStr)
      permissions         = blockchain.permissions(address)
      activePermissionOps = permissions.activeAsOps(timestamp)
      roleInfoSeq         = activePermissionOps.map(RoleInfo.fromPermissionOp).toSeq
    } yield RolesResponse(roleInfoSeq, timestamp)).leftMap(ApiError.fromValidationError)
  }

  def forAddressSeq(request: PermissionsForAddressesReq): Either[ApiError, RolesForSeqResponse] = {
    val timestamp = request.timestamp
    request.addressesOrAliases.toList
      .traverse { reqAddressOrAlias =>
        for {
          address <- accountFrom(reqAddressOrAlias)
          permissions         = blockchain.permissions(address)
          activePermissionOps = permissions.activeAsOps(timestamp)
          roleInfoSeq         = activePermissionOps.map(RoleInfo.fromPermissionOp).toSeq
        } yield RolesForAddressResponse(address.address, roleInfoSeq)
      }
      .map(addressToRoles => RolesForSeqResponse(addressToRoles, timestamp))
      .leftMap(ApiError.fromValidationError)
  }

  def addressContractValidator: Either[ApiError, RolesAddress] = {
    for {
      block <- blockchain.lastBlock.toRight[ApiError](CustomValidationError("Last block is incorrect"))
      timestamp = block.timestamp
      addresses = blockchain.contractValidators.currentValidatorSet(timestamp).map(address => address.address)
    } yield RolesAddress(addresses)
  }

  def addressContractValidator(height: Int): Either[ApiError, RolesAddress] = {
    for {
      block <- blockchain.blockAt(height).toRight[ApiError](RequestedHeightDoesntExist(height, blockchain.height))
      timestamp = block.timestamp
      addresses = blockchain.contractValidators.currentValidatorSet(timestamp).map(address => address.address)
    } yield RolesAddress(addresses)
  }
}

object PermissionApiService {

  case class RoleInfo(role: String, dueTimestamp: Option[Long])

  object RoleInfo {
    def fromPermissionOp(permOp: PermissionOp): RoleInfo =
      RoleInfo(permOp.role.prefixS, permOp.dueTimestampOpt)
  }

  implicit val roleInfoFormat: Format[RoleInfo] = Json.format

  case class RolesResponse(roles: Seq[RoleInfo], timestamp: Long)

  case class RolesForAddressResponse(address: String, roles: Seq[RoleInfo])

  case class RolesForSeqResponse(addressToRoles: List[RolesForAddressResponse], timestamp: Long)

  case class RolesAddress(addresses: Set[String])

  implicit val rolesAddress: Format[RolesAddress]                       = Json.format
  implicit val permissionsResponseFormat: Format[RolesResponse]         = Json.format
  implicit val rolesForAddressResponse: Format[RolesForAddressResponse] = Json.format
  implicit val rolesForSeqResponseFormat: Format[RolesForSeqResponse]   = Json.format
}
