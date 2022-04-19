package com.wavesenterprise.api.http.auth

import com.wavesenterprise.account.Address
import enumeratum.values._
import play.api.libs.functional.syntax._
import play.api.libs.json.{JsPath, Json, Reads}

import scala.collection.immutable

case class JwtContent(roles: Set[AuthRole], addresses: Set[AddressEntry])

object JwtContent {
  implicit val jwtContentReads: Reads[JwtContent] =
    (
      (JsPath \ "roles").read[Set[Option[AuthRole]]].map(_.flatten) and
        (JsPath \ "addresses").readWithDefault[Set[Option[AddressEntry]]](Set.empty).map(_.flatten)
    )(JwtContent.apply _)
}

case class AddressEntry(address: Address, `type`: String = AddressEntry.NodeType)

object AddressEntry {

  val NodeType = "node"

  implicit val optionReads: Reads[Option[AddressEntry]] = Reads.optionNoError(Json.reads[AddressEntry])
}

sealed abstract class AuthRole(val value: String) extends StringEnumEntry

case object AuthRole extends StringEnum[AuthRole] with StringPlayJsonValueEnum[AuthRole] {

  case object User          extends AuthRole(value = "user")
  case object PrivacyUser   extends AuthRole(value = "privacy")
  case object Administrator extends AuthRole(value = "admin")

  implicit val optionReads: Reads[Option[AuthRole]] = Reads.optionNoError

  val values: immutable.IndexedSeq[AuthRole] = findValues
}
