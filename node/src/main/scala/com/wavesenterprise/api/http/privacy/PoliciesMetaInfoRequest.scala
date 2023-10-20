package com.wavesenterprise.api.http.privacy

import com.wavesenterprise.privacy.PolicyItemInfo
import play.api.libs.json.{Json, OFormat}

case class PoliciesMetaInfoRequest(policiesDataHashes: List[PolicyIdWithDataHash])

case class PolicyIdWithDataHash(policyId: String, datahashes: Set[String])

object PolicyIdWithDataHash {
  implicit val policyIdWithDataHashFormat: OFormat[PolicyIdWithDataHash] = Json.format
}

object PoliciesMetaInfoRequest {
  implicit val policiesMetaInfoRequestFormat: OFormat[PoliciesMetaInfoRequest] = Json.format
}

case class PoliciesMetaInfoResponse(policiesDataInfo: Seq[PolicyDatasInfo])

object PoliciesMetaInfoResponse {
  implicit val policiesMetaInfoResponseFormat: OFormat[PoliciesMetaInfoResponse] = Json.format
}

case class PolicyDatasInfo(policyId: String, datasInfo: Seq[PolicyItemInfo])

object PolicyDatasInfo {
  implicit val policyDatasInfoFormat: OFormat[PolicyDatasInfo] = Json.format
}
