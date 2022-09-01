package com.wavesenterprise.api.http

import com.wavesenterprise.network.peers.PeerInfo
import com.wavesenterprise.state.Sponsorship
import com.wavesenterprise.transaction.validation.FeeCalculator.{FeeHolder, FeeInAsset, FeeInNatives}
import play.api.libs.json.{JsValue, Json}

/**
  * Type class for jsonification of objects, that are handled out through REST API
  */
trait Jsonify[T] {
  def json(obj: T): JsValue
}

object Jsonify {
  val feeHolderJsonify: Jsonify[FeeHolder] = {
    case FeeInNatives(minFeeAmount) =>
      Json.obj(
        "feeAssetId" -> None,
        "feeAmount"  -> minFeeAmount
      )

    case FeeInAsset(assetId, assetDescription, minWestAmount) =>
      val maybeMinAssetAmount = if (assetDescription.sponsorshipIsEnabled) Some(Sponsorship.fromWest(minWestAmount)) else None
      Json.obj(
        "feeAssetId" -> assetId,
        "feeAmount"  -> maybeMinAssetAmount
      )
  }

  val peerInfoJsonify: Jsonify[PeerInfo] = { peerInfo =>
    Json.obj(
      "address"              -> peerInfo.remoteAddress.toString,
      "declaredAddress"      -> peerInfo.declaredAddress.fold("N/A")(_.toString),
      "nodeOwnerAddress"     -> peerInfo.nodeOwnerAddress.stringRepr,
      "peerName"             -> peerInfo.nodeName,
      "peerNonce"            -> peerInfo.nodeNonce,
      "applicationVersion"   -> peerInfo.nodeVersion.asFlatString,
      "applicationConsensus" -> s"${peerInfo.applicationConsensus}"
    )
  }
}
