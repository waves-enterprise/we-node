package com.wavesenterprise.block

import com.wavesenterprise.api.http.Jsonify
import com.wavesenterprise.utils.Base58
import play.api.libs.json.{JsArray, JsNumber, JsObject, Json}

object BlockHeaderJsonify extends Jsonify[BlockHeader] {

  override def json(bh: BlockHeader): JsObject = {
    Json.obj(
      "version"   -> bh.version.toInt,
      "timestamp" -> bh.timestamp,
      "reference" -> Base58.encode(bh.reference.arr),
      "generator" -> bh.signerData.generator.toString,
      "signature" -> bh.signerData.signature.base58
    ) ++
      bh.consensusData.json ++
      featuresJson(bh) ++
      bh.genesisDataOpt.fold(JsObject.empty)(Json.toJsObject)
  }

  private def featuresJson(bh: BlockHeader): JsObject = {
    bh.version match {
      case v if v < 3 => JsObject.empty
      case _          => Json.obj("features" -> JsArray(bh.featureVotes.map(id => JsNumber(id.toInt)).toSeq))
    }
  }
}
