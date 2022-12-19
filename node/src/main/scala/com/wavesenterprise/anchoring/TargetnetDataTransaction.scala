package com.wavesenterprise.anchoring

import com.wavesenterprise.account.{Address, PublicKeyAccount}
import com.wavesenterprise.crypto.internals.{CryptoAlgorithms, KeyPair, WavesAlgorithms, WavesPrivateKey, WavesPublicKey}
import com.wavesenterprise.state.{ByteStr, DataEntry}
import com.wavesenterprise.transaction.{DataTransactionV1, Proofs, ValidationError}
import monix.eval.Coeval
import play.api.libs.json.{JsArray, JsObject, JsString, Json}

case class TargetnetDataTransaction private (dataTransaction: DataTransactionV1, id: ByteStr, proofs: Proofs, chainId: Byte) {

  val json: Coeval[JsObject] = Coeval.evalOnce {
    val originJson = jsonForAnchoring(chainId, WavesAlgorithms)
    val proofs     = Seq("proofs" -> JsArray(this.proofs.proofs.map(p => JsString(p.base58))))
    originJson ++ JsObject(Map("id" -> JsString(id.base58))) ++ JsObject(proofs)
  }

  private def jsonForAnchoring(chainId: Byte, cryptoAlgorithms: CryptoAlgorithms[_ <: KeyPair]): JsObject = {
    // do not use `jsonBase()` here, because inside it calls package object `crypto` which use project global cryptoAlgorithms
    Json.obj(
      "type"            -> dataTransaction.builder.typeId,
      "id"              -> dataTransaction.id().base58,
      "sender"          -> Address.fromPublicKey(dataTransaction.sender.publicKey.getEncoded, chainId, cryptoAlgorithms).address,
      "senderPublicKey" -> dataTransaction.sender.publicKeyBase58,
      "fee"             -> dataTransaction.fee,
      "timestamp"       -> dataTransaction.timestamp
    ) ++
      dataTransaction.proofField ++
      Json.obj(
        "version"         -> dataTransaction.version,
        "authorPublicKey" -> dataTransaction.author.publicKeyBase58,
        "author"          -> Address.fromPublicKey(dataTransaction.author.publicKey.getEncoded, chainId, cryptoAlgorithms).address,
        "data"            -> Json.toJson(dataTransaction.data),
      )
  }
}

object TargetnetDataTransaction {
  def create(sender: WavesPublicKey,
             author: WavesPublicKey,
             data: List[DataEntry[_]],
             feeAmount: Long,
             timestamp: Long,
             wavesPrivateKey: WavesPrivateKey,
             chainId: Byte): Either[ValidationError, TargetnetDataTransaction] = {
    for {
      dataTx <-
        DataTransactionV1.create(PublicKeyAccount(sender.getEncoded), PublicKeyAccount(author.getEncoded), data, timestamp, feeAmount, Proofs.empty)
      txBodyBytes = dataTx.bodyBytes()
      signature   = WavesAlgorithms.sign(wavesPrivateKey, txBodyBytes)
      createdProofs <- Proofs.create(Seq(ByteStr(signature)))
      wavesId = ByteStr(WavesAlgorithms.fastHash(txBodyBytes))
    } yield TargetnetDataTransaction(dataTx, wavesId, createdProofs, chainId)
  }
}
