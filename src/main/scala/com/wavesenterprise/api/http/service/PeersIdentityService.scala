package com.wavesenterprise.api.http.service

import com.wavesenterprise.account.{Address, PublicKeyAccount}
import com.wavesenterprise.crypto
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.utils.ScorexLogging
import scorex.crypto.signatures.Signature

class PeersIdentityService(ownerAddress: Address, blockchain: Blockchain) extends ScorexLogging {

  def getIdentity(requestAddress: Address, message: Array[Byte], signature: Signature): Either[ValidationError, PublicKeyAccount] = {
    for {
      pubKey <- blockchain.participantPubKey(requestAddress).toRight(ValidationError.ParticipantNotRegistered(requestAddress))
      verified = crypto.verify(signature, message, pubKey.publicKey.getEncoded)
      _ <- Either.cond(verified, (), ValidationError.InvalidRequestSignature)
      ownerPubKey <- blockchain.participantPubKey(ownerAddress).toRight {
        log.error(s"Node own address ${ownerAddress.stringRepr} is not registered in blockchain!")
        ValidationError.GenericError("Invalid server state")
      }
    } yield ownerPubKey
  }
}
