package com.wavesenterprise.api.http.assets

import com.wavesenterprise.account.AddressOrAlias
import com.wavesenterprise.transaction.ValidationError.Validation
import com.wavesenterprise.transaction.transfer.{ParsedTransfer, TransferDescriptor}
import cats.implicits._
import com.wavesenterprise.transaction.ValidationError

object MassTransferRequest {

  def parseTransfersList(transfers: List[TransferDescriptor]): Validation[List[ParsedTransfer]] = {
    transfers.traverse {
      case TransferDescriptor(recipient, amount) =>
        AddressOrAlias.fromString(recipient).leftMap(ValidationError.fromCryptoError).map(ParsedTransfer(_, amount))
    }
  }
}
