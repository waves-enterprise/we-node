package com.wavesenterprise.api.http.utils

import com.wavesenterprise.account.{Address, AddressOrAlias, Alias}
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.transaction.ValidationError
import cats.implicits._

trait AccountFromAddressOrAliasString {

  def blockchain: Blockchain

  protected def accountFrom(addressOrAlias: String): Either[ValidationError, Address] = {
    AddressOrAlias
      .fromString(addressOrAlias)
      .leftMap(ValidationError.fromCryptoError)
      .flatMap {
        case address: Address => Right(address)
        case alias: Alias     => blockchain.resolveAlias(alias)
      }
  }

}
