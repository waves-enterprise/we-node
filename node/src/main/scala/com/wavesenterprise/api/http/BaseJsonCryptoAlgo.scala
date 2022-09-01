package com.wavesenterprise.api.http

import com.wavesenterprise.crypto
import com.wavesenterprise.settings.{CryptoSettings, Waves}

trait BaseJsonCryptoAlgo extends JsonCryptoAlgo {
  override def cryptoAlgos: Set[String] = selectCryptoAlgos(crypto.cryptoSettings)

  protected def selectCryptoAlgos(settings: CryptoSettings): Set[String] =
    settings match {
      case _: Waves => Set("aes")
      case _        => Set.empty
    }
}
