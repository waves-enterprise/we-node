package com.wavesenterprise.javadsl.settings;

import com.wavesenterprise.settings.PkiCryptoSettings;
import com.wavesenterprise.settings.Waves$;

public interface WavesCryptoSettings {
  com.wavesenterprise.settings.CryptoSettings WAVES_CRYPTO_SETTINGS = Waves$.MODULE$.apply(PkiCryptoSettings.DisabledPkiSettings$.MODULE$);
}