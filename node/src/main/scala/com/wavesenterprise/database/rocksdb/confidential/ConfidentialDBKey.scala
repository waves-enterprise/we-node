package com.wavesenterprise.database.rocksdb.confidential

import com.wavesenterprise.database.rocksdb.ConfidentialDBColumnFamily
import com.wavesenterprise.database.KeyConstructors

object ConfidentialDBKey extends KeyConstructors[ConfidentialDBColumnFamily] {

  override val presetCF: ConfidentialDBColumnFamily = ConfidentialDBColumnFamily.PresetCF

}
