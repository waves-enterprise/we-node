package com.wavesenterprise.database.rocksdb

import com.wavesenterprise.database.BaseKey

package object confidential {
  type ConfidentialKey[V] = BaseKey[V, ConfidentialDBColumnFamily]
}
