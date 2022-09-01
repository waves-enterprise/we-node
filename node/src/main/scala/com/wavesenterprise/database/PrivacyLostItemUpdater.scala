package com.wavesenterprise.database

import com.wavesenterprise.account.Address
import com.wavesenterprise.privacy.PolicyDataId
import com.wavesenterprise.state.PrivacyBlockchain

trait PrivacyLostItemUpdater extends PrivacyState with PrivacyBlockchain {
  def forceUpdateIfNotInProgress(dataId: PolicyDataId, ownerAddress: Address): Unit
}
