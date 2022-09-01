package com.wavesenterprise.transaction

import com.wavesenterprise.account.Address

case class AssetAcc(account: Address, assetId: Option[AssetId])
