package com.wavesenterprise.features.api

import com.wavesenterprise.features.BlockchainFeatureStatus

case class FeatureActivationStatus(id: Short,
                                   description: String,
                                   blockchainStatus: BlockchainFeatureStatus,
                                   nodeStatus: NodeFeatureStatus,
                                   activationHeight: Option[Int],
                                   supportingBlocks: Option[Int])
