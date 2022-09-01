package com.wavesenterprise

import java.net.InetSocketAddress

case class ApplicationInfo(
    chainId: Char,
    nodeVersion: NodeVersion,
    consensusType: String,
    nodeName: String,
    nodeNonce: Long,
    declaredAddress: Option[InetSocketAddress]
)
