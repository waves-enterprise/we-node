package com.wavesenterprise

import java.net.InetSocketAddress

case class ApplicationInfo(
    applicationName: String,
    nodeVersion: NodeVersion,
    consensusType: String,
    nodeName: String,
    nodeNonce: Long,
    declaredAddress: Option[InetSocketAddress]
) {
  val chainId: Char = ApplicationInfo.extractChainIdFromAppName(applicationName)
}

object ApplicationInfo {

  /** Legacy implementation. Needed to maintain backward compatibility */
  def extractChainIdFromAppName(appName: String): Char = {
    if (appName.isEmpty)
      throw new IllegalArgumentException("Invalid application name. The last character must contain chain id")
    else
      appName.last
  }
}
