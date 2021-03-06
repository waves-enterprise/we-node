package com.wavesenterprise

import cats.Show
import com.wavesenterprise.NodeVersion.featureIntroductionHistory
import com.wavesenterprise.network.ProtocolFeature
import com.wavesenterprise.network.ProtocolFeature._

import scala.Ordering.Implicits.infixOrderingOps

case class NodeVersion(majorVersion: Int, minorVersion: Int, patchVersion: Int) {
  lazy val features: Set[ProtocolFeature] = {
    featureIntroductionHistory.foldLeft(Set.empty[ProtocolFeature]) {
      case (accumulatedFeatures, (version, versionFeatures)) =>
        if (this >= version) {
          accumulatedFeatures ++ versionFeatures
        } else {
          accumulatedFeatures
        }
    }
  }

  // "1.2.3" format
  def asFlatString: String = (majorVersion :: minorVersion :: patchVersion :: Nil).mkString(".")
}

object NodeVersion {
  val MinInventoryBasedPrivacyProtocolSupport: NodeVersion = NodeVersion(1, 7, 0)

  def apply(tupledVersion: (Int, Int, Int)): NodeVersion = {
    new NodeVersion(tupledVersion._1, tupledVersion._2, tupledVersion._3)
  }

  // "v1.2.3" format
  implicit val toPrintable: Show[NodeVersion] = { nv =>
    "v" + nv.asFlatString
  }

  implicit val nodeVersionOrdering: Ordering[NodeVersion] = Ordering
    .by(nv => (nv.majorVersion, nv.minorVersion, nv.patchVersion))

  val featureIntroductionHistory = Map(
    NodeVersion(1, 2, 1) -> List(PeersHostnameSupport),
    NodeVersion(1, 5, 0) -> List(MultipleExtBlocksRequest),
    NodeVersion(1, 6, 0) -> List(NetworkMessageShaChecksum),
    NodeVersion(1, 7, 0) -> List(NodeAttributesFeature, PrivacyProtocolExtensionV1, HistoryReplierExtensionV1)
  )
}
