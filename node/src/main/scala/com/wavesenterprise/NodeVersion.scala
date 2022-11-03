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
  def apply(tupledVersion: (Int, Int, Int)): NodeVersion = {
    new NodeVersion(tupledVersion._1, tupledVersion._2, tupledVersion._3)
  }

  // "v1.2.3" format
  implicit val toPrintable: Show[NodeVersion] = { nv =>
    "v" + nv.asFlatString
  }

  implicit val nodeVersionOrdering: Ordering[NodeVersion] = Ordering
    .by(nv => (nv.majorVersion, nv.minorVersion, nv.patchVersion))

  val featureIntroductionHistory: Map[NodeVersion, List[ProtocolFeature]] = Map(
    NodeVersion(1, 11, 1) -> List(SeparateBlockAndTxMessages, PeerIdentityWithCertsMessages)
  )
}
