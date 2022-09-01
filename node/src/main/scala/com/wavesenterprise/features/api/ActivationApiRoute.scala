package com.wavesenterprise.features.api

import akka.http.scaladsl.server.Route
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.{ApiRoute, CommonApiFunctions}
import com.wavesenterprise.features.FeatureProvider._
import com.wavesenterprise.features.{BlockchainFeatureStatus, BlockchainFeature}
import com.wavesenterprise.settings.{ApiSettings, FeaturesSettings, FunctionalitySettings}
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.utils.{ScorexLogging, Time}
import monix.execution.schedulers.SchedulerService
import play.api.libs.json.{Format, Json}

class ActivationApiRoute(val settings: ApiSettings,
                         val time: Time,
                         functionalitySettings: FunctionalitySettings,
                         featuresSettings: FeaturesSettings,
                         blockchain: Blockchain,
                         val nodeOwner: Address,
                         val scheduler: SchedulerService)
    extends ApiRoute
    with CommonApiFunctions
    with ScorexLogging {

  override lazy val route: Route = pathPrefix("activation") {
    withAuth() {
      status
    }
  }

  implicit val activationStatusFormat: Format[ActivationStatus] = Json.format

  /**
    * GET /activation/status
    **/
  def status: Route = (get & path("status")) {
    withExecutionContext(scheduler) {
      val height = blockchain.height

      val featureVotes      = blockchain.featureVotes(height)
      val votedFeatures     = featureVotes.keySet
      val approvedFeatures  = blockchain.approvedFeatures.keySet
      val supportedFeatures = featuresSettings.featuresSupportMode.fold(_.supportedFeatures.toSet, _ => BlockchainFeature.implemented)

      val features = (votedFeatures ++ approvedFeatures ++ BlockchainFeature.implemented).toSeq.sorted.map { id =>
        val status           = blockchain.featureStatus(id, height)
        val supportingBlocks = if (status == BlockchainFeatureStatus.Undefined) featureVotes.get(id).orElse(Some(0)) else None

        FeatureActivationStatus(
          id = id,
          description = BlockchainFeature.withValueOpt(id).fold("Unknown feature")(_.description),
          blockchainStatus = status,
          nodeStatus = (BlockchainFeature.implemented.contains(id), supportedFeatures.contains(id)) match {
            case (false, _) => NodeFeatureStatus.NotImplemented
            case (_, true)  => NodeFeatureStatus.Voted
            case _          => NodeFeatureStatus.Implemented
          },
          activationHeight = blockchain.featureActivationHeight(id),
          supportingBlocks = supportingBlocks
        )
      }

      complete(
        ActivationStatus(
          height,
          functionalitySettings.featureCheckBlocksPeriod,
          functionalitySettings.blocksForFeatureActivation,
          functionalitySettings.activationWindow(height).last,
          features
        ))
    }
  }
}
