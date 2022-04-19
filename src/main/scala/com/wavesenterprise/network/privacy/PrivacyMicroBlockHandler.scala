package com.wavesenterprise.network.privacy

import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.block.{MicroBlock, TxMicroBlock}
import com.wavesenterprise.database.PrivacyState
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.privacy.PolicyStorage
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.transaction.{AtomicTransaction, PolicyDataHashTransactionV3}
import monix.eval.Task
import com.wavesenterprise.utils.ScorexLogging

/** Component for actions with micro-block for privacy exchange */
trait PrivacyMicroBlockHandler {
  def broadcastInventoryIfNeed(microBlock: MicroBlock): Task[Unit]
}

object NoOpPrivacyMicroBlockHandler extends PrivacyMicroBlockHandler {
  override def broadcastInventoryIfNeed(microBlock: MicroBlock): Task[Unit] =
    Task.unit
}

class EnabledPrivacyMicroBlockHandler(
    owner: PrivateKeyAccount,
    protected val peers: ActivePeerConnections,
    protected val state: Blockchain with PrivacyState,
    protected val storage: PolicyStorage
) extends PrivacyMicroBlockHandler
    with PolicyInventoryBroadcaster
    with PolicyItemTypeSelector
    with ScorexLogging {

  /**
    * Sends privacy inventory for every PolicyDataHashTransaction inside atomic tx.
    *
    * This is necessary because an Inventory of the PolicyDataHash for a `policyId`
    *   that is created inside of the same AtomicTx cannot be sent optimistically.
    *
    * For example, HTTP API '/sendData?broadcast=false' doesn't send privacy inventory.
    * @param microBlock - appended microBlock
    */
  def broadcastInventoryIfNeed(microBlock: MicroBlock): Task[Unit] =
    microBlock match {
      case txMicro: TxMicroBlock =>
        Task
          .defer {
            val pdhTxs = extractDataHashTxs(txMicro)

            Task
              .parTraverseN(Runtime.getRuntime.availableProcessors())(pdhTxs) { pdhTx =>
                for {
                  dataType <- policyItemType(pdhTx.policyId, pdhTx.dataHash)

                  inventory <- buildPrivacyInventory(
                    dataType = dataType,
                    policyId = pdhTx.policyId,
                    dataHash = pdhTx.dataHash,
                    signer = owner
                  )

                  _ = broadcastInventory(inventory)
                } yield log.debug(s"Broadcast inventory for policy id '${pdhTx.policyId}' after append micro-block")
              }
              .void
          }
          .onErrorHandle { throwable =>
            log.error(s"Failed to broadcast privacy inventory for appended micro-block", throwable)
          }
      case _ => Task.unit
    }

  private def extractDataHashTxs(microBlock: TxMicroBlock): List[PolicyDataHashTransactionV3] = {
    val phdTxs = scala.collection.mutable.ListBuffer[PolicyDataHashTransactionV3]()

    microBlock.transactionData.foreach {
      case atomicTx: AtomicTransaction =>
        atomicTx.transactions.foreach {
          case phd: PolicyDataHashTransactionV3 =>
            phdTxs += phd
          case _ => ()
        }
      case _ => ()
    }

    phdTxs.toList
  }
}
