package com.wavesenterprise.state.contracts.confidential

import cats.implicits._
import com.google.common.io.ByteArrayDataOutput
import com.google.common.io.ByteStreams.newDataOutput
import com.wavesenterprise.ObservableExt
import com.wavesenterprise.block.Block.BlockId
import com.wavesenterprise.block.{Block, TxMicroBlock}
import com.wavesenterprise.crypto.internals.confidentialcontracts.Commitment
import com.wavesenterprise.database.RocksDBDeque.ConfidentialRocksDBDeque
import com.wavesenterprise.database.docker.KeysRequest
import com.wavesenterprise.database.rocksdb.confidential._
import com.wavesenterprise.network.contracts.ConfidentialDataSynchronizer
import com.wavesenterprise.serialization.BinarySerializer
import com.wavesenterprise.state.{ByteStr, ContractBlockchain, ContractId, DataEntry}
import com.wavesenterprise.transaction.Transaction
import com.wavesenterprise.transaction.docker.{ExecutedContractData, ExecutedContractTransactionV4}
import com.wavesenterprise.utils.ReadWriteLocking
import monix.catnap.MVar
import monix.eval.{Coeval, Task}
import monix.execution.Scheduler
import monix.reactive.OverflowStrategy
import monix.reactive.subjects.ConcurrentSubject
import scorex.util.ScorexLogging

import java.util.concurrent.locks.{ReadWriteLock, ReentrantReadWriteLock}
import scala.collection.mutable
import scala.concurrent.duration.DurationInt

class ConfidentialStateUpdater(confidentialDataSynchronizer: ConfidentialDataSynchronizer,
                               state: PersistentConfidentialState,
                               confidentialStorage: ConfidentialRocksDBStorage,
                               blockchain: ContractBlockchain)(implicit val scheduler: Scheduler) extends ConfidentialState with ReadWriteLocking
    with ScorexLogging
    with AutoCloseable {
  import ConfidentialStateUpdater._

  override protected val lock: ReadWriteLock = new ReentrantReadWriteLock()

  private val persistenceBlockQueue: ConfidentialRocksDBDeque[PersistenceBlockTask] =
    ConfidentialStateCFKeys.persistenceBlockQueue(state)

  @volatile private var currentLiquidId: Option[BlockId]                     = None
  private val liquidBlocksQueue: mutable.Queue[LiquidBlockTask]              = mutable.Queue.empty
  private val liquidStates: mutable.LinkedHashMap[BlockId, ConfidentialDiff] = mutable.LinkedHashMap.empty

  private val persistenceBlocksQueueUpdates = ConcurrentSubject.publish[Unit]
  private val persistenceBlocksUpdates      = ConcurrentSubject.publish[BlockId]
  private val liquidBlockQueueUpdates       = ConcurrentSubject.publish[Unit]

  private val persistenceBlocksProcessing = persistenceBlocksQueueUpdates
    .asyncBoundary(OverflowStrategy.Default)
    .whileBusyDropEventsAndSignal(_ => ()) // Collapse all updates into one
    .mapEval { _ =>
      readLock(persistenceBlockQueue.peekFirst).fold(Task.unit)(processPersistenceBlockQueue)
    }
    .logErr
    .onErrorRestartUnlimited
    .subscribe()

  private def processPersistenceBlockQueue(task: PersistenceBlockTask): Task[Unit] =
    Task(log.trace(s"Processing persistence block task $task")) *>
      task.txOutputCommitments
        .toList
        .foldLeftM[Task, ConfidentialDiff](EmptyConfidentialDiff) { (acc, outCommitment) =>
          awaitConfidentialOutputLoading(outCommitment).map { output =>
            val diff = ConfidentialDiff.fromOutput(output)
            acc combine diff
          }
        }.flatMap { blockDiff =>
          Task.defer {
            val (maybeQueueTask, isExtracted) = readLock(persistenceBlockQueue.pollFirstIf(_.contains(task)))

            maybeQueueTask.map { queueTask =>
              if (isExtracted) {
                Task(state.appendBlock(blockDiff, queueTask.id, queueTask.height)) *>
                  Task.deferFuture(persistenceBlocksUpdates.onNext(task.id)) *>
                  readLock(persistenceBlockQueue.peekFirst)
                    .map(nextTask => Task.defer(processPersistenceBlockQueue(nextTask)))
                    .orEmpty
              } else {
                Task(log.warn(s"Extracted persistence task '$queueTask' is not equal expected '$task'"))
              }
            }.getOrElse {
              Task(log.warn(s"Persistence queue is empty, expected '$task' item"))
            }
          }
        }

  private val liquidBlockProcessing = liquidBlockQueueUpdates.asyncBoundary(OverflowStrategy.Default)
    .whileBusyDropEventsAndSignal(_ => ()) // Collapse all updates into one
    .mapEval { _ =>
      readLock(liquidBlocksQueue.headOption).fold(Task.unit)(processLiquidBlockQueue)
    }
    .logErr
    .onErrorRestartUnlimited
    .subscribe()

  private def processLiquidBlockQueue(task: LiquidBlockTask): Task[Unit] = {
    Task(log.trace(s"Processing liquid block task $task")) *>
      task.txOutputCommitments
        .toList
        .foldLeftM[Task, ConfidentialDiff](EmptyConfidentialDiff) { (acc, outCommitment) =>
          awaitConfidentialOutputLoading(outCommitment).map { output =>
            val keyValueMap  = output.entries.view.map(entry => entry.key -> entry).toMap[String, DataEntry[_]]
            val executedData = ExecutedContractData(keyValueMap)
            acc.combine(ConfidentialDiffValue(Map(output.contractId -> executedData)))
          }
        }.flatMap { microBlockDiff =>
          Task.defer {
            log.trace(s"Processing liquid block task diff $microBlockDiff")
            task match {
              case LiquidBlockTaskWithReference(_, referenceLiquidBlockId, _) =>
                Task {
                  readLock {
                    currentLiquidId.ensuring(_.contains(referenceLiquidBlockId),
                                             s"Expected '$referenceLiquidBlockId' liquid block id, actual '$currentLiquidId'")
                    liquidStates
                      .getOrElse(referenceLiquidBlockId, throw new IllegalStateException(s"Referenced state '$referenceLiquidBlockId' not found"))
                      .combine(microBlockDiff)
                  }
                }
              case KeyBlockTask(id, referencePersistenceBlockId, _) =>
                Task.defer {
                  log.trace(s"Processing liquid key block '$id' task")
                  discardLiquidState()
                  awaitPersistenceBlockApply(referencePersistenceBlockId).as(microBlockDiff)
                }
            }
          }.flatMap { newDiff =>
            Task.defer {
              writeLock {
                if (liquidBlocksQueue.nonEmpty) {
                  val queueTask = liquidBlocksQueue.dequeue()
                  if (queueTask == task) {
                    liquidStates.put(task.liquidBlockId, newDiff)
                    currentLiquidId = Some(task.liquidBlockId)

                    readLock(liquidBlocksQueue.headOption) match {
                      case Some(nextTask) => Task.defer(processLiquidBlockQueue(nextTask))
                      case None           => Task.unit
                    }
                  } else {
                    Task {
                      liquidBlocksQueue.enqueue(queueTask)
                      log.warn(s"Extracted liquid task '$queueTask' is not equal expected '$task'")
                    }
                  }
                } else {
                  Task(log.warn(s"Liquid queue is empty, expected '$task' item"))
                }
              }
            }
          }
        }
  }

  def processMicroBlock(microBlock: TxMicroBlock): Unit = {
    val liquidBlockId          = microBlock.totalLiquidBlockSig
    val referenceLiquidBLockId = microBlock.prevLiquidBlockSig
    val txOutputCommitments    = extractOutputCommitments(microBlock.transactionData)
    val task                   = LiquidBlockTaskWithReference(liquidBlockId, referenceLiquidBLockId, txOutputCommitments)
    writeLock(liquidBlocksQueue.enqueue(task))
    liquidBlockQueueUpdates.onNext(())
  }

  def processBlock(forgedLiquidBlock: Block, forgedBlockHeight: Int, newBlock: Block): Unit = {
    writeLock {
      addPersistenceBlockTask(forgedLiquidBlock, forgedBlockHeight)
      discardLiquidState()
      val txOutputCommitments = extractOutputCommitments(newBlock.transactionData)
      val task                = KeyBlockTask(newBlock.uniqueId, newBlock.reference, txOutputCommitments)
      liquidBlocksQueue.enqueue(task)
    }

    liquidBlockQueueUpdates.onNext(())
  }

  def rollbackPersistenceBlocks(blocks: Seq[Block], height: Int): Unit =
    writeLock {
      val rollbackDepth = blocks.size
      val queueSize     = persistenceBlockQueue.size

      // Rollback queue
      (1 to (queueSize min rollbackDepth)).view.zip(blocks).foreach { case (_, block) =>
        val lastTask = persistenceBlockQueue.pollLast
        lastTask.ensuring(_.exists(_.id == block.uniqueId), s"Expected '${block.uniqueId}' last block in queue, actual '${lastTask.map(_.id)}'")
      }

      // Rollback already processed blocks
      blocks.takeRight(rollbackDepth - queueSize).zipWithIndex.foreach { case (persistedBlock, i) =>
        state.rollbackBlock(persistedBlock, height - i)
      }
    }

  def discardLiquidState(): Unit =
    writeLock {
      log.trace("Discard liquid state")
      currentLiquidId = None
      liquidStates.clear()
      liquidBlocksQueue.clear()
    }

  def isSynchronized: Boolean =
    readLock {
      liquidBlocksQueue.isEmpty &&
      persistenceBlockQueue.isEmpty
    }

  private def addPersistenceBlockTask(block: Block, forgedBlockHeight: Int): Unit = {
    val txOutputCommitments = extractOutputCommitments(block.transactionData)
    val task                = PersistenceBlockTask(block.uniqueId, forgedBlockHeight, txOutputCommitments)
    writeLock(persistenceBlockQueue.addLast(task))
    persistenceBlocksQueueUpdates.onNext(())
  }

  private def extractOutputCommitments(txs: Seq[Transaction]): Seq[Commitment] = {
    txs.collect {
      case tx: ExecutedContractTransactionV4 if blockchain.contract(ContractId(tx.tx.contractId)).exists(_.isConfidential) =>
        Some(tx.outputCommitment)
    }.flatten
  }

  private def awaitPersistenceBlockApply(id: BlockId): Task[Unit] =
    Task.defer {
      for {
        subscribtionTrigger <- MVar.empty[Task, Unit]()
        awaiting <- persistenceBlocksUpdates.asyncBoundary(OverflowStrategy.Default)
          .doAfterSubscribe(subscribtionTrigger.put(()))
          .findL(_ == id).void.start
        _ <- subscribtionTrigger.take *>
          Task {
            if (readLock(state.lastPersistenceBlock.contains(id)))
              Task(awaiting.cancel)
            else
              awaiting.join
          }
      } yield {
        log.trace(s"Completed block '$id' wait")
      }

    }

  private def awaitConfidentialOutputLoading(commitment: Commitment): Task[ConfidentialOutput] =
    Task.defer {
      confidentialStorage.getOutput(commitment).fold(
        confidentialDataSynchronizer.synchronizedOutputs.filter(_.commitment == commitment).firstL
          .timeoutTo(
            30.seconds,
            Task(log.warn(s"Confidential output '$commitment' loading timeout, retrying")) *>
              awaitConfidentialOutputLoading(commitment)
          )
      ) { output =>
        Task.pure(output)
      }
    }

  override def close(): Unit = {
    persistenceBlocksQueueUpdates.onComplete()
    persistenceBlocksUpdates.onComplete()
    liquidBlockQueueUpdates.onComplete()

    persistenceBlocksProcessing.cancel()
    liquidBlockProcessing.cancel()
  }
  private def lastLiquidDiff: ConfidentialDiff = {
    liquidStates.lastOption
      .map { case (_, lastDiff) =>
        lastDiff
      }
      .orEmpty
  }

  override def contractKeys(request: KeysRequest): Vector[String] =
    readLock {
      CompositeConfidentialState.composite(state, lastLiquidDiff).contractKeys(request)
    }

  override def contractData(contractId: ContractId): ExecutedContractData =
    readLock {
      CompositeConfidentialState.composite(state, lastLiquidDiff).contractData(contractId)
    }

  override def contractData(contractId: ContractId, keys: Iterable[String]): ExecutedContractData =
    readLock {
      CompositeConfidentialState.composite(state, lastLiquidDiff).contractData(contractId, keys)
    }

  override def contractData(contractId: ContractId, key: String): Option[DataEntry[_]] =
    readLock {
      CompositeConfidentialState.composite(state, lastLiquidDiff).contractData(contractId, key)
    }
}

object ConfidentialStateUpdater {
  case class PersistenceBlockTask(
      id: BlockId,
      height: Int,
      txOutputCommitments: Seq[Commitment]
  ) {
    val bytes: Coeval[Array[Byte]] = Coeval.evalOnce {
      def writeCommitment(commitment: Commitment, out: ByteArrayDataOutput): Unit = BinarySerializer.writeShortByteArray(commitment.hash.arr, out)
      // noinspection UnstableApiUsage
      val out = newDataOutput()
      BinarySerializer.writeShortByteArray(id.arr, out)
      out.writeInt(height)
      BinarySerializer.writeBigIterable(txOutputCommitments, writeCommitment, out)
      out.toByteArray
    }
  }

  object PersistenceBlockTask {
    def parseBytesUnsafe(bytes: Array[Byte]): PersistenceBlockTask = {
      val (idBytes, idBytesEnd) = BinarySerializer.parseShortByteArray(bytes, 0)

      val (height, heightEnd)   = BinarySerializer.parseInt(bytes, idBytesEnd)
      val (commitmentsBytes, _) = BinarySerializer.parseBigList(bytes, BinarySerializer.parseShortByteArray, heightEnd)

      PersistenceBlockTask(
        ByteStr(idBytes),
        height,
        commitmentsBytes.map(bytes => Commitment(ByteStr(bytes)))
      )
    }
  }

  private sealed trait LiquidBlockTask {
    def liquidBlockId: BlockId

    def txOutputCommitments: Seq[Commitment]
  }

  private case class LiquidBlockTaskWithReference(
      liquidBlockId: BlockId,
      referenceLiquidBlockId: BlockId,
      txOutputCommitments: Seq[Commitment]
  ) extends LiquidBlockTask

  private case class KeyBlockTask(
      liquidBlockId: BlockId,
      referencePersistenceBlockId: BlockId,
      txOutputCommitments: Seq[Commitment]
  ) extends LiquidBlockTask
}
