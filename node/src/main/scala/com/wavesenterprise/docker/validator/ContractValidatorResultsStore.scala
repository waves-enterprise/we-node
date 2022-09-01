package com.wavesenterprise.docker.validator

import com.wavesenterprise.account.Address
import com.wavesenterprise.network.ContractValidatorResults
import com.wavesenterprise.state.ByteStr

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.collection.SeqView

class ContractValidatorResultsStore {

  private[this] val resultsByKeyBlock = new ConcurrentHashMap[ByteStr, ResultsByTx]()

  def findResults(blockId: ByteStr,
                  txId: ByteStr,
                  validators: Set[Address],
                  requiredResultHash: Option[ByteStr] = None,
                  limit: Option[Int] = None): Set[ContractValidatorResults] = {
    (for {
      resultsByTx    <- get(blockId)
      resultsListSet <- resultsByTx.get(txId)
    } yield {
      val filteredResult =
        resultsListSet.view.reverse // The results are added using prepend operation, so we need reverse the list
          .filter { result =>
            validators.contains(result.sender.toAddress) &&
            requiredResultHash.forall(result.resultsHash == _)
          }

      limit.fold(filteredResult)(filteredResult.take).toSet
    }).getOrElse(Set.empty)
  }

  private[this] def get(blockId: ByteStr): Option[ResultsByTx] = {
    Option(resultsByKeyBlock.get(blockId))
  }

  def add(results: ContractValidatorResults): Unit = {
    val resultsByTx = resultsByKeyBlock.computeIfAbsent(results.keyBlockId, _ => new ResultsByTx())
    resultsByTx.add(results)
  }

  def contains(results: ContractValidatorResults): Boolean = {
    get(results.keyBlockId).exists(_.contains(results))
  }

  def removeExceptFor(blockId: ByteStr): Unit = {
    resultsByKeyBlock.keySet().removeIf(_ != blockId)
  }

  def removeInvalidResults(blockId: ByteStr, txId: ByteStr, validResultHash: ByteStr): Unit = {
    get(blockId).foreach(_.removeInvalidResults(txId, validResultHash))
  }

  def blocksSize: Int = resultsByKeyBlock.size

  def txsSize: Int = resultsByKeyBlock.values.iterator.asScala.map(_.txsSize).sum

  def size: Int = resultsByKeyBlock.values.iterator.asScala.map(_.size).sum
}

private[this] class ResultsByTx {

  private[this] val resultsMap = new ConcurrentHashMap[ByteStr, ListSet[ContractValidatorResults]]

  def get(txId: ByteStr): Option[ListSet[ContractValidatorResults]] = Option(resultsMap.get(txId))

  def contains(results: ContractValidatorResults): Boolean = {
    get(results.txId).exists(_.contains(results))
  }

  def add(results: ContractValidatorResults): Unit = {
    resultsMap.putIfAbsent(results.txId, ListSet.empty)
    resultsMap.computeIfPresent(results.txId, (_, resultsSet) => resultsSet.prepend(results))
  }

  def removeInvalidResults(txId: ByteStr, validResultHash: ByteStr): ListSet[ContractValidatorResults] = {
    resultsMap.computeIfPresent(txId, (_, resultsSet) => resultsSet.filter(_.resultsHash == validResultHash))
  }

  def txsSize: Int = resultsMap.size

  def size: Int = resultsMap.values.iterator.asScala.map(_.size).sum
}

/**
  * ListSet implementation with cheap prepend and contains operations and excessive memory usage.
  */
private class ListSet[T](val list: List[T], val set: Set[T]) {

  def contains(value: T): Boolean = set.contains(value)

  def prepend(value: T): ListSet[T] = {
    if (set.contains(value))
      this
    else
      new ListSet(value :: list, set + value)
  }

  def view: SeqView[T, List[T]] = list.view

  def filter(p: T => Boolean): ListSet[T] = new ListSet(list.filter(p), set.filter(p))

  def size: Int = set.size
}

private object ListSet {
  def empty[T]: ListSet[T] = new ListSet[T](List.empty, Set.empty)
}
