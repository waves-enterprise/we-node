package com.wavesenterprise.docker.grpc.service

import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.io.ByteStreams.newDataOutput
import com.wavesenterprise.crypto
import com.wavesenterprise.database.docker.KeysRequest
import com.wavesenterprise.network.contracts.ConfidentialDataUtils
import com.wavesenterprise.protobuf.service.contract.{ContractBalancesRequest, ContractBalancesResponse, ContractKeyRequest, ContractKeysRequest}
import com.wavesenterprise.settings.{ConsensusSettings, WESettings}
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext
import com.wavesenterprise.state.{Blockchain, ByteStr, DataEntry}
import com.wavesenterprise.transaction.docker._
import com.wavesenterprise.transaction.protobuf.KeysFilter

import java.util.concurrent.TimeUnit
import scala.collection._
import scala.concurrent.duration.{FiniteDuration, _}

//noinspection UnstableApiUsage
class ContractReadLogService(settings: WESettings, blockchain: Blockchain) {

  private type TxIdByteStr = ByteStr

  private[this] val cache: Cache[TxIdByteStr, Seq[(ReadDescriptor, Array[Byte])]] = {
    val expireAfterSeconds = settings.blockchain.consensus match {
      case ConsensusSettings.PoSSettings      => settings.blockchain.custom.genesis.toPlainSettingsUnsafe.averageBlockDelay
      case cft: ConsensusSettings.CftSettings => cft.roundDuration
      case poa: ConsensusSettings.PoASettings => poa.roundDuration
    }

    val delta: FiniteDuration = 5 seconds

    CacheBuilder
      .newBuilder()
      .expireAfterWrite(expireAfterSeconds.toMillis + delta.toMillis, TimeUnit.MILLISECONDS)
      .build()
  }

  def logGetContractKey(txId: TxIdByteStr, contractKeyRequest: ContractKeyRequest, dataEntry: DataEntry[_]): Unit = {
    val contractId = ByteStr.decodeBase58(contractKeyRequest.contractId).getOrElse(throw new IllegalArgumentException(
      s"Unable to decode String: '${contractKeyRequest.contractId}' to contractId ByteStr"))
    val getContractKeyReadDescriptor: ReadDescriptor = SpecificKeys(contractId, Seq(contractKeyRequest.key))
    val dataByteArray = {
      val ndo = newDataOutput()
      ContractTransactionEntryOps.writeBytes(dataEntry, ndo)
      ndo.toByteArray
    }
    val hashedResultOfGetContractKey: Array[Byte] = crypto.algorithms.secureHash(dataByteArray)
    updateCache(txId, getContractKeyReadDescriptor, hashedResultOfGetContractKey)
  }

  def getStateWithReadDescriptor(txId: TxIdByteStr, readDescriptor: ReadDescriptor): ExecutedContractData = {
    val (contractId, keys) = readDescriptor match {
      case SpecificKeys(contractId, keys) => (contractId, keys)
      case FilteredKeys(contractId, matches, offset, limit) =>
        val keysRequest    = KeysRequest(contractId, offset, limit, matchedToKeyFilterUnsafe(matches))
        val readingContext = ContractReadingContext.TransactionExecution(txId)
        (contractId, blockchain.contractKeys(keysRequest, readingContext))
      case SpecificBalances(contractId, assetIds) => (contractId, Seq.empty) // TODO: proper processing this case
    }
    blockchain.contractData(contractId, keys, ContractReadingContext.TransactionExecution(txId))
  }

  private def matchedToKeyFilterUnsafe(matches: Option[String]): Option[String => Boolean] =
    matches match {
      case Some(regex) => Some(key => regex.r.pattern.matcher(key).matches())
      case _           => None
    }

  def logGetContractKeys(txId: TxIdByteStr, contractKeyRequest: ContractKeysRequest, dataEntries: Vector[DataEntry[_]]): Unit = {
    val contractId = ByteStr.decodeBase58(contractKeyRequest.contractId).getOrElse(throw new IllegalArgumentException(
      s"Unable to decode String: '${contractKeyRequest.contractId}' to contractId ByteStr"))
    val getContractKeysReadDescriptor: ReadDescriptor = contractKeyRequest.keysFilter match {
      case Some(KeysFilter(keys, _)) => SpecificKeys(contractId, keys)
      case None                      => FilteredKeys(contractId, contractKeyRequest.matches, contractKeyRequest.offset, contractKeyRequest.limit)
    }
    val dataByteArray: Array[Byte]                 = ConfidentialDataUtils.entriesToBytes(dataEntries.toList)
    val hashedResultOfGetContractKeys: Array[Byte] = crypto.algorithms.secureHash(dataByteArray)
    updateCache(txId, getContractKeysReadDescriptor, hashedResultOfGetContractKeys)
  }

  def logGetContractAssetBalance(txId: TxIdByteStr,
                                 contractIdStr: String,
                                 contractBalancesRequest: ContractBalancesRequest,
                                 contractBalances: ContractBalancesResponse): Unit = {
    val contractId = ByteStr.decodeBase58(contractIdStr).getOrElse(throw new IllegalArgumentException(
      s"Unable to decode String: '$contractIdStr' to contractId ByteStr"))

    val getContractBalancesDescriptor = SpecificBalances(contractId, contractBalancesRequest.assetsIds)

    val contractBalancesBytes     = ConfidentialDataUtils.contractBalancesToBytes(contractBalances.assetsBalances)
    val responseHash: Array[Byte] = crypto.algorithms.secureHash(contractBalancesBytes)

    updateCache(txId, getContractBalancesDescriptor, responseHash)
  }

  def createFinalReadingsJournal(txId: TxIdByteStr): (Seq[ReadDescriptor], Option[ReadingsHash]) = {
    Option(cache.asMap().remove(txId)).map { inverseReadings =>
      val (readings, hashes) = inverseReadings.reverse.unzip
      val ndo                = newDataOutput()
      hashes.foreach(ndo.write)
      val resultHash = ReadingsHash(ByteStr(crypto.algorithms.secureHash(ndo.toByteArray)))
      readings -> Some(resultHash)
    }.getOrElse {
      Seq.empty -> None
    }
  }

  private def updateCache(txId: TxIdByteStr, readDescriptor: ReadDescriptor, hashResult: Array[Byte]): Unit =
    cache.asMap().merge(txId, Seq(readDescriptor -> hashResult), (valueInCache, newValue) => newValue ++: valueInCache)

}
