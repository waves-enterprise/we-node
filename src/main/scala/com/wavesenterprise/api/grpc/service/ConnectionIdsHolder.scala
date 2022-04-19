package com.wavesenterprise.api.grpc.service

import akka.grpc.scaladsl.Metadata
import com.wavesenterprise.api.grpc.service.ConnectionsLimiter.ConnectionId
import com.wavesenterprise.api.grpc.utils.{ConnectionIdMaxLength, ConnectionIdMetadataKey}
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.utils.ScorexLogging
import org.apache.commons.lang3.RandomStringUtils

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable
import scala.jdk.CollectionConverters._

trait ConnectionIdsHolder extends ScorexLogging {

  protected val connectionIds: mutable.Set[ConnectionId] = ConcurrentHashMap.newKeySet[ConnectionId]().asScala

  def getConnectionId(metadata: Metadata): Either[ApiError, ConnectionId] = {
    metadata
      .getText(ConnectionIdMetadataKey)
      .fold[Either[ApiError, ConnectionId]] {
        Right(generateUniqueConnectionId)
      } { connectionId =>
        if (connectionId.length > ConnectionIdMaxLength) {
          Left(ApiError.InvalidConnectionId(s"'$ConnectionIdMetadataKey' metadata value max length is 16. Got '${connectionId.length}'"))
        } else if (connectionId.isBlank) {
          Left(ApiError.InvalidConnectionId(s"'$ConnectionIdMetadataKey' metadata value must be non-empty"))
        } else if (!connectionIds.add(connectionId)) {
          Left(ApiError.InvalidConnectionId(s"Connection Id '$connectionId' already in use"))
        } else {
          Right(connectionId)
        }
      }
  }

  @annotation.tailrec
  final def generateUniqueConnectionId: ConnectionId = {
    val id = RandomStringUtils.randomAlphanumeric(8).toLowerCase
    if (!connectionIds.add(id)) {
      generateUniqueConnectionId
    } else {
      log.trace(s"Generated connection-id '$id'")
      id
    }
  }
}
