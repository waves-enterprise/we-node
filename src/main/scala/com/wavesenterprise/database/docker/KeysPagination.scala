package com.wavesenterprise.database.docker

import com.wavesenterprise.state.{ByteStr, _}

case class KeysRequest(contractId: ByteStr,
                       offsetOpt: Option[Int] = None,
                       limitOpt: Option[Int] = None,
                       keysFilter: Option[String => Boolean] = None,
                       knownKeys: Iterable[String] = Iterable.empty)

class KeysPagination(keysIterator: Iterator[String]) {

  def paginatedKeys(offsetOpt: Option[Int], limitOpt: Option[Int], keysFilter: Option[String => Boolean]): Iterator[String] = {
    val filteredKeys = keysFilter.map(keysIterator.filter).getOrElse(keysIterator)
    if (filteredKeys.isEmpty) {
      Iterator.empty
    } else {
      (offsetOpt, limitOpt) match {
        case (None, None)         => filteredKeys
        case (Some(offset), None) => filteredKeys.drop(offset.positiveOrZero)
        case (None, Some(limit))  => filteredKeys.take(limit)
        case (Some(offset), Some(limit)) =>
          val realOffset = offset.positiveOrZero
          val to         = Math.addExact(realOffset, limit)
          filteredKeys.slice(realOffset, to)
      }
    }
  }
}
