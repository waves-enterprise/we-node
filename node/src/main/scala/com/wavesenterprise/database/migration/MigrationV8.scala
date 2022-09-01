package com.wavesenterprise.database.migration

import com.google.common.primitives.Shorts
import com.wavesenterprise.account.Alias
import com.wavesenterprise.database.address.AddressTransactions
import com.wavesenterprise.database.keys.AddressCFKeys.IssuedAliasesByAddressPrefix
import com.wavesenterprise.database.rocksdb.ColumnFamily.AddressCF
import com.wavesenterprise.database.rocksdb.RW
import com.wavesenterprise.database.{InternalRocksDBSet, Keys}
import com.wavesenterprise.transaction.CreateAliasTransaction
import com.wavesenterprise.utils.EitherUtils.EitherExt

/**
  * WE-7978: Adds new key with Aliases by Address
  */
object MigrationV8 {
  private def issuedAliasesByAddressId(addressId: BigInt) = new InternalRocksDBSet[Alias](
    name = "issued-aliases-by-address-id",
    columnFamily = AddressCF,
    prefix = Shorts.toByteArray(IssuedAliasesByAddressPrefix) ++ addressId.toByteArray,
    itemEncoder = _.bytes.arr,
    itemDecoder = Alias.fromBytes(_).explicitGet()
  )

  def apply(rw: RW): Unit = {
    val lastAddressId = rw.get(Keys.lastAddressId).getOrElse(BigInt(0))
    for (id <- BigInt(1) to lastAddressId) {
      val address = rw.get(Keys.idToAddress(id))
      val issuedAliases = AddressTransactions(rw, address, Set(CreateAliasTransaction.typeId), Int.MaxValue, None)
        .explicitGet()
        .collect {
          case (_, tx: CreateAliasTransaction) if tx.sender.toAddress == address =>
            tx.alias
        }
        .toSet
      issuedAliasesByAddressId(id).add(rw, issuedAliases)
    }
  }
}
