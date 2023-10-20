package com.wavesenterprise.database.keys

import com.google.common.primitives.Shorts
import com.wavesenterprise.account.{Address, Alias}
import com.wavesenterprise.database.KeyHelpers.{addr, bytes, hash}
import com.wavesenterprise.database.rocksdb.MainDBColumnFamily.AddressCF
import com.wavesenterprise.database.rocksdb.MainRocksDBStorage
import com.wavesenterprise.database.{MainDBKey, RocksDBSet}
import com.wavesenterprise.database.RocksDBSet._
import com.wavesenterprise.utils.EitherUtils.EitherExt

object AddressCFKeys {

  val AddressIdOfAliasPrefix: Short        = 1
  val LastAddressIdPrefix: Short           = 2
  val AddressIdPrefix: Short               = 3
  val IdToAddressPrefix: Short             = 4
  val LastNonEmptyRoleAddressPrefix: Short = 5
  val NonEmptyRoleAddressIdPrefix: Short   = 6
  val NonEmptyRoleAddressPrefix: Short     = 7
  val IssuedAliasesByAddressPrefix: Short  = 8

  def addressIdOfAlias(alias: Alias): MainDBKey[Option[BigInt]] =
    MainDBKey.opt("address-id-of-alias", AddressCF, bytes(AddressIdOfAliasPrefix, alias.bytes.arr), BigInt(_), _.toByteArray)

  val LastAddressId: MainDBKey[Option[BigInt]] =
    MainDBKey.opt("last-address-id", AddressCF, bytes(LastAddressIdPrefix, Array.emptyByteArray), BigInt(_), _.toByteArray)

  def addressId(address: Address): MainDBKey[Option[BigInt]] =
    MainDBKey.opt("address-id", AddressCF, bytes(AddressIdPrefix, address.bytes.arr), BigInt(_), _.toByteArray)

  def idToAddress(id: BigInt): MainDBKey[Address] =
    MainDBKey("id-to-address", AddressCF, bytes(IdToAddressPrefix, id.toByteArray), Address.fromBytes(_).explicitGet(), _.bytes.arr)

  val LastNonEmptyRoleAddressId: MainDBKey[Option[BigInt]] = {
    MainDBKey.opt("last-non-empty-role-address-id", AddressCF, bytes(LastNonEmptyRoleAddressPrefix, Array.emptyByteArray), BigInt(_), _.toByteArray)
  }

  def nonEmptyRoleAddressId(address: Address): MainDBKey[Option[BigInt]] = {
    MainDBKey.opt("non-empty-role-address-id", AddressCF, hash(NonEmptyRoleAddressIdPrefix, address.bytes), BigInt(_), _.toByteArray)
  }

  def idToNonEmptyRoleAddress(id: BigInt): MainDBKey[Address] = {
    MainDBKey("non-empty-role-address", AddressCF, addr(NonEmptyRoleAddressPrefix, id), Address.fromBytes(_).explicitGet(), _.bytes.arr)
  }

  def issuedAliasesByAddressId(addressId: BigInt, storage: MainRocksDBStorage): MainRocksDBSet[Alias] = {
    RocksDBSet.newMain(
      name = "issued-aliases-by-address-id",
      columnFamily = AddressCF,
      prefix = Shorts.toByteArray(IssuedAliasesByAddressPrefix) ++ addressId.toByteArray,
      storage = storage,
      itemEncoder = _.bytes.arr,
      itemDecoder = Alias.fromBytes(_).explicitGet()
    )
  }
}
