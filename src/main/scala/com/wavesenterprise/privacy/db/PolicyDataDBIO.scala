package com.wavesenterprise.privacy.db

import com.wavesenterprise.state.ByteStr
import slick.jdbc.{JdbcProfile, JdbcType}

class PolicyDataDBIO(protected val driver: JdbcProfile) extends CommonSlickTrait {

  import driver.api._

  implicit private val policyDataColumn: JdbcType[ByteStr] =
    MappedColumnType.base[ByteStr, Array[Byte]](policyData => policyData.arr, column => ByteStr(column))

  class PolicyDataTable(tag: Tag) extends Table[PolicyData](tag, PolicyDataTable.TableName) {

    def policyId: Rep[String] = column[String](PolicyDataTable.PolicyId)
    def hash: Rep[String]     = column[String](PolicyDataTable.Hash, O.PrimaryKey)
    def data: Rep[ByteStr]    = column[ByteStr](PolicyDataTable.Data)

    def * = (policyId, hash, data).mapTo[PolicyData]
  }

  object PolicyDataTable {
    val TableName = "policy_data"
    val PolicyId  = "policy_id"
    val Hash      = "hash"
    val Data      = "data"
  }

  lazy val policyDataTable = TableQuery[PolicyDataTable]

  def all(): DBIO[Seq[PolicyData]] =
    policyDataTable.result

  def insert(policyId: String, hash: String, data: ByteStr): DBIO[Int] =
    policyDataTable += PolicyData(policyId, hash, data)

  def deleteAll(): DBIO[Int] =
    policyDataTable.delete

  def delete(policyId: String, hash: String): DBIO[Int] =
    policyDataTable
      .filter(entry => entry.policyId === policyId && entry.hash === hash)
      .delete

  def find(policyId: String, hash: String): DBIO[Option[ByteStr]] =
    policyDataTable
      .filter { data =>
        data.policyId === policyId &&
        data.hash === hash
      }
      .map(_.data)
      .result
      .headOption
}
