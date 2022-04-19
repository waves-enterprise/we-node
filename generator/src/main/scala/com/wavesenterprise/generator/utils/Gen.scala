package com.wavesenterprise.generator.utils

import com.wavesenterprise.account.{Address, PrivateKeyAccount}
import com.wavesenterprise.acl.{OpType, PermissionOp, Role}
import com.wavesenterprise.crypto.KeyLength
import com.wavesenterprise.generator.utils.Implicits._
import com.wavesenterprise.state.{BinaryDataEntry, BooleanDataEntry, ByteStr, DataEntry, IntegerDataEntry, StringDataEntry}
import com.wavesenterprise.transaction.Transaction
import com.wavesenterprise.transaction.smart.script.{Script, ScriptCompiler}
import com.wavesenterprise.transaction.transfer.{ParsedTransfer, _}
import com.wavesenterprise.transaction.validation.TransferValidation
import com.wavesenterprise.utils.LoggerFacade
import org.slf4j.LoggerFactory

import java.util.concurrent.ThreadLocalRandom

object Gen {
  private def random = ThreadLocalRandom.current

  val log = LoggerFacade(LoggerFactory.getLogger("Gen"))

  def script(complexity: Boolean = true): Script = {
    val s = if (complexity) s"""
                               |${(for (b <- 1 to 10) yield {
                                 s"let a$b = blake2b256(base58'') != base58'' && keccak256(base58'') != base58'' && sha256(base58'') != base58'' && sigVerify(base58'333', base58'123', base58'567')"
                               }).mkString("\n")}
                               |
                               |${(for (b <- 1 to 10) yield { s"a$b" }).mkString("&&")} || true
       """.stripMargin
    else
      s"""
        |${recString(10)} || true
      """.stripMargin

    val script = ScriptCompiler(s, isAssetScript = false).explicitGet()

    script._1
  }

  def recString(n: Int): String =
    if (n <= 1) "true"
    else
      s"if (${recString(n - 1)}) then true else false"

  def oracleScript(oracle: PrivateKeyAccount, data: Set[DataEntry[_]]): Script = {
    val conditions =
      data.map {
        case IntegerDataEntry(key, value) => s"""(extract(getInteger(oracle, "$key")) == $value)"""
        case BooleanDataEntry(key, _)     => s"""extract(getBoolean(oracle, "$key"))"""
        case BinaryDataEntry(key, value)  => s"""(extract(getBinary(oracle, "$key")) == $value)"""
        case StringDataEntry(key, value)  => s"""(extract(getString(oracle, "$key")) == "$value")"""
      } reduce [String] { case (l, r) => s"$l && $r " }

    val src =
      s"""
         |let oracle = Address(base58'${oracle.address}')
         |
         |match tx {
         |  case _: SetScriptTransaction => true
         |  case _                       => $conditions
         |}
       """.stripMargin

    val script = ScriptCompiler(src, isAssetScript = false).explicitGet()

    script._1
  }

  def multiSigScript(owners: Seq[PrivateKeyAccount], requiredProofsCount: Int): Script = {
    val accountsWithIndexes = owners.zipWithIndex
    val keyLets =
      accountsWithIndexes map {
        case (acc, i) =>
          s"let accountPK$i = base58'${ByteStr(acc.publicKey.getEncoded).base58}'"
      } mkString "\n"

    val signedLets =
      accountsWithIndexes map {
        case (_, i) =>
          s"let accountSigned$i = if(sigVerify(tx.bodyBytes, tx.proofs[$i], accountPK$i)) then 1 else 0"
      } mkString "\n"

    val proofSum = accountsWithIndexes map {
      case (_, ind) =>
        s"accountSigned$ind"
    } mkString ("let proofSum = ", " + ", "")

    val finalStatement = s"proofSum >= $requiredProofsCount"

    val src =
      s"""
       |$keyLets
       |
       |$signedLets
       |
       |$proofSum
       |
       |$finalStatement
      """.stripMargin

    val (script, _) = ScriptCompiler(src, isAssetScript = false)
      .explicitGet()
    log.info(s"${script.text}")
    script
  }

  def randomFrom[T](c: Seq[T]): Option[T] = if (c.nonEmpty) Some(c(random.nextInt(c.size))) else None

  def txs(minFee: Long, maxFee: Long, senderAccounts: Seq[PrivateKeyAccount], recipientGen: Iterator[Address]): Iterator[Transaction] = {
    val senderGen = Iterator.randomContinually(senderAccounts)
    val feeGen    = Iterator.continually(minFee + random.nextLong(maxFee - minFee))
    transfers(senderGen, recipientGen, feeGen)
  }

  def transfers(senderGen: Iterator[PrivateKeyAccount], recipientGen: Iterator[Address], feeGen: Iterator[Long]): Iterator[Transaction] = {
    senderGen
      .zip(recipientGen)
      .zip(feeGen)
      .map {
        case ((src, dst), fee) =>
          TransferTransactionV2.selfSigned(src, None, None, System.currentTimeMillis(), fee, fee, dst, Array.emptyByteArray)
      }
      .collect { case Right(x) => x }
  }

  def massTransfers(senderGen: Iterator[PrivateKeyAccount], recipientGen: Iterator[Address], amountGen: Iterator[Long]): Iterator[Transaction] = {
    val transferCountGen = Iterator.continually(random.nextInt(TransferValidation.MaxTransferCount + 1))
    senderGen
      .zip(transferCountGen)
      .map {
        case (sender, count) =>
          val transfers = List.tabulate(count)(_ => ParsedTransfer(recipientGen.next(), amountGen.next()))
          val fee       = 100000 + count * 50000
          MassTransferTransactionV1.selfSigned(sender, None, transfers, System.currentTimeMillis, fee, Array.emptyByteArray)
      }
      .collect { case Right(tx) => tx }
  }

  val address: Iterator[Address] = Iterator.continually {
    val pk = Array.fill[Byte](KeyLength)(random.nextInt(Byte.MaxValue).toByte)
    Address.fromPublicKey(pk)
  }

  def address(uniqNumber: Int): Iterator[Address] = Iterator.randomContinually(address.take(uniqNumber).toSeq)

  def address(limitUniqNumber: Option[Int]): Iterator[Address] = limitUniqNumber.map(address(_)).getOrElse(address)

  private val permissionOpTypes = OpType.Add :: Nil
  val roles: List[Role] = Role.Miner ::
    Role.Issuer ::
    Role.Dexer ::
    Role.Permissioner ::
    Role.Blacklister ::
    Role.ContractDeveloper :: Nil

  def genPermissionOp(timestamp: Long): Option[PermissionOp] =
    for {
      permissionOp <- randomFrom(permissionOpTypes)
      role         <- randomFrom(roles)
      dueTimestampOpt <- randomFrom(Seq(0, 1)).map {
        case 1 => Some(System.currentTimeMillis() + 100000)
        case _ => None
      }
    } yield PermissionOp(permissionOp, role, timestamp, dueTimestampOpt)
}
