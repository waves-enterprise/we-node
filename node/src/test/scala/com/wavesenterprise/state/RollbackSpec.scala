package com.wavesenterprise.state

import java.nio.charset.StandardCharsets.UTF_8
import com.google.common.base.Charsets
import com.wavesenterprise.account.{Address, AddressScheme, PrivateKeyAccount}
import com.wavesenterprise.acl.PermissionsGen._
import com.wavesenterprise.acl.{OpType, PermissionOp, Role}
import com.wavesenterprise.block.Block
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.consensus.ConsensusPostAction
import com.wavesenterprise.crypto.SignatureLength
import com.wavesenterprise.db.WithDomain
import com.wavesenterprise.docker.ContractApiVersion
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.history.{DefaultBlockchainSettings, DefaultWESettings}
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.lang.v1.compiler.Terms.TRUE
import com.wavesenterprise.privacy.PolicyDataHash
import com.wavesenterprise.settings.{FunctionalitySettings, TestFees, TestFunctionalitySettings, WESettings}
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext.Default
import com.wavesenterprise.state.reader.LeaseDetails
import com.wavesenterprise.transaction.ValidationError.AliasDoesNotExist
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.acl.{PermitTransaction, PermitTransactionV1, PermitTransactionV2}
import com.wavesenterprise.transaction.assets._
import com.wavesenterprise.transaction.docker._
import com.wavesenterprise.transaction.docker.assets.ContractAssetOperation.{
  ContractBurnV1,
  ContractIssueV1,
  ContractReissueV1,
  ContractTransferOutV1
}
import com.wavesenterprise.transaction.docker.assets.ContractTransferInV1
import com.wavesenterprise.transaction.lease._
import com.wavesenterprise.transaction.smart.script.v1.ScriptV1
import com.wavesenterprise.transaction.smart.{SetScriptTransaction, SetScriptTransactionV1}
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.utils.NumberUtils.DoubleExt
import com.wavesenterprise.{NoShrink, TestTime, TransactionGen, crypto, history}
import org.apache.commons.codec.digest.DigestUtils
import org.scalacheck.Gen
import org.scalatest.Assertions
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import tools.GenHelper._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class RollbackSpec extends AnyFreeSpec with Matchers with WithDomain with TransactionGen with ScalaCheckPropertyChecks with NoShrink {
  private val time    = new TestTime
  private def nextTs  = time.getTimestamp()
  private val chainId = AddressScheme.getAddressSchema.chainId

  private val createAliasFee    = TestFees.defaultFees.forTxType(CreateAliasTransaction.typeId)
  private val issueFee          = TestFees.defaultFees.forTxType(IssueTransaction.typeId)
  private val reissueFee        = TestFees.defaultFees.forTxType(ReissueTransaction.typeId)
  private val transferFee       = TestFees.defaultFees.forTxType(TransferTransaction.typeId)
  private val massTxFee         = TestFees.defaultFees.forTxType(MassTransferTransaction.typeId)
  private val massTxPerTransfer = TestFees.defaultFees.forTxTypeAdditional(MassTransferTransaction.typeId)
  private val leaseFee          = TestFees.defaultFees.forTxType(LeaseTransaction.typeId)
  private val leaseCancelFee    = TestFees.defaultFees.forTxType(LeaseCancelTransaction.typeId)
  private val setScriptFee      = TestFees.defaultFees.forTxType(SetScriptTransaction.typeId)
  private val registerNodeFee   = TestFees.defaultFees.forTxType(RegisterNodeTransactionV1.typeId)
  private val permitTxFee       = TestFees.defaultFees.forTxType(PermitTransaction.typeId)
  private val createPolicyFee   = TestFees.defaultFees.forTxType(CreatePolicyTransactionV1.typeId)
  private val updatePolicyFee   = TestFees.defaultFees.forTxType(CreatePolicyTransactionV1.typeId)
  private val policyDataHashFee = TestFees.defaultFees.forTxType(PolicyDataHashTransactionV1.typeId)
  private val createContractFee = TestFees.defaultFees.forTxType(CreateContractTransaction.typeId)
  private val callContractFee   = TestFees.defaultFees.forTxType(CallContractTransaction.typeId)
  private val updateContractFee = TestFees.defaultFees.forTxType(UpdateContractTransaction.typeId)

  private def genesisBlock(genesisTs: Long, address: Address, initialBalance: Long, roles: Seq[Role] = Seq.empty): Block = {
    genesisBlock(genesisTs, Set(address), initialBalance, Map(address -> roles))
  }

  private def genesisBlock(genesisTs: Long, addresses: Set[Address], initialBalance: Long, roles: Map[Address, Seq[Role]]): Block = {
    val genesisPermitTxs = roles.flatMap {
      case (address, rolesSeq) =>
        rolesSeq.zipWithIndex.map {
          case (role, offset) =>
            GenesisPermitTransaction.create(address, role, genesisTs + offset).explicitGet()
        }
    }.toSeq
    val genesisTxs = addresses.map(GenesisTransaction.create(_, initialBalance, genesisTs).explicitGet()).toSeq
    TestBlock.create(
      genesisTs,
      ByteStr(Array.fill[Byte](SignatureLength)(0)),
      genesisTxs ++ genesisPermitTxs
    )
  }

  private def transfer(sender: PrivateKeyAccount, recipient: Address, amount: Long) =
    TransferTransactionV2.selfSigned(sender, None, None, nextTs, amount, transferFee, recipient, Array.empty[Byte]).explicitGet()

  private def randomOp(sender: PrivateKeyAccount, recipient: Address, amount: Long, op: Int, nextTs: => Long = nextTs) = {
    import com.wavesenterprise.transaction.transfer.ParsedTransfer
    op match {
      case 1 =>
        val lease = LeaseTransactionV2.selfSigned(None, sender, recipient, amount, leaseFee, nextTs).explicitGet()
        List(
          lease,
          LeaseCancelTransactionV2
            .selfSigned(chainId = AddressScheme.getAddressSchema.chainId, sender, leaseCancelFee, nextTs, lease.id())
            .explicitGet()
        )

      case 2 =>
        List(
          MassTransferTransactionV1
            .selfSigned(sender,
                        None,
                        List(ParsedTransfer(recipient, amount), ParsedTransfer(recipient, amount)),
                        nextTs,
                        massTxFee + (2 * massTxPerTransfer),
                        Array.empty[Byte])
            .explicitGet())
      case 3 =>
        List(
          MassTransferTransactionV2
            .selfSigned(sender,
                        None,
                        List(ParsedTransfer(recipient, amount), ParsedTransfer(recipient, amount)),
                        nextTs,
                        massTxFee + (2 * massTxPerTransfer),
                        Array.empty[Byte],
                        None)
            .explicitGet())

      case _ =>
        List(TransferTransactionV2.selfSigned(sender, None, None, nextTs, amount, transferFee, recipient, Array.empty[Byte]).explicitGet())
    }
  }

  private def stringGen(length: Int): Gen[String] = {
    byteArrayGen(length).map(bytes => new String(bytes, UTF_8))
  }

  "Rollback resets" - {
    "Rollback save dropped blocks order" in forAll(accountGen, positiveLongGen, Gen.choose(1, 10)) {
      case (sender, initialBalance, blocksCount) =>
        withDomain() { d =>
          d.appendBlock(genesisBlock(nextTs, sender.toAddress, initialBalance))
          val genesisSignature = d.lastBlockId
          def newBlocks(i: Int): List[ByteStr] = {
            if (i == blocksCount) {
              Nil
            } else {
              val block = TestBlock.create(nextTs + i, d.lastBlockId, Seq())
              d.appendBlock(block)
              block.uniqueId :: newBlocks(i + 1)
            }
          }
          val blocks        = newBlocks(0)
          val droppedBlocks = d.removeAfter(genesisSignature)
          droppedBlocks.head.reference shouldBe genesisSignature
          droppedBlocks.map(_.uniqueId).toList shouldBe blocks
          droppedBlocks foreach { b =>
            d.appendBlock(b, ConsensusPostAction.NoAction)
          }
        }
    }

    "blockchain-feature" in forAll(accountGen, positiveLongGen, Gen.choose(2, 30), Gen.choose(0, 3), Gen.oneOf(BlockchainFeature.implemented)) {
      case (sender, initialBalance, blocksForFeatureActivation, checkAddendum, testFeatureIndex) =>
        val featureCheckBlocksPeriod = blocksForFeatureActivation + checkAddendum
        val functionalitySettings    = FunctionalitySettings(featureCheckBlocksPeriod, blocksForFeatureActivation)
        val blockchainSettings       = DefaultBlockchainSettings.custom.copy(functionality = functionalitySettings)
        val testFeature              = BlockchainFeature.withValueOpt(testFeatureIndex).get
        val approveHeight            = if (featureCheckBlocksPeriod > blocksForFeatureActivation) featureCheckBlocksPeriod else featureCheckBlocksPeriod * 2
        val activationHeight         = approveHeight + featureCheckBlocksPeriod

        withDomain(DefaultWESettings.copy(blockchain = DefaultBlockchainSettings.copy(custom = blockchainSettings))) { domain =>
          val firstBlock = genesisBlock(nextTs, sender.toAddress, initialBalance)
          domain.appendBlock(firstBlock)

          (1 to approveHeight).foreach { i =>
            domain.appendBlock {
              TestBlock.create(nextTs + i, domain.lastBlockId, Seq.empty, version = Block.NgBlockVersion, features = Set(testFeature.id))
            }
          }

          (approveHeight to activationHeight).foreach { i =>
            domain.appendBlock {
              TestBlock.create(nextTs + i, domain.lastBlockId, Seq.empty, version = Block.NgBlockVersion)
            }
          }

          domain.blockchainUpdater.approvedFeatures.get(testFeature.id) shouldBe Some(approveHeight)
          domain.blockchainUpdater.activatedFeatures.get(testFeature.id) shouldBe Some(activationHeight)

          domain.removeAfter(firstBlock.signerData.signature)

          domain.blockchainUpdater.approvedFeatures.get(testFeature.id) shouldBe None
          domain.blockchainUpdater.activatedFeatures.get(testFeature.id) shouldBe None

          (1 to approveHeight).foreach { i =>
            domain.appendBlock {
              TestBlock.create(nextTs + i, domain.lastBlockId, Seq.empty, version = Block.NgBlockVersion, features = Set(testFeature.id))
            }
          }

          domain.blockchainUpdater.approvedFeatures.get(testFeature.id) shouldBe Some(approveHeight)
          domain.blockchainUpdater.activatedFeatures.get(testFeature.id) shouldBe Some(activationHeight)
        }
    }

    "forget rollbacked transaction for querying" in forAll(accountGen, addressGen, Gen.nonEmptyListOf(Gen.choose(1, 10))) {
      case (sender, recipient, txCount) =>
        withDomain(createSettings()) { d =>
          d.appendBlock(genesisBlock(nextTs, sender.toAddress, com.wavesenterprise.state.diffs.ENOUGH_AMT))

          val genesisSignature = d.lastBlockId

          val transferAmount = 100

          val transfers = txCount.map(tc => Seq.fill(tc)(randomOp(sender, recipient, transferAmount, tc % 4)).flatten)

          for (transfer <- transfers) {
            d.appendBlock(
              TestBlock.create(
                nextTs,
                d.lastBlockId,
                transfer
              ))
          }

          val stransactions1 = d.addressTransactions(sender.toAddress).explicitGet().sortBy(_._2.timestamp)
          val rtransactions1 = d.addressTransactions(recipient).explicitGet().sortBy(_._2.timestamp)

          d.removeAfter(genesisSignature)

          for (transfer <- transfers) {
            d.appendBlock(
              TestBlock.create(
                nextTs,
                d.lastBlockId,
                transfer
              ))
          }

          val stransactions2 = d.addressTransactions(sender.toAddress).explicitGet().sortBy(_._2.timestamp)
          val rtransactions2 = d.addressTransactions(recipient).explicitGet().sortBy(_._2.timestamp)

          stransactions1 shouldBe stransactions2
          rtransactions1 shouldBe rtransactions2
        }
    }

    "WEST balances" in forAll(accountGen, positiveLongGen, addressGen, Gen.nonEmptyListOf(Gen.choose(1, 10))) {
      case (sender, initialBalance, recipient, txCount) =>
        withDomain() { d =>
          d.appendBlock(genesisBlock(nextTs, sender.toAddress, initialBalance))

          val genesisSignature = d.lastBlockId

          d.portfolio(sender.toAddress).balance shouldBe initialBalance
          d.portfolio(recipient).balance shouldBe 0

          val totalTxCount   = txCount.sum
          val transferAmount = initialBalance / (totalTxCount * 2)

          for (tc <- txCount) {
            d.appendBlock(
              TestBlock.create(
                nextTs,
                d.lastBlockId,
                Seq.fill(tc)(transfer(sender, recipient, transferAmount))
              ))
          }

          d.portfolio(recipient).balance shouldBe (transferAmount * totalTxCount)
          d.portfolio(sender.toAddress).balance shouldBe (initialBalance - (transferAmount + transferFee) * totalTxCount)

          d.removeAfter(genesisSignature)

          d.portfolio(sender.toAddress).balance shouldBe initialBalance
          d.portfolio(recipient).balance shouldBe 0
        }
    }

    "lease balances and states" in forAll(accountGen, addressGen) {
      case (sender, recipient) =>
        withDomain() { d =>
          val initialBalance = 1000.west
          d.appendBlock(genesisBlock(nextTs, sender.toAddress, initialBalance))
          val genesisBlockId = d.lastBlockId

          val leaseAmount = initialBalance - 2 - leaseFee
          val lt          = LeaseTransactionV2.selfSigned(None, sender, recipient, leaseAmount, leaseFee, nextTs).explicitGet()
          d.appendBlock(TestBlock.create(nextTs, genesisBlockId, Seq(lt)))
          val blockWithLeaseId = d.lastBlockId
          d.blockchainUpdater.leaseDetails(LeaseId(lt.id())) should contain(LeaseDetails(sender, recipient, 2, leaseAmount, true, None))
          d.portfolio(sender.toAddress).lease.out shouldEqual leaseAmount
          d.portfolio(recipient).lease.in shouldEqual leaseAmount

          val leaseCancelTransactionBlock =
            TestBlock.create(
              nextTs,
              blockWithLeaseId,
              Seq(
                LeaseCancelTransactionV2
                  .selfSigned(chainId = AddressScheme.getAddressSchema.chainId, sender, leaseCancelFee, nextTs, lt.id())
                  .explicitGet())
            )
          d.appendBlock(leaseCancelTransactionBlock)
          d.appendBlock(TestBlock.create(nextTs, leaseCancelTransactionBlock.uniqueId, Seq.empty))

          d.blockchainUpdater.leaseDetails(LeaseId(lt.id())) should contain(LeaseDetails(sender, recipient, 2, leaseAmount, false, None))
          d.portfolio(sender.toAddress).lease.out shouldEqual 0
          d.portfolio(recipient).lease.in shouldEqual 0

          d.removeAfter(blockWithLeaseId)
          d.blockchainUpdater.leaseDetails(LeaseId(lt.id())) should contain(LeaseDetails(sender, recipient, 2, leaseAmount, true, None))
          d.portfolio(sender.toAddress).lease.out shouldEqual leaseAmount
          d.portfolio(recipient).lease.in shouldEqual leaseAmount

          d.removeAfter(genesisBlockId)
          d.blockchainUpdater.leaseDetails(LeaseId(lt.id())) shouldBe 'empty
          d.portfolio(sender.toAddress).lease.out shouldEqual 0
          d.portfolio(recipient).lease.in shouldEqual 0
        }
    }

    "asset balances" in forAll(accountGen, positiveLongGen, positiveLongGen, addressGen) {
      case (sender, initialBalance, assetAmount, recipient) =>
        withDomain() { d =>
          d.appendBlock(genesisBlock(nextTs, sender.toAddress, initialBalance, roles = Seq(Role.Issuer)))
          val genesisBlockId = d.lastBlockId
          val issueTransaction =
            IssueTransactionV2
              .selfSigned(
                chainId = AddressScheme.getAddressSchema.chainId,
                sender,
                "test".getBytes(UTF_8),
                Array.empty[Byte],
                assetAmount,
                8,
                true,
                issueFee,
                nextTs,
                None
              )
              .explicitGet()

          d.appendBlock(
            TestBlock.create(
              nextTs,
              genesisBlockId,
              Seq(issueTransaction)
            ))

          val blockIdWithIssue = d.lastBlockId

          d.portfolio(sender.toAddress).assets.get(issueTransaction.id()) should contain(assetAmount)
          d.portfolio(recipient).assets.get(issueTransaction.id()) shouldBe 'empty

          val transferTransactionBlock = TestBlock.create(
            nextTs,
            d.lastBlockId,
            Seq(
              TransferTransactionV2
                .selfSigned(sender, Some(issueTransaction.id()), None, nextTs, assetAmount, transferFee, recipient, Array.empty[Byte])
                .explicitGet())
          )
          d.appendBlock(transferTransactionBlock)
          d.appendBlock(TestBlock.create(nextTs, transferTransactionBlock.uniqueId, Seq.empty))

          d.portfolio(sender.toAddress).assets.getOrElse(issueTransaction.id(), 0) shouldEqual 0
          d.portfolio(recipient).assets.getOrElse(issueTransaction.id(), 0) shouldEqual assetAmount

          d.removeAfter(blockIdWithIssue)

          d.portfolio(sender.toAddress).assets.getOrElse(issueTransaction.id(), 0) shouldEqual assetAmount
          d.portfolio(recipient).assets.getOrElse(issueTransaction.id(), 0) shouldEqual 0
        }
    }

    "asset quantity and reissuability" in forAll(accountGen, positiveLongGen, stringGen(4), stringGen(10)) {
      case (sender, initialBalance, name, description) =>
        withDomain() { d =>
          d.appendBlock(genesisBlock(nextTs, sender.toAddress, initialBalance, roles = Seq(Role.Issuer)))
          val genesisBlockId = d.lastBlockId

          val issueTransaction = IssueTransactionV2
            .selfSigned(chainId = AddressScheme.getAddressSchema.chainId,
                        sender,
                        name.getBytes(UTF_8),
                        description.getBytes(UTF_8),
                        2000,
                        8,
                        true,
                        issueFee,
                        nextTs,
                        None)
            .explicitGet()
          d.blockchainUpdater.assetDescription(issueTransaction.id()) shouldBe 'empty

          d.appendBlock(
            TestBlock.create(
              nextTs,
              genesisBlockId,
              Seq(issueTransaction)
            ))

          val blockIdWithIssue = d.lastBlockId

          d.blockchainUpdater.assetDescription(issueTransaction.id()) should contain(
            AssetDescription(sender.toAddress.toAssetHolder,
                             2,
                             issueTransaction.timestamp,
                             name,
                             description,
                             8,
                             reissuable = true,
                             BigInt(2000),
                             None,
                             sponsorshipIsEnabled = false))

          val reissueTransactionBlock =
            TestBlock.create(
              nextTs,
              blockIdWithIssue,
              Seq(
                ReissueTransactionV2
                  .selfSigned(chainId = AddressScheme.getAddressSchema.chainId,
                              sender,
                              issueTransaction.id(),
                              2000,
                              reissuable = false,
                              reissueFee,
                              nextTs)
                  .explicitGet()
              )
            )
          d.appendBlock(reissueTransactionBlock)
          d.appendBlock(TestBlock.create(nextTs, reissueTransactionBlock.uniqueId, Seq.empty))

          d.blockchainUpdater.assetDescription(issueTransaction.id()) should contain(
            AssetDescription(sender.toAddress.toAssetHolder,
                             2,
                             issueTransaction.timestamp,
                             name,
                             description,
                             8,
                             reissuable = false,
                             BigInt(4000),
                             None,
                             sponsorshipIsEnabled = false))

          d.removeAfter(blockIdWithIssue)
          d.blockchainUpdater.assetDescription(issueTransaction.id()) should contain(
            AssetDescription(sender.toAddress.toAssetHolder,
                             2,
                             issueTransaction.timestamp,
                             name,
                             description,
                             8,
                             reissuable = true,
                             BigInt(2000),
                             None,
                             sponsorshipIsEnabled = false))

          d.removeAfter(genesisBlockId)
          d.blockchainUpdater.assetDescription(issueTransaction.id()) shouldBe 'empty
        }
    }

    "aliases" in forAll(accountGen, accountGen, positiveLongGen, aliasGen, aliasGen) {
      case (sender1, sender2, initialBalance, alias1, alias2) =>
        withDomain(createSettings()) { d =>
          d.appendBlock(genesisBlock(nextTs, Set(sender1.toAddress, sender2.toAddress), initialBalance, Map.empty[Address, Seq[Role]]))
          val genesisBlockId = d.lastBlockId

          d.blockchainUpdater.resolveAlias(alias1) shouldBe Left(AliasDoesNotExist(alias1))
          d.blockchainUpdater.resolveAlias(alias2) shouldBe Left(AliasDoesNotExist(alias2))
          val createAliasBlock =
            TestBlock.create(
              nextTs,
              genesisBlockId,
              Seq(
                CreateAliasTransactionV2.selfSigned(sender1, alias1, createAliasFee, nextTs).explicitGet(),
                CreateAliasTransactionV3.selfSigned(sender2, alias2, createAliasFee, nextTs, None).explicitGet()
              )
            )
          d.appendBlock(createAliasBlock)
          d.appendBlock(TestBlock.create(nextTs, createAliasBlock.uniqueId, Seq.empty))

          d.blockchainUpdater.resolveAlias(alias1) shouldBe Right(sender1.toAddress)
          d.blockchainUpdater.resolveAlias(alias2) shouldBe Right(sender2.toAddress)
          d.removeAfter(genesisBlockId)

          d.blockchainUpdater.resolveAlias(alias1) shouldBe Left(AliasDoesNotExist(alias1))
          d.blockchainUpdater.resolveAlias(alias2) shouldBe Left(AliasDoesNotExist(alias2))
        }
    }

    "data transaction" in forAll(accountGen, positiveLongGen, dataEntryGen(1000)) {
      case (sender, initialBalance, dataEntry) =>
        withDomain(createSettings()) { d =>
          d.appendBlock(genesisBlock(nextTs, sender.toAddress, initialBalance))
          val genesisBlockId = d.lastBlockId

          val dataTransactionBlock =
            TestBlock.create(
              nextTs,
              genesisBlockId,
              Seq(
                DataTransactionV1.selfSigned(sender, sender, List(dataEntry), nextTs, 10.west).explicitGet(),
                DataTransactionV2.selfSigned(sender, sender, List(dataEntry), nextTs, 10.west, None).explicitGet()
              )
            )
          d.appendBlock(dataTransactionBlock)
          d.appendBlock(TestBlock.create(nextTs, dataTransactionBlock.uniqueId, Seq.empty))

          d.blockchainUpdater.accountData(sender.toAddress, dataEntry.key) should contain(dataEntry)

          d.removeAfter(genesisBlockId)
          d.blockchainUpdater.accountData(sender.toAddress, dataEntry.key) shouldBe 'empty
        }
    }

    "address script" in forAll(accountGen, positiveLongGen) {
      case (sender, initialBalance) =>
        withDomain(createSettings()) { d =>
          d.appendBlock(genesisBlock(nextTs, sender.toAddress, initialBalance))
          val script = ScriptV1(TRUE).explicitGet()

          val genesisBlockId = d.lastBlockId
          d.blockchainUpdater.accountScript(sender.toAddress) shouldBe 'empty
          d.appendBlock(
            TestBlock.create(
              nextTs,
              genesisBlockId,
              Seq(
                SetScriptTransactionV1
                  .selfSigned(chainId, sender, Some(script), "script".getBytes(Charsets.UTF_8), Array.empty[Byte], setScriptFee, nextTs)
                  .explicitGet())
            ))

          val blockWithScriptId = d.lastBlockId

          d.blockchainUpdater.accountScript(sender.toAddress) should contain(script)

          val setScriptTransactionBlock = TestBlock.create(
            nextTs,
            blockWithScriptId,
            Seq(
              SetScriptTransactionV1
                .selfSigned(chainId, sender, None, "script".getBytes(Charsets.UTF_8), Array.empty[Byte], setScriptFee, nextTs)
                .explicitGet())
          )
          d.appendBlock(setScriptTransactionBlock)
          d.appendBlock(TestBlock.create(nextTs, setScriptTransactionBlock.uniqueId, Seq.empty))

          d.blockchainUpdater.accountScript(sender.toAddress) shouldBe 'empty

          d.removeAfter(blockWithScriptId)
          d.blockchainUpdater.accountScript(sender.toAddress) should contain(script)

          d.removeAfter(genesisBlockId)
          d.blockchainUpdater.accountScript(sender.toAddress) shouldBe 'empty
        }
    }

    def createSettings(): WESettings = {
      val tfs = TestFunctionalitySettings.Enabled.copy(
        preActivatedFeatures = BlockchainFeature.implemented
          .map(id => id -> 0)
          .toMap,
        blocksForFeatureActivation = 1,
        featureCheckBlocksPeriod = 2
      )

      history.DefaultWESettings.copy(
        blockchain = history.DefaultWESettings.blockchain.copy(
          custom = history.DefaultWESettings.blockchain.custom.copy(functionality = tfs)
        )
      )
    }

    "asset sponsorship" in forAll(for {
      sender      <- accountGen
      sponsorship <- sponsorFeeCancelSponsorFeeGen(sender)
    } yield {
      (sender, sponsorship)
    }) {
      case (sender, (issueTransaction, sponsor1, sponsor2, cancel, _)) =>
        val ts = issueTransaction.timestamp
        withDomain(createSettings()) { d =>
          d.appendBlock(genesisBlock(ts, sender.toAddress, Long.MaxValue / 3, roles = Seq(Role.Issuer)))
          val genesisBlockId = d.lastBlockId

          d.appendBlock(
            TestBlock.create(
              ts,
              genesisBlockId,
              Seq(issueTransaction)
            ))

          val blockIdWithIssue = d.lastBlockId
          d.appendBlock(TestBlock.create(nextTs, blockIdWithIssue, Seq.empty))

          d.appendBlock(
            TestBlock.create(
              ts + 2,
              d.lastBlockId,
              Seq(sponsor1)
            ))

          val blockIdWithSponsor = d.lastBlockId
          d.appendBlock(TestBlock.create(nextTs, blockIdWithSponsor, Seq.empty))

          d.blockchainUpdater.assetDescription(sponsor1.assetId).get.sponsorshipIsEnabled shouldBe true
          d.portfolio(sender.toAddress).assets.get(issueTransaction.id()) should contain(issueTransaction.quantity)

          val cancelBlock = TestBlock.create(
            ts + 2,
            d.lastBlockId,
            Seq(cancel)
          )
          d.appendBlock(cancelBlock)
          d.appendBlock(TestBlock.create(nextTs, cancelBlock.uniqueId, Seq.empty))
          d.blockchainUpdater.assetDescription(sponsor1.assetId).get.sponsorshipIsEnabled shouldBe false

          d.removeAfter(blockIdWithSponsor)

          d.blockchainUpdater.assetDescription(sponsor1.assetId).get.sponsorshipIsEnabled shouldBe true
          d.portfolio(sender.toAddress).assets.get(issueTransaction.id()) should contain(issueTransaction.quantity)

          val sponsor2Block = TestBlock.create(
            ts + 2,
            d.lastBlockId,
            Seq(sponsor2)
          )
          d.appendBlock(sponsor2Block)
          d.appendBlock(TestBlock.create(nextTs, sponsor2Block.uniqueId, Seq.empty))

          d.portfolio(sender.toAddress).assets.get(issueTransaction.id()) should contain(issueTransaction.quantity)
          d.blockchainUpdater.assetDescription(sponsor1.assetId).get.sponsorshipIsEnabled shouldBe true

          d.removeAfter(blockIdWithIssue)

          d.blockchainUpdater.assetDescription(sponsor1.assetId).get.sponsorshipIsEnabled shouldBe false
        }
    }

    "permit transactions" - {
      "single permit tx" in forAll(accountGen, positiveLongGen, addressGen, roleGen) {
        case (sender, initialBalance, permitRecipient, role) =>
          withDomain(createSettings()) { d =>
            d.appendBlock(genesisBlock(nextTs, sender.toAddress, initialBalance, roles = Seq(Role.Permissioner)))

            val genesisSignature = d.lastBlockId

            val txTimestamp = nextTs

            val permitTx =
              PermitTransactionV1
                .selfSigned(sender, permitRecipient, txTimestamp, permitTxFee, PermissionOp(OpType.Add, role, txTimestamp, None))
                .explicitGet()

            val permitTxBlock = TestBlock.create(nextTs, genesisSignature, Seq(permitTx))
            d.appendBlock(permitTxBlock)
            d.appendBlock(TestBlock.create(nextTs, permitTxBlock.uniqueId, Seq.empty))

            d.blockchainUpdater.permissions(permitRecipient).active(nextTs).headOption shouldBe Some(role)

            d.removeAfter(genesisSignature)

            d.blockchainUpdater.permissions(permitRecipient).active(nextTs).headOption shouldBe None
          }
      }

      val recipientToRolesMapGen: Gen[Map[Address, List[Role]]] = for {
        recipients <- Gen.nonEmptyListOf(addressGen)
        roles      <- Gen.nonEmptyListOf(roleGen).map(_.distinct)
      } yield {
        recipients
          .flatMap(recipient => roles.map(role => recipient -> role))
          .groupBy(_._1)
          .mapValues(_.map(_._2))
      }

      "multiple permit txs" in forAll(accountGen, positiveLongGen, recipientToRolesMapGen) {
        case (permissioner, initialBalance, recipientToRoles) =>
          withDomain(createSettings()) { d =>
            d.appendBlock(genesisBlock(nextTs, permissioner.toAddress, initialBalance, roles = Seq(Role.Permissioner)))

            val genesisSignature = d.lastBlockId

            val txs = recipientToRoles.toSeq.flatMap {
              case (recipient, roles) =>
                roles.map { role =>
                  val txTimestamp = nextTs
                  PermitTransactionV1
                    .selfSigned(permissioner, recipient, txTimestamp, permitTxFee, PermissionOp(OpType.Add, role, txTimestamp, None))
                    .explicitGet()
                }
            }

            val txsBlock = TestBlock.create(nextTs, genesisSignature, txs, version = 3)
            d.appendBlock(txsBlock)
            d.appendBlock(TestBlock.create(nextTs, txsBlock.uniqueId, Seq.empty, version = 3))

            recipientToRoles.foreach {
              case (recipient, roles) =>
                d.blockchainUpdater.permissions(recipient).activeAsOps(nextTs).toSeq.map(_.role) should contain theSameElementsAs roles
            }

            d.removeAfter(genesisSignature)

            recipientToRoles.keys.foreach { recipient =>
              d.blockchainUpdater.permissions(recipient).activeAsOps(nextTs) shouldBe empty
            }

          }
      }
    }

    val nodeName = Some("NodeName")

    "register node transaction" - {
      "single tx" in forAll(accountGen, positiveLongGen, accountGen) {
        case (sender, initialBalance, accountToReg) =>
          withDomain(createSettings()) { d =>
            d.appendBlock(genesisBlock(nextTs, sender.toAddress, initialBalance, roles = Seq(Role.Permissioner, Role.ConnectionManager)))

            val genesisSignature = d.lastBlockId

            val regTx = RegisterNodeTransactionV1.selfSigned(sender, accountToReg, nodeName, OpType.Add, nextTs, registerNodeFee).explicitGet()

            val regTxBlock = TestBlock.create(nextTs, genesisSignature, Seq(regTx))
            d.appendBlock(regTxBlock)
            d.appendBlock(TestBlock.create(nextTs, regTxBlock.uniqueId, Seq.empty))

            d.blockchainUpdater.participantPubKey(accountToReg.toAddress) shouldBe Some(accountToReg)
            d.blockchainUpdater.networkParticipants() should contain theSameElementsAs Seq(accountToReg.toAddress)

            d.removeAfter(genesisSignature)

            d.blockchainUpdater.participantPubKey(accountToReg.toAddress) shouldBe None
            d.blockchainUpdater.networkParticipants() should contain theSameElementsAs Seq.empty[Address]
          }
      }
      "two tx" in forAll(accountGen, positiveLongGen, accountGen, accountGen) {
        case (sender, initialBalance, firstAccToReg, secondAccToReg) =>
          withDomain(createSettings()) { d =>
            d.appendBlock(genesisBlock(nextTs, sender.toAddress, initialBalance, roles = Seq(Role.Permissioner, Role.ConnectionManager)))

            val genesisSignature = d.lastBlockId

            val regTx1 = RegisterNodeTransactionV1.selfSigned(sender, firstAccToReg, nodeName, OpType.Add, nextTs, registerNodeFee).explicitGet()
            val regTx2 =
              RegisterNodeTransactionV1.selfSigned(sender, secondAccToReg, nodeName.map(_ + "2"), OpType.Add, nextTs, registerNodeFee).explicitGet()

            val twoTxBlock = TestBlock.create(nextTs, genesisSignature, Seq(regTx1, regTx2))
            d.appendBlock(twoTxBlock)
            d.appendBlock(TestBlock.create(nextTs, twoTxBlock.uniqueId, Seq.empty))

            d.blockchainUpdater.participantPubKey(firstAccToReg.toAddress) shouldBe Some(firstAccToReg)
            d.blockchainUpdater.participantPubKey(secondAccToReg.toAddress) shouldBe Some(secondAccToReg)
            d.blockchainUpdater.networkParticipants() should contain theSameElementsAs Seq(firstAccToReg, secondAccToReg).map(_.toAddress)

            d.removeAfter(genesisSignature)

            d.blockchainUpdater.participantPubKey(firstAccToReg.toAddress) shouldBe None
            d.blockchainUpdater.participantPubKey(secondAccToReg.toAddress) shouldBe None
            d.blockchainUpdater.networkParticipants() should contain theSameElementsAs Seq.empty
          }
      }
    }

    "create policy transaction" - {
      "single tx" in forAll(accountGen, positiveLongGen, severalAddressGenerator(), severalPrivKeysGenerator()) {
        case (sender, initialBalance, owners, recipients) =>
          withDomain(createSettings()) { d =>
            d.appendBlock(genesisBlock(nextTs, sender.toAddress, initialBalance, roles = Seq(Role.Permissioner, Role.ConnectionManager)))

            val genesisSignature = d.lastBlockId

            val regRecipientsAsParticipants = recipients.map { recipient =>
              RegisterNodeTransactionV1.selfSigned(sender, recipient, nodeName, OpType.Add, nextTs, registerNodeFee).explicitGet()
            }
            val blockWithRegNode = TestBlock.create(nextTs, genesisSignature, regRecipientsAsParticipants)
            d.appendBlock(blockWithRegNode)

            val recipientAddresses   = recipients.map(_.toAddress)
            val createPolicyTxOwners = sender.toAddress :: owners
            val createPolicyTransactionV1 =
              CreatePolicyTransactionV1
                .selfSigned(sender, "some name", "some description", recipientAddresses, createPolicyTxOwners, nextTs, createPolicyFee)
                .explicitGet()

            val createPolicyTransactionV2 =
              CreatePolicyTransactionV2
                .selfSigned(sender, "some name", "some description", recipientAddresses, createPolicyTxOwners, nextTs, createPolicyFee, None)
                .explicitGet()

            val policyV1Id = createPolicyTransactionV1.id.value
            val policyV2Id = createPolicyTransactionV2.id.value

            val createPolicyTransactionBlock =
              TestBlock.create(nextTs, blockWithRegNode.uniqueId, Seq(createPolicyTransactionV1, createPolicyTransactionV2))
            d.appendBlock(createPolicyTransactionBlock)
            d.appendBlock(TestBlock.create(nextTs, createPolicyTransactionBlock.uniqueId, Seq.empty))

            d.blockchainUpdater.policyExists(policyV1Id) shouldBe true
            d.blockchainUpdater.policyOwners(policyV1Id) should contain theSameElementsAs createPolicyTxOwners
            d.blockchainUpdater.policyRecipients(policyV1Id) should contain theSameElementsAs recipientAddresses

            d.blockchainUpdater.policyExists(policyV2Id) shouldBe true
            d.blockchainUpdater.policyOwners(policyV2Id) should contain theSameElementsAs createPolicyTxOwners
            d.blockchainUpdater.policyRecipients(policyV2Id) should contain theSameElementsAs recipientAddresses

            d.removeAfter(genesisSignature)

            d.blockchainUpdater.policyExists(policyV1Id) shouldBe false
            d.blockchainUpdater.policyOwners(policyV1Id) shouldBe empty
            d.blockchainUpdater.policyRecipients(policyV1Id) shouldBe empty

            d.blockchainUpdater.policyExists(policyV2Id) shouldBe false
            d.blockchainUpdater.policyOwners(policyV2Id) shouldBe empty
            d.blockchainUpdater.policyRecipients(policyV2Id) shouldBe empty
          }
      }

      val transactionsGen = for {
        txsCount <- Gen.choose(5, 30)
        tx       <- Gen.listOfN(txsCount, createPolicyTransactionV1GenWithRecipients())
      } yield tx
      "multiple tx" in forAll(accountGen, positiveLongGen, transactionsGen) {
        case (sender, initialBalance, transactionsWithRecipients) =>
          val txSenders = transactionsWithRecipients.map(_.txWrap.tx.sender.toAddress).toSet
          withDomain(createSettings()) { d =>
            d.appendBlock(
              genesisBlock(nextTs,
                           txSenders + sender.toAddress,
                           initialBalance,
                           Map(sender.toAddress -> Seq(Role.Permissioner, Role.ConnectionManager))))

            val genesisSignature = d.lastBlockId

            val allRecipients = transactionsWithRecipients.flatMap(_.recipientsPriKey)
            val regRecipientsAsParticipants = allRecipients.map { recipient =>
              RegisterNodeTransactionV1.selfSigned(sender, recipient, nodeName, OpType.Add, nextTs, registerNodeFee).explicitGet()
            }
            val blockWithRegNode = TestBlock.create(nextTs, genesisSignature, regRecipientsAsParticipants, version = 3)
            d.appendBlock(blockWithRegNode)

            val createPolicyTransactionsBlock =
              TestBlock.create(nextTs, blockWithRegNode.uniqueId, transactionsWithRecipients.map(_.txWrap.tx), version = 3)
            d.appendBlock(createPolicyTransactionsBlock)
            d.appendBlock(TestBlock.create(nextTs, createPolicyTransactionsBlock.uniqueId, Seq.empty, version = 3))

            transactionsWithRecipients.foreach { transactionWithRecipients =>
              val policyId = transactionWithRecipients.txWrap.tx.id.value
              d.blockchainUpdater.policyExists(policyId) shouldBe true
              d.blockchainUpdater.policyOwners(policyId) should contain theSameElementsAs transactionWithRecipients.txWrap.tx.owners
              d.blockchainUpdater.policyRecipients(policyId) should contain theSameElementsAs transactionWithRecipients.txWrap.tx.recipients
            }

            d.removeAfter(genesisSignature)

            transactionsWithRecipients.foreach { transactionWithRecipients =>
              val policyId = transactionWithRecipients.txWrap.tx.id.value
              d.blockchainUpdater.policyExists(policyId) shouldBe false
              d.blockchainUpdater.policyOwners(policyId) shouldBe empty
              d.blockchainUpdater.policyRecipients(policyId) shouldBe empty
            }
          }
      }
    }

    "update policy transaction" - {
      "single tx: OpType.Add" in forAll(accountGen,
                                        positiveLongGen,
                                        severalAddressGenerator(),
                                        severalPrivKeysGenerator(),
                                        severalAddressGenerator(),
                                        severalPrivKeysGenerator()) {
        case (sender, initialBalance, ownersInCreate, recipientsInCreate, ownersInUpdate, recipientsInUpdate) =>
          withDomain(createSettings()) { d =>
            d.appendBlock(genesisBlock(nextTs, sender.toAddress, initialBalance, roles = Seq(Role.Permissioner, Role.ConnectionManager)))
            val updateOpType     = OpType.Add
            val genesisSignature = d.lastBlockId

            val regRecipientsAsParticipants = (recipientsInCreate ++ recipientsInUpdate).map { recipient =>
              RegisterNodeTransactionV1.selfSigned(sender, recipient, nodeName, OpType.Add, nextTs, registerNodeFee).explicitGet()
            }

            val recipientAddressesInCreate = recipientsInCreate.map(_.toAddress)
            val createPolicyTxOwners       = sender.toAddress :: ownersInCreate
            val createPolicyTransaction =
              CreatePolicyTransactionV1
                .selfSigned(sender, "some name", "some description", recipientAddressesInCreate, createPolicyTxOwners, nextTs, createPolicyFee)
                .explicitGet()
            val policyId = createPolicyTransaction.id.value

            val blockWithRegNodeAndPolicyCreation = TestBlock.create(nextTs, genesisSignature, regRecipientsAsParticipants :+ createPolicyTransaction)
            d.appendBlock(blockWithRegNodeAndPolicyCreation)

            val recipientAddressesInUpdate = recipientsInUpdate.map(_.toAddress)

            val blockWithUpdatePolicy = TestBlock.create(
              nextTs,
              blockWithRegNodeAndPolicyCreation.uniqueId,
              Seq(
                UpdatePolicyTransactionV2
                  .selfSigned(sender, policyId, recipientAddressesInUpdate, ownersInUpdate, updateOpType, nextTs, updatePolicyFee, None)
                  .explicitGet()
              )
            )
            d.appendBlock(blockWithUpdatePolicy)
            d.appendBlock(TestBlock.create(nextTs, blockWithUpdatePolicy.uniqueId, Seq.empty))

            d.blockchainUpdater.policyExists(policyId) shouldBe true
            d.blockchainUpdater.policyOwners(policyId) should contain theSameElementsAs createPolicyTxOwners ++ ownersInUpdate
            d.blockchainUpdater.policyRecipients(policyId) should contain theSameElementsAs recipientAddressesInCreate ++ recipientAddressesInUpdate

            d.removeAfter(blockWithRegNodeAndPolicyCreation.uniqueId)

            d.blockchainUpdater.policyExists(policyId) shouldBe true
            d.blockchainUpdater.policyOwners(policyId) should contain theSameElementsAs createPolicyTxOwners
            d.blockchainUpdater.policyRecipients(policyId) should contain theSameElementsAs recipientAddressesInCreate
          }
      }

      "single tx: OpType.Remove" in forAll(accountGen, positiveLongGen, severalAddressGenerator(), severalPrivKeysGenerator()) {
        case (sender, initialBalance, ownersInCreate, recipientsInCreate) =>
          withDomain() { d =>
            d.appendBlock(genesisBlock(nextTs, sender.toAddress, initialBalance, roles = Seq(Role.Permissioner, Role.ConnectionManager)))
            val updateOpType     = OpType.Remove
            val genesisSignature = d.lastBlockId

            val regRecipientsAsParticipants = recipientsInCreate.map { recipient =>
              RegisterNodeTransactionV1.selfSigned(sender, recipient, nodeName, OpType.Add, nextTs, registerNodeFee).explicitGet()
            }

            val recipientAddressesInCreate = recipientsInCreate.map(_.toAddress)
            val createPolicyTxOwners       = sender.toAddress :: ownersInCreate
            val createPolicyTransaction =
              CreatePolicyTransactionV1
                .selfSigned(sender, "some name", "some description", recipientAddressesInCreate, createPolicyTxOwners, nextTs, createPolicyFee)
                .explicitGet()
            val policyId = createPolicyTransaction.id.value

            val blockWithRegNodeAndPolicyCreation = TestBlock.create(nextTs, genesisSignature, regRecipientsAsParticipants :+ createPolicyTransaction)
            d.appendBlock(blockWithRegNodeAndPolicyCreation)

            val (remainRecipientAddresses, removedRecipientAddresses) = recipientAddressesInCreate.splitAt(recipientsInCreate.length / 2)
            val (remainOwners, removedOwners)                         = ownersInCreate.splitAt(ownersInCreate.length / 2)
            val updatePolicyTransaction =
              UpdatePolicyTransactionV1
                .selfSigned(sender, policyId, removedRecipientAddresses, removedOwners, updateOpType, nextTs, updatePolicyFee)
                .explicitGet()
            val blockWithUpdatePolicy = TestBlock.create(nextTs, blockWithRegNodeAndPolicyCreation.uniqueId, Seq(updatePolicyTransaction))
            d.appendBlock(blockWithUpdatePolicy)
            d.appendBlock(TestBlock.create(nextTs, blockWithUpdatePolicy.uniqueId, Seq.empty))

            d.blockchainUpdater.policyExists(policyId) shouldBe true
            d.blockchainUpdater.policyOwners(policyId) should contain theSameElementsAs sender.toAddress :: remainOwners
            d.blockchainUpdater.policyRecipients(policyId) should contain theSameElementsAs remainRecipientAddresses

            d.removeAfter(blockWithRegNodeAndPolicyCreation.uniqueId)

            d.blockchainUpdater.policyExists(policyId) shouldBe true
            d.blockchainUpdater.policyOwners(policyId) should contain theSameElementsAs createPolicyTxOwners
            d.blockchainUpdater.policyRecipients(policyId) should contain theSameElementsAs recipientAddressesInCreate
          }
      }

      "multiple tx: OpType.Add and OpType.Remove" in forAll(accountGen,
                                                            positiveLongGen,
                                                            severalAddressGenerator(),
                                                            severalPrivKeysGenerator(),
                                                            severalAddressGenerator(),
                                                            severalPrivKeysGenerator()) {
        case (sender, initialBalance, ownersInCreate, recipientsInCreate, ownersInUpdate, recipientsInUpdate) =>
          withDomain(createSettings()) { d =>
            d.appendBlock(genesisBlock(nextTs, sender.toAddress, initialBalance, roles = Seq(Role.Permissioner, Role.ConnectionManager)))
            val genesisSignature = d.lastBlockId

            val regRecipientsAsParticipants = (recipientsInCreate ++ recipientsInUpdate).map { recipient =>
              RegisterNodeTransactionV1.selfSigned(sender, recipient, nodeName, OpType.Add, nextTs, registerNodeFee).explicitGet()
            }

            val recipientAddressesInCreate = recipientsInCreate.map(_.toAddress)
            val createPolicyTxOwners       = sender.toAddress :: ownersInCreate
            val createPolicyTransaction =
              CreatePolicyTransactionV1
                .selfSigned(sender, "some name", "some description", recipientAddressesInCreate, createPolicyTxOwners, nextTs, createPolicyFee)
                .explicitGet()
            val policyId = createPolicyTransaction.id.value

            val blockWithRegNodeAndPolicyCreation = TestBlock.create(nextTs, genesisSignature, regRecipientsAsParticipants :+ createPolicyTransaction)
            d.appendBlock(blockWithRegNodeAndPolicyCreation)

            val ownersInRemove             = ownersInUpdate.take(ownersInUpdate.size / 2)
            val recipientAddressesInAdd    = recipientsInUpdate.map(_.toAddress)
            val recipientAddressesInRemove = recipientAddressesInAdd.take(recipientsInUpdate.size / 2)

            val blockWithUpdatePolicy = TestBlock.create(
              nextTs,
              blockWithRegNodeAndPolicyCreation.uniqueId,
              Seq(
                UpdatePolicyTransactionV2
                  .selfSigned(sender, policyId, recipientAddressesInAdd, ownersInUpdate, OpType.Add, nextTs, updatePolicyFee, None)
                  .explicitGet(),
                UpdatePolicyTransactionV2
                  .selfSigned(sender, policyId, recipientAddressesInRemove, ownersInRemove, OpType.Remove, nextTs + 1, updatePolicyFee, None)
                  .explicitGet()
              )
            )
            d.appendBlock(blockWithUpdatePolicy)
            d.appendBlock(TestBlock.create(nextTs, blockWithUpdatePolicy.uniqueId, Seq.empty))

            d.blockchainUpdater.policyExists(policyId) shouldBe true
            d.blockchainUpdater.policyOwners(
              policyId) should contain theSameElementsAs (createPolicyTxOwners.toSet ++ ownersInUpdate -- ownersInRemove)
            d.blockchainUpdater.policyRecipients(
              policyId) should contain theSameElementsAs (recipientAddressesInCreate.toSet ++ recipientAddressesInAdd -- recipientAddressesInRemove)

            d.removeAfter(blockWithRegNodeAndPolicyCreation.uniqueId)

            d.blockchainUpdater.policyExists(policyId) shouldBe true
            d.blockchainUpdater.policyOwners(policyId) should contain theSameElementsAs createPolicyTxOwners
            d.blockchainUpdater.policyRecipients(policyId) should contain theSameElementsAs recipientAddressesInCreate
          }
      }
    }

    "policy data hash transaction" - {
      "multiple tx" in forAll(accountGen, positiveLongGen, severalAddressGenerator(), severalPrivKeysGenerator()) {
        case (sender, initialBalance, ownersInCreate, recipientsInCreate) =>
          withDomain(createSettings()) { d =>
            val addressesWithBalance = Set(sender.toAddress) ++ recipientsInCreate.map(_.toAddress)

            d.appendBlock(
              genesisBlock(
                genesisTs = nextTs,
                addresses = addressesWithBalance,
                initialBalance = initialBalance,
                roles = Map(sender.toAddress -> Seq(Role.Permissioner, Role.ConnectionManager))
              )
            )

            val genesisSignature = d.lastBlockId

            val regRecipientsAsParticipants = recipientsInCreate.map { recipient =>
              RegisterNodeTransactionV1.selfSigned(sender, recipient, nodeName, OpType.Add, nextTs, registerNodeFee).explicitGet()
            }

            val recipientAddressesInCreate = recipientsInCreate.map(_.toAddress)
            val createPolicyTxOwners       = sender.toAddress :: ownersInCreate
            val createPolicyTransaction =
              CreatePolicyTransactionV1
                .selfSigned(sender, "some name", "some description", recipientAddressesInCreate, createPolicyTxOwners, nextTs, createPolicyFee)
                .explicitGet()
            val policyId = createPolicyTransaction.id.value

            val blockWithRegNodeAndPolicyCreation = TestBlock.create(nextTs, genesisSignature, regRecipientsAsParticipants :+ createPolicyTransaction)
            d.appendBlock(blockWithRegNodeAndPolicyCreation)

            val policyDataHash1: PolicyDataHash = PolicyDataHash.fromDataBytes(byteArrayGen(15000).generateSample())
            val policyDataHash2: PolicyDataHash = PolicyDataHash.fromDataBytes(byteArrayGen(15000).generateSample())

            val policyDataHashTransaction1 =
              PolicyDataHashTransactionV1.selfSigned(recipientsInCreate.head, policyDataHash1, policyId, nextTs, policyDataHashFee).explicitGet()
            val policyDataHashTransaction2 =
              PolicyDataHashTransactionV2
                .selfSigned(recipientsInCreate.head, policyDataHash2, policyId, nextTs, policyDataHashFee, None)
                .explicitGet()

            val blockWithPolicyDataHash =
              TestBlock.create(nextTs, blockWithRegNodeAndPolicyCreation.uniqueId, Seq(policyDataHashTransaction1, policyDataHashTransaction2))
            d.appendBlock(blockWithPolicyDataHash)
            d.appendBlock(TestBlock.create(nextTs, blockWithPolicyDataHash.uniqueId, Seq.empty))

            d.blockchainUpdater.policyExists(policyId) shouldBe true
            d.blockchainUpdater.policyDataHashExists(policyId, policyDataHashTransaction1.dataHash) shouldBe true
            d.blockchainUpdater.policyDataHashExists(policyId, policyDataHashTransaction2.dataHash) shouldBe true

            d.removeAfter(blockWithRegNodeAndPolicyCreation.uniqueId)

            d.blockchainUpdater.policyExists(policyId) shouldBe true
            d.blockchainUpdater.policyDataHashExists(policyId, policyDataHashTransaction1.dataHash) shouldBe false
            d.blockchainUpdater.policyDataHashExists(policyId, policyDataHashTransaction2.dataHash) shouldBe false
          }
      }
    }

    "carry fee" in forAll(for {
      sender      <- accountGen
      sponsorship <- sponsorFeeCancelSponsorFeeGen(sender)
      transfer    <- transferGeneratorPV2(sponsorship._1.timestamp, sender, sender.toAddress, 10000000000L)
    } yield {
      (sender, sponsorship, transfer)
    }) {
      case (sender, (issue, sponsor1, sponsor2, _, _), transfer) =>
        withDomain(createSettings()) { d =>
          val ts = issue.timestamp
          def appendBlock(tx: Transaction) = {
            val block = TestBlock.create(ts, d.lastBlockId, Seq(tx))
            d.appendBlock(block)
            block.uniqueId
          }
          def carry(fee: Long) = fee - fee / 5 * 2

          d.appendBlock(genesisBlock(ts, sender.toAddress, Long.MaxValue / 3, roles = Seq(Role.Issuer)))
          d.carryFee shouldBe carry(0)

          val issueBlockId = appendBlock(issue)
          d.carryFee shouldBe carry(issue.fee)

          val sponsorBlockId = appendBlock(sponsor1)
          d.carryFee shouldBe carry(sponsor1.fee)

          val transferBlock = appendBlock(transfer)
          d.carryFee shouldBe carry(transfer.fee)
          d.appendBlock(TestBlock.create(nextTs, transferBlock, Seq.empty))

          d.removeAfter(sponsorBlockId)
          d.carryFee shouldBe carry(sponsor1.fee)

          d.removeAfter(issueBlockId)
          d.carryFee shouldBe carry(issue.fee)

          val transferBlockId = appendBlock(transfer)
          d.carryFee shouldBe carry(transfer.fee)

          appendBlock(sponsor2)
          d.carryFee shouldBe carry(sponsor2.fee)

          d.removeAfter(transferBlockId)
          d.carryFee shouldBe carry(transfer.fee)
        }
    }

    "relean rollbacked transaction" in forAll(accountGen, addressGen, Gen.listOfN(66, Gen.choose(1, 10))) {
      case (sender, recipient, txCount) =>
        withDomain(createSettings()) { d =>
          val ts = nextTs

          d.appendBlock(genesisBlock(ts, sender.toAddress, com.wavesenterprise.state.diffs.ENOUGH_AMT))

          val transferAmount = 100

          val interval = (3 * 60 * 60 * 1000 + 30 * 60 * 1000) / txCount.size

          val transfers =
            txCount.zipWithIndex.map(tc =>
              Range(0, tc._1).flatMap(i => randomOp(sender, recipient, transferAmount, tc._1 % 3, ts + interval * tc._2 + i)))

          val blocks = for ((transfer, i) <- transfers.zipWithIndex) yield {
            val tsb   = ts + interval * i
            val block = TestBlock.create(tsb, d.lastBlockId, transfer)
            d.appendBlock(block)
            (d.lastBlockId, tsb)
          }

          val middleBlock = blocks(txCount.size / 2)

          d.removeAfter(middleBlock._1)

          try {
            d.appendBlock(
              TestBlock.create(
                middleBlock._2 + 10,
                middleBlock._1,
                transfers(0)
              ))
            throw new Exception("Duplicate transaction wasn't checked")
          } catch {
            case e: Throwable => Assertions.assert(e.getMessage().contains("AlreadyInTheState"))
          }
        }
    }

    "atomic transaction" - {
      "transfer and permit" in forAll(accountGen, accountGen, roleGen) {
        case (sender, recipient, role) =>
          withDomain(createSettings()) { d =>
            d.appendBlock(genesisBlock(nextTs, sender.toAddress, com.wavesenterprise.state.diffs.ENOUGH_AMT, Seq(Role.Permissioner)))
            val genesisSignature = d.lastBlockId

            val atomicBadge = Some(AtomicBadge(Some(sender.toAddress)))
            val transferTx = TransferTransactionV3
              .selfSigned(sender, None, None, nextTs, 1.west, transferFee, recipient.toAddress, Array.emptyByteArray, atomicBadge)
              .explicitGet()
            val txTimestamp = nextTs
            val permitTx = PermitTransactionV2
              .selfSigned(sender, recipient.toAddress, txTimestamp, permitTxFee, PermissionOp(OpType.Add, role, txTimestamp, None), atomicBadge)
              .explicitGet()
            val atomicTx      = AtomicTransactionV1.selfSigned(sender, Some(TestBlock.defaultSigner), List(transferTx, permitTx), nextTs).explicitGet()
            val minedAtomicTx = AtomicUtils.addMinerProof(atomicTx, TestBlock.defaultSigner).explicitGet()

            val blockWithAtomicTx = TestBlock.create(nextTs, genesisSignature, Seq(minedAtomicTx))
            d.appendBlock(blockWithAtomicTx)

            val nextBlock = TestBlock.create(nextTs, blockWithAtomicTx.uniqueId, Seq.empty)
            d.appendBlock(nextBlock)

            d.blockchainUpdater.containsTransaction(atomicTx) shouldBe true
            d.blockchainUpdater.containsTransaction(transferTx) shouldBe true
            d.blockchainUpdater.containsTransaction(permitTx) shouldBe true
            d.blockchainUpdater.addressBalance(recipient.toAddress) shouldBe 1.west
            d.blockchainUpdater.permissions(recipient.toAddress).active(nextTs).headOption shouldBe Some(role)

            d.removeAfter(genesisSignature)

            d.blockchainUpdater.containsTransaction(atomicTx) shouldBe false
            d.blockchainUpdater.containsTransaction(transferTx) shouldBe false
            d.blockchainUpdater.containsTransaction(permitTx) shouldBe false
            d.blockchainUpdater.addressBalance(recipient.toAddress) shouldBe 0
            d.blockchainUpdater.permissions(recipient.toAddress).active(nextTs).headOption shouldBe None
          }
      }

      "executed contract create, call and update" in forAll(accountGen, Gen.listOfN(10, dataEntryGen(10)), Gen.listOfN(10, dataEntryGen(10))) {
        case (sender, createResults, callResults) =>
          withDomain(createSettings()) { d =>
            d.appendBlock(genesisBlock(nextTs, sender.toAddress, com.wavesenterprise.state.diffs.ENOUGH_AMT))
            val genesisSignature = d.lastBlockId

            val atomicBadge = Some(AtomicBadge(Some(sender.toAddress)))
            val createContractTx = CreateContractTransactionV3
              .selfSigned(sender, "image", DigestUtils.sha256Hex("imageHash"), "contract", List.empty, createContractFee, nextTs, None, atomicBadge)
              .explicitGet()
            val contractId = createContractTx.id()
            val executedCreateTx =
              ExecutedContractTransactionV1.selfSigned(TestBlock.defaultSigner, createContractTx, createResults, nextTs).explicitGet()
            val callContractTx = CallContractTransactionV4
              .selfSigned(sender, contractId, List.empty, callContractFee, nextTs, 1, None, atomicBadge)
              .explicitGet()
            val executedCallTx =
              ExecutedContractTransactionV1.selfSigned(TestBlock.defaultSigner, callContractTx, callResults, nextTs).explicitGet()
            val updateContractTx = UpdateContractTransactionV3
              .selfSigned(sender, contractId, "new image", DigestUtils.sha256Hex("new imageHash"), updateContractFee, nextTs, None, atomicBadge)
              .explicitGet()
            val executedUpdateTx =
              ExecutedContractTransactionV1.selfSigned(TestBlock.defaultSigner, updateContractTx, List.empty, nextTs).explicitGet()
            val atomicTx = AtomicTransactionV1
              .selfSigned(sender, Some(TestBlock.defaultSigner), List(executedCreateTx, executedCallTx, executedUpdateTx), nextTs)
              .explicitGet()
            val minedAtomicTx = AtomicUtils.addMinerProof(atomicTx, TestBlock.defaultSigner).explicitGet()

            val blockWithAtomicTx = TestBlock.create(nextTs, genesisSignature, Seq(minedAtomicTx))
            d.appendBlock(blockWithAtomicTx)

            val nextBlock = TestBlock.create(nextTs, blockWithAtomicTx.uniqueId, Seq.empty)
            d.appendBlock(nextBlock)

            d.blockchainUpdater.containsTransaction(atomicTx) shouldBe true
            d.blockchainUpdater.containsTransaction(executedCreateTx) shouldBe true
            d.blockchainUpdater.containsTransaction(executedCallTx) shouldBe true
            d.blockchainUpdater.containsTransaction(executedUpdateTx) shouldBe true
            d.blockchainUpdater.contract(ContractId(contractId)).isDefined shouldBe true
            d.blockchainUpdater.contract(ContractId(contractId)).map(_.version) shouldBe Some(2)
            d.blockchainUpdater.contractData(contractId, Default) shouldBe
              ExecutedContractData((createResults ++ callResults).map(d => d.key -> d).toMap)

            d.removeAfter(genesisSignature)

            d.blockchainUpdater.containsTransaction(atomicTx) shouldBe false
            d.blockchainUpdater.containsTransaction(executedCreateTx) shouldBe false
            d.blockchainUpdater.containsTransaction(executedCallTx) shouldBe false
            d.blockchainUpdater.containsTransaction(executedUpdateTx) shouldBe false
            d.blockchainUpdater.contract(ContractId(contractId)).isDefined shouldBe false
            d.blockchainUpdater.contractData(contractId, Default) shouldBe ExecutedContractData(Map.empty)
          }
      }

      "create policy, policy data hash and update policy" in forAll(accountGen,
                                                                    positiveLongGen,
                                                                    severalAddressGenerator(),
                                                                    severalPrivKeysGenerator()) {
        case (sender, initialBalance, ownersInCreate, recipientsInCreate) =>
          withDomain(createSettings()) { d =>
            val addressesWithBalance = Set(sender.toAddress) ++ recipientsInCreate.map(_.toAddress)
            val atomicBadge          = Some(AtomicBadge(Some(sender.toAddress)))

            d.appendBlock(
              genesisBlock(nextTs, addressesWithBalance, initialBalance, Map(sender.toAddress -> Seq(Role.Permissioner, Role.ConnectionManager))))
            val genesisSignature = d.lastBlockId

            val regRecipientsAsParticipants = recipientsInCreate.map { recipient =>
              RegisterNodeTransactionV1.selfSigned(sender, recipient, nodeName, OpType.Add, nextTs, registerNodeFee).explicitGet()
            }

            val recipientAddressesInCreate = recipientsInCreate.map(_.toAddress)
            val createPolicyTxOwners       = sender.toAddress :: ownersInCreate
            val createPolicyTransaction =
              CreatePolicyTransactionV3
                .selfSigned(sender,
                            "some name",
                            "some description",
                            recipientAddressesInCreate,
                            createPolicyTxOwners,
                            nextTs,
                            createPolicyFee,
                            None,
                            atomicBadge)
                .explicitGet()
            val policyId       = createPolicyTransaction.id.value
            val policyDataHash = PolicyDataHash.fromDataBytes(byteArrayGen(10).generateSample())
            val policyDataHashTransaction =
              PolicyDataHashTransactionV3
                .selfSigned(recipientsInCreate.head, policyDataHash, policyId, nextTs, policyDataHashFee, None, atomicBadge)
                .explicitGet()
            val (remainRecipientAddresses, removedRecipientAddresses) = recipientAddressesInCreate.splitAt(recipientsInCreate.length / 2)
            val (remainOwners, removedOwners)                         = ownersInCreate.splitAt(ownersInCreate.length / 2)
            val updatePolicyTransaction =
              UpdatePolicyTransactionV3
                .selfSigned(sender, policyId, removedRecipientAddresses, removedOwners, OpType.Remove, nextTs, updatePolicyFee, None, atomicBadge)
                .explicitGet()
            val atomicTx = AtomicTransactionV1
              .selfSigned(sender,
                          Some(TestBlock.defaultSigner),
                          List(createPolicyTransaction, policyDataHashTransaction, updatePolicyTransaction),
                          nextTs)
              .explicitGet()
            val minedAtomicTx = AtomicUtils.addMinerProof(atomicTx, TestBlock.defaultSigner).explicitGet()

            val blockWithAtomicTx = TestBlock.create(nextTs, genesisSignature, regRecipientsAsParticipants :+ minedAtomicTx)
            d.appendBlock(blockWithAtomicTx)

            val nextBlock = TestBlock.create(nextTs, blockWithAtomicTx.uniqueId, Seq.empty)
            d.appendBlock(nextBlock)

            d.blockchainUpdater.containsTransaction(atomicTx) shouldBe true
            d.blockchainUpdater.containsTransaction(createPolicyTransaction) shouldBe true
            d.blockchainUpdater.containsTransaction(policyDataHashTransaction) shouldBe true
            d.blockchainUpdater.containsTransaction(updatePolicyTransaction) shouldBe true
            d.blockchainUpdater.policyExists(policyId) shouldBe true
            d.blockchainUpdater.policyDataHashExists(policyId, policyDataHashTransaction.dataHash) shouldBe true
            d.blockchainUpdater.policyOwners(policyId) should contain theSameElementsAs sender.toAddress :: remainOwners
            d.blockchainUpdater.policyRecipients(policyId) should contain theSameElementsAs remainRecipientAddresses

            d.removeAfter(genesisSignature)

            d.blockchainUpdater.containsTransaction(atomicTx) shouldBe false
            d.blockchainUpdater.containsTransaction(createPolicyTransaction) shouldBe false
            d.blockchainUpdater.containsTransaction(policyDataHashTransaction) shouldBe false
            d.blockchainUpdater.containsTransaction(updatePolicyTransaction) shouldBe false
            d.blockchainUpdater.policyExists(policyId) shouldBe false
            d.blockchainUpdater.policyDataHashExists(policyId, policyDataHashTransaction.dataHash) shouldBe false
            d.blockchainUpdater.policyOwners(policyId) shouldBe empty
            d.blockchainUpdater.policyRecipients(policyId) shouldBe empty
          }
      }
    }

    "executed contract create with native token operations" in forAll(accountGen,
                                                                      Gen.listOfN(10, dataEntryGen(10)),
                                                                      Gen.listOfN(10, dataEntryGen(10))) {
      case (sender, createResults, callResults) =>
        withDomain(createSettings()) { d =>
          val initialSenderBalance = com.wavesenterprise.state.diffs.ENOUGH_AMT
          d.appendBlock(genesisBlock(nextTs, sender.toAddress, initialSenderBalance))
          val genesisSignature = d.lastBlockId

          val atomicBadge               = Some(AtomicBadge(Some(sender.toAddress)))
          val createContractInputAmount = initialSenderBalance / 2 - createContractFee
          val contractTransferIns       = List(ContractTransferInV1(None, createContractInputAmount))
          val contractApiVersion        = ContractApiVersion.Current
          val createContractTx = CreateContractTransactionV5
            .selfSigned(
              sender,
              "image",
              DigestUtils.sha256Hex("imageHash"),
              "contract",
              List.empty,
              createContractFee,
              nextTs,
              None,
              atomicBadge,
              ValidationPolicy.Any,
              contractApiVersion,
              contractTransferIns
            )
            .explicitGet()
          val createResultsHash = ContractTransactionValidation.resultsHash(createResults)
          val executedCreateTx = ExecutedContractTransactionV3
            .selfSigned(TestBlock.defaultSigner, createContractTx, createResults, createResultsHash, List.empty, nextTs, List.empty)
            .explicitGet()
          val contractId = createContractTx.id()
          val callContractTx = CallContractTransactionV5
            .selfSigned(sender, contractId, List.empty, callContractFee, nextTs, 1, None, atomicBadge, List.empty)
            .explicitGet()
          val callContractWestOutputAmount = createContractInputAmount
          val contractIssue = {
            val nonce   = 1.toByte
            val assetId = ByteStr(crypto.fastHash(callContractTx.id().arr :+ nonce))
            ContractIssueV1(assetId, "test", "test asset", 10000000, 0, true, nonce)
          }
          val contractReissue = {
            ContractReissueV1(contractIssue.assetId, contractIssue.quantity, isReissuable = true)
          }
          val contractBurnAmount = contractIssue.quantity / 2
          val contractBurn       = ContractBurnV1(Some(contractIssue.assetId), contractBurnAmount)

          val contractAssetTransferAmount = contractIssue.quantity
          val westTransferOut             = ContractTransferOutV1(None, sender.toAddress, callContractWestOutputAmount)
          val assetTransferOut            = ContractTransferOutV1(Some(contractIssue.assetId), sender.toAddress, contractAssetTransferAmount)
          val contractAssetTransferOuts   = List(westTransferOut, assetTransferOut)

          val assetOperations = List(contractIssue, contractReissue, contractBurn) ++ contractAssetTransferOuts
          val callResultsHash = ContractTransactionValidation.resultsHash(callResults, assetOperations)
          val executedCallTx =
            ExecutedContractTransactionV3
              .selfSigned(TestBlock.defaultSigner, callContractTx, callResults, callResultsHash, List.empty, nextTs, assetOperations)
              .explicitGet()

          val atomicTx = AtomicTransactionV1
            .selfSigned(sender, Some(TestBlock.defaultSigner), List(executedCreateTx, executedCallTx), nextTs)
            .explicitGet()
          val minedAtomicTx = AtomicUtils.addMinerProof(atomicTx, TestBlock.defaultSigner).explicitGet()

          val blockWithAtomicTx = TestBlock.create(nextTs, genesisSignature, Seq(minedAtomicTx))
          d.appendBlock(blockWithAtomicTx)

          val nextBlock = TestBlock.create(nextTs, blockWithAtomicTx.uniqueId, Seq.empty)
          d.appendBlock(nextBlock)

          val expectedAssetVolume          = contractIssue.quantity + contractReissue.quantity - contractBurn.amount
          val expectedSenderWestBalance    = initialSenderBalance - createContractFee - callContractFee
          val expectedContractAssetBalance = (contractIssue.quantity + contractReissue.quantity) - contractAssetTransferAmount - contractBurn.amount

          d.blockchainUpdater.containsTransaction(atomicTx) shouldBe true
          d.blockchainUpdater.containsTransaction(executedCreateTx) shouldBe true
          d.blockchainUpdater.assetDescription(contractIssue.assetId).isDefined shouldBe true
          val assetDescription = d.blockchainUpdater.assetDescription(contractIssue.assetId).get
          assetDescription.totalVolume shouldBe BigInt(expectedAssetVolume)
          d.blockchainUpdater.contract(ContractId(contractId)).isDefined shouldBe true
          d.blockchainUpdater.contract(ContractId(contractId)).map(_.version) shouldBe Some(1)
          d.blockchainUpdater.addressBalance(sender.toAddress) shouldBe expectedSenderWestBalance
          d.blockchainUpdater.addressBalance(sender.toAddress, Some(contractIssue.assetId)) shouldBe contractAssetTransferAmount
          d.blockchainUpdater.contractBalance(ContractId(contractId),
                                              Some(contractIssue.assetId),
                                              ContractReadingContext.Default) shouldBe expectedContractAssetBalance

          d.removeAfter(genesisSignature)

          d.blockchainUpdater.containsTransaction(atomicTx) shouldBe false
          d.blockchainUpdater.containsTransaction(executedCreateTx) shouldBe false
          d.blockchainUpdater.assetDescription(contractIssue.assetId).isDefined shouldBe false
          d.blockchainUpdater.contract(ContractId(contractId)).isDefined shouldBe false
          d.blockchainUpdater.addressBalance(sender.toAddress) shouldBe initialSenderBalance
          d.blockchainUpdater.contractBalance(ContractId(contractId), None, ContractReadingContext.Default) shouldBe 0
          d.blockchainUpdater.assets().isEmpty shouldBe true
        }
    }
  }
}
