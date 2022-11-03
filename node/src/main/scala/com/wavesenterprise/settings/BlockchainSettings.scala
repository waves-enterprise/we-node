package com.wavesenterprise.settings

import cats.Show
import cats.implicits._
import com.github.ghik.silencer.silent
import com.google.common.base.CaseFormat
import com.wavesenterprise.account.{Address, PublicKeyAccount}
import com.wavesenterprise.acl.Role
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.features.FeatureProvider.FeatureProviderExt
import com.wavesenterprise.settings.FeeSettings.FeesEnabled
import com.wavesenterprise.state.{Blockchain, ByteStr}
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.validation.FeeCalculator
import com.wavesenterprise.transaction.{AtomicTransaction, TransactionParser, TransactionParsers, ValidationError}
import com.wavesenterprise.utils.{Base64, CertUtils}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.utils.pki.CrlData
import enumeratum.EnumEntry.Hyphencase
import enumeratum.values.{ByteEnum, ByteEnumEntry, StringEnum, StringEnumEntry}
import enumeratum.{Enum, EnumEntry}
import play.api.libs.json._
import pureconfig.ConvertHelpers._
import pureconfig.Derivation.Successful
import pureconfig.configurable._
import pureconfig.error.{CannotConvert, CannotParse, ConfigReaderFailures}
import pureconfig.generic.semiauto._
import pureconfig.{ConfigCursor, ConfigObjectCursor, ConfigReader, ConfigSource, ConvertHelpers}

import java.net.URL
import java.security.cert.{X509CRL, X509Certificate}
import scala.collection.immutable
import scala.concurrent.duration._
import scala.util.{Failure, Success}

case class FunctionalitySettings(featureCheckBlocksPeriod: Int, blocksForFeatureActivation: Int, preActivatedFeatures: Map[Short, Int] = Map.empty) {

  require(featureCheckBlocksPeriod > 1, "featureCheckBlocksPeriod must be greater than 1")
  require(
    (blocksForFeatureActivation > 0) && (blocksForFeatureActivation <= featureCheckBlocksPeriod),
    s"blocksForFeatureActivation must be in range 1 to $featureCheckBlocksPeriod"
  )

  def activationWindow(height: Int): Range =
    if (height < 1) Range(0, 0)
    else {
      val ws = featureCheckBlocksPeriod
      Range.inclusive((height - 1) / ws * ws + 1, ((height - 1) / ws + 1) * ws)
    }
}

object FunctionalitySettings extends WEConfigReaders {

  val TESTNET = apply(
    featureCheckBlocksPeriod = 3000,
    blocksForFeatureActivation = 2700,
    preActivatedFeatures = Map.empty,
  )

  implicit val shortToIntMapReader: ConfigReader[Map[Short, Int]] =
    emptyMapReader(genericMapReader[Short, Int](catchReadError(_.toShort)))

  implicit val configReader: ConfigReader[FunctionalitySettings] = deriveReader

  implicit val toPrintable: Show[FunctionalitySettings] = { x =>
    import x._

    val preActivatedFeaturesStr = preActivatedFeatures.map { case (k, v) => s"$k -> $v" }.mkString(", ")
    s"""
       |featureCheckBlocksPeriod: $featureCheckBlocksPeriod
       |blocksForFeatureActivation: $blocksForFeatureActivation
       |preActivatedFeatures: [$preActivatedFeaturesStr]
     """.stripMargin
  }
}

case class GenesisTransactionSettings(recipient: Address, amount: WestAmount)

object GenesisTransactionSettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[GenesisTransactionSettings] = deriveReader

  implicit val toPrintable: Show[GenesisTransactionSettings] = { x =>
    import x._
    s"""
       |recipient: ${recipient.stringRepr}
       |amount: $amount
     """.stripMargin
  }
}

case class NetworkParticipantDescription(publicKey: String, roles: Seq[String] = Seq.empty)

object NetworkParticipantDescription extends WEConfigReaders {

  implicit val configReader: ConfigReader[NetworkParticipantDescription] = deriveReader

  implicit val toPrintable: Show[NetworkParticipantDescription] = { x =>
    import x._

    s"""
       |publicKey: $publicKey, address: ${PublicKeyAccount
         .fromBase58String(publicKey)
         .map(_.toAddress.stringRepr)
         .fold(l => s"InvalidAddress, cause ${l.message}", r => r)}
       |roles: [${roles.mkString(", ")}]
     """.stripMargin
  }
}

sealed abstract class GenesisSettingsVersion(val value: Byte) extends ByteEnumEntry

object GenesisSettingsVersion extends ByteEnum[GenesisSettingsVersion] {
  case object LegacyVersion extends GenesisSettingsVersion(1)
  case object ModernVersion extends GenesisSettingsVersion(2)

  override def values: immutable.IndexedSeq[GenesisSettingsVersion] = findValues
}

sealed trait GenesisType extends EnumEntry with Hyphencase

object GenesisType extends Enum[GenesisType] {
  override val values: immutable.IndexedSeq[GenesisType] = findValues

  implicit val format: Format[GenesisType] = Format(
    Reads(jsv => JsSuccess(fromStr(jsv.as[String]))),
    Writes(genesisType => JsString(genesisType.entryName))
  )

  def fromStr(str: String): GenesisType = withNameInsensitiveOption(str).getOrElse(Unknown)

  case object Plain         extends GenesisType
  case object SnapshotBased extends GenesisType
  case object Unknown       extends GenesisType
}

sealed trait GenesisSettings {
  def `type`: GenesisType
  def blockTimestamp: Long
  def genesisPublicKeyBase58: String
  def signature: Option[ByteStr]
  def senderRoleEnabled: Boolean

  def pki: Option[PkiGenesisSettings]

  require(
    blockTimestamp >= GenesisSettings.minTimestamp,
    s"Genesis block timestamp '$blockTimestamp' should be no less than '${GenesisSettings.minTimestamp}'"
  )

  def toPlainSettings: Either[ValidationError, PlainGenesisSettings] = this match {
    case plain: PlainGenesisSettings => Right(plain)
    case other                       => Left(GenericError(s"Unexpected genesis settings: '$other'"))
  }

  def toPlainSettingsUnsafe: PlainGenesisSettings = this match {
    case plain: PlainGenesisSettings => plain
    case other                       => throw new IllegalStateException(s"Unexpected genesis settings: '$other'")
  }
}

case class SnapshotBasedGenesisSettings(blockTimestamp: Long,
                                        genesisPublicKeyBase58: String,
                                        signature: Option[ByteStr],
                                        senderRoleEnabled: Boolean = false,
                                        pki: Option[PkiGenesisSettings] = None)
    extends GenesisSettings {
  override val `type`: GenesisType = GenesisType.SnapshotBased
}

object SnapshotBasedGenesisSettings extends WEConfigReaders {
  import pureconfig.module.enumeratum._
  import pureconfig.generic.auto._
  implicit val configReader: ConfigReader[SnapshotBasedGenesisSettings] = deriveReader
}

case class PlainGenesisSettings(blockTimestamp: Long,
                                initialBalance: WestAmount,
                                genesisPublicKeyBase58: String,
                                signature: Option[ByteStr],
                                transactions: Seq[GenesisTransactionSettings],
                                networkParticipants: Seq[NetworkParticipantDescription],
                                initialBaseTarget: Long,
                                averageBlockDelay: FiniteDuration,
                                version: GenesisSettingsVersion = GenesisSettingsVersion.LegacyVersion,
                                senderRoleEnabled: Boolean = false,
                                pki: Option[PkiGenesisSettings] = None)
    extends GenesisSettings {

  lazy val permissionerAddresses: List[Address] = {
    filterRoleAddresses(Role.Permissioner)
  }

  lazy val connectionManagerAddresses: List[Address] = {
    filterRoleAddresses(Role.ConnectionManager)
  }

  require(!senderRoleEnabled || version == GenesisSettingsVersion.ModernVersion, "Sender role is only supported in genesis version 2")

  private def filterRoleAddresses(targetRole: Role): List[Address] = {
    val pubkeysStr = networkParticipants
      .filter(_.roles.contains(targetRole.prefixS))
      .map(_.publicKey)

    for {
      _ <- Either
        .cond(pubkeysStr.nonEmpty, (), GenericError(s"Couldn't find ${targetRole.prefixS} public key in genesis configuration!"))

      publicAccounts <- pubkeysStr.toList
        .traverse(pubkeyStr => PublicKeyAccount.fromBase58String(pubkeyStr))
    } yield publicAccounts.map(_.toAddress)
  }.explicitGet()

  override val `type`: GenesisType = GenesisType.Plain
}

object PlainGenesisSettings extends WEConfigReaders {
  import pureconfig.module.enumeratum._
  import pureconfig.generic.auto._
  implicit val configReader: ConfigReader[PlainGenesisSettings] = deriveReader[PlainGenesisSettings]

  val configPath = s"${WESettings.configPath}.blockchain.custom.genesis"
}

object GenesisSettings extends WEConfigReaders {

  val mainnetStartDate = 1559260800000L
  val minTimestamp     = mainnetStartDate

  implicit val configReader: ConfigReader[GenesisSettings] = ConfigReader.fromCursor { cursor =>
    for {
      objectCursor <- cursor.asObjectCursor
      typeCursor = objectCursor.atKeyOrUndefined("type")
      genesisType <- if (typeCursor.isUndefined) {
        Right(GenesisType.Plain)
      } else {
        typeCursor.asString.map(GenesisType.fromStr)
      }
      settings <- genesisType match {
        case GenesisType.Plain         => PlainGenesisSettings.configReader.from(cursor)
        case GenesisType.SnapshotBased => SnapshotBasedGenesisSettings.configReader.from(cursor)
        case GenesisType.Unknown       => ConfigReader.Result.fail(CannotParse("Unsupported Genesis type", typeCursor.location))
      }
    } yield settings
  }

  implicit val toPrintable: Show[GenesisSettings] = gs => {
    s"""type: ${gs.`type`}""" +
      (gs match {
        case x: PlainGenesisSettings =>
          import x._
          val transactionsStr        = transactions.map(t => s"${show"$t"}").mkString("")
          val networkParticipantsStr = networkParticipants.map(np => s"${show"$np"}").mkString("")
          s"""
             |version: ${version.value}
             |senderRoleEnabled: $senderRoleEnabled
             |blockTimestamp: $blockTimestamp
             |initialBalance: $initialBalance
             |genesisPublicKeyBase58: $genesisPublicKeyBase58
             |signature: ${signature.map(_.base58)}
             |initialBaseTarget: $initialBaseTarget
             |averageBlockDelay: $averageBlockDelay
             |transactions:
             |  ${transactionsStr.replace("\n", "\n--")}
             |networkParticipants:
             |  ${networkParticipantsStr.replace("\n", "\n--")}
             |pki: ${show"$pki".replace("\n", "\n--")}
           """.stripMargin
        case x: SnapshotBasedGenesisSettings =>
          import x._
          s"""
             |senderRoleEnabled: $senderRoleEnabled
             |blockTimestamp: $blockTimestamp
             |genesisPublicKeyBase58: $genesisPublicKeyBase58
             |signature: ${signature.map(_.base58)}
           """.stripMargin
        case _ => ""
      })
  }
}

sealed abstract class ConsensusType(val value: String) extends StringEnumEntry

object ConsensusType extends StringEnum[ConsensusType] {
  case object PoA extends ConsensusType("poa")
  case object PoS extends ConsensusType("pos")
  case object CFT extends ConsensusType("cft")

  override def values: immutable.IndexedSeq[ConsensusType] = findValues

  override def withValue(i: String): ConsensusType            = super.withValue(i.toLowerCase())
  override def withValueOpt(i: String): Option[ConsensusType] = super.withValueOpt(i.toLowerCase())
}

sealed abstract class ConsensusSettings(val consensusType: ConsensusType) extends Product with Serializable

object ConsensusSettings extends WEConfigReaders {
  case object PoSSettings extends ConsensusSettings(ConsensusType.PoS)

  case class CftSettings(roundDuration: FiniteDuration,
                         syncDuration: FiniteDuration,
                         banDurationBlocks: Int,
                         warningsForBan: Int,
                         maxBansPercentage: Int,
                         maxValidators: PositiveInt,
                         fullVoteSetTimeout: Option[FiniteDuration],
                         finalizationTimeout: FiniteDuration)
      extends ConsensusSettings(ConsensusType.CFT) {
    require(fullVoteSetTimeout.forall(_ <= finalizationTimeout + syncDuration),
            "The full-vote-set-timeout should not exceed the finalization-timeout + sync-duration")
    require(finalizationTimeout < roundDuration, "The finalization-timeout must be less than the round-duration")
  }

  object CftSettings {
    implicit val configReader: ConfigReader[CftSettings] = deriveReader
  }

  case class PoASettings(roundDuration: FiniteDuration,
                         syncDuration: FiniteDuration,
                         banDurationBlocks: Int,
                         warningsForBan: Int,
                         maxBansPercentage: Int)
      extends ConsensusSettings(ConsensusType.PoA) {
    require(maxBansPercentage >= 0 && maxBansPercentage <= 100, "max-bans-percentage should be between 0 and 100")
  }

  object PoASettings {
    implicit val configReader: ConfigReader[PoASettings] = deriveReader
  }

  private def extractByType(tpe: String, objectCursor: ConfigObjectCursor): Either[ConfigReaderFailures, ConsensusSettings] = {
    tpe.toLowerCase match {
      case ConsensusType.PoS.value => Right(ConsensusSettings.PoSSettings)
      case ConsensusType.PoA.value => PoASettings.configReader.from(objectCursor)
      case ConsensusType.CFT.value => CftSettings.configReader.from(objectCursor)
      case unknown =>
        objectCursor.failed(
          CannotConvert(
            objectCursor.value.toString,
            "ConsensusSettings",
            s"type has value '$unknown' instead of '${ConsensusType.PoS}' or '${ConsensusType.PoA}' or '${ConsensusType.CFT}'"
          ))
    }
  }

  implicit val configReader: ConfigReader[ConsensusSettings] = ConfigReader.fromCursor { cursor =>
    for {
      objectCursor <- cursor.asObjectCursor
      typeCursor   <- objectCursor.atKey("type")
      tpe          <- typeCursor.asString
      settings     <- extractByType(tpe, objectCursor)
    } yield settings
  }

  implicit val toPrintable: Show[ConsensusSettings] = {
    case PoSSettings => "Proof of Stake"
    case cft: CftSettings =>
      s"""
         |Crash Fault Tolerance:
         |  roundDuration: ${cft.roundDuration}
         |  syncDuration: ${cft.syncDuration}
         |  banDurationBlocks: ${cft.banDurationBlocks}
         |  warningsForBan: ${cft.warningsForBan}
         |  maxBansPercentage: ${cft.maxBansPercentage}
         |  maxValidators: ${cft.maxValidators.value}
         |  fullVoteSetTimeout: ${cft.fullVoteSetTimeout.fold("-")(_.toString())}
         |  finalizationTimeout: ${cft.finalizationTimeout}
         """.stripMargin
    case poa: PoASettings =>
      s"""
         |Proof of Authority:
         |  roundDuration: ${poa.roundDuration}
         |  syncDuration: ${poa.syncDuration}
         |  banDurationBlocks: ${poa.banDurationBlocks}
         |  warningsForBan: ${poa.warningsForBan}
         |  maxBansPercentage: ${poa.maxBansPercentage}
         """.stripMargin
  }
}

sealed trait Fees {
  def resolveActual(blockchain: Blockchain, currentBlockHeight: Int): FeeSettings
}

object Fees {

  implicit val toPrintable: Show[Fees] = {
    case fs: FeeSettings           => FeeSettings.toPrintable.show(fs)
    case mf: MainnetRelease120Fees => MainnetRelease120Fees.toPrintable.show(mf)
  }

  implicit val configReader: ConfigReader[Fees] = FeeSettings.configReader.map(identity[Fees])

  val mainnetConfigReader: ConfigReader[Fees] = ConfigReader.fromCursor { cursor =>
    for {
      objectCursor         <- cursor.asObjectCursor
      fees                 <- FeesEnabled.configReader.from(objectCursor)
      feesBeforeRelease120 <- ConfigSource.resources(FeeSettings.mainNetFeesBeforeRelease120ConfResource).load[FeesEnabled]
    } yield MainnetRelease120Fees(fees, feesBeforeRelease120)
  }
}

sealed abstract class FeeSettings(val areFeesEnabled: Boolean) extends Product with Serializable with Fees {
  def forTxType(txTypeId: Byte): Long
  def forTxTypeAdditional(txTypeId: Byte): Long
  def resolveActual(blockchain: Blockchain, currentBlockHeight: Int): FeeSettings = this
}

object FeeSettings extends WEConfigReaders {

  val mainNetFeesBeforeRelease120ConfResource = "mainnet-fees-before-release120.conf"

  case object FeesDisabled extends FeeSettings(areFeesEnabled = false) {
    def forTxType(txTypeId: Byte): Long           = 0L
    def forTxTypeAdditional(txTypeId: Byte): Long = 0L
  }

  case class FeesEnabled(base: Map[Byte, WestAmount], additional: Map[Byte, WestAmount] = Map.empty) extends FeeSettings(areFeesEnabled = true) {
    {
      val missingFeeIds = txTypesRequiringFees.values.map(_.typeId).toSet.filterNot(base.contains)
      val missingFee    = txConfNameToTypeByte.collect { case (name, typeId) if missingFeeIds.contains(typeId) => name }
      require(missingFeeIds.isEmpty, s"Unspecified fees for transaction types: [${missingFee.mkString(", ")}]")
      val missingAdditionalFeeIds = txTypesRequiringAdditionalFees.values.map(_.typeId).toSet.filterNot(additional.contains)
      val missingAdditionalFee    = txConfNameToTypeByte.collect { case (name, typeId) if missingAdditionalFeeIds.contains(typeId) => name }
      require(missingFeeIds.isEmpty, s"Unspecified additional fees for transaction types: [${missingAdditionalFee.mkString(", ")}]")
    }

    def forTxType(txTypeId: Byte): Long = base(txTypeId).units

    def forTxTypeAdditional(txTypeId: Byte): Long = additional(txTypeId).units
  }

  object FeesEnabled extends WEConfigReaders {
    implicit val byteToAmountMapReader: ConfigReader[Map[Byte, WestAmount]] = genericMapReader(catchReadError(txConfNameToTypeByte))
    implicit val configReader: ConfigReader[FeesEnabled]                    = deriveReader
  }

  implicit val configReader: ConfigReader[FeeSettings] = ConfigReader.fromCursor { cursor =>
    for {
      objectCursor <- cursor.asObjectCursor
      isEnabled = objectCursor.atKey("enabled").flatMap(_.asBoolean).getOrElse(true)
      settings <- if (isEnabled) FeesEnabled.configReader.from(objectCursor) else Right(FeesDisabled)
    } yield settings
  }

  val txTypesRequiringFees: Map[String, TransactionParser] =
    TransactionParsers.byName.filterNot {
      case (_, builder) => FeeCalculator.zeroFeeTransactionTypes.contains(builder.typeId)
    }

  val txTypesRequiringAdditionalFees: Map[String, TransactionParser] =
    TransactionParsers.byName.filter {
      case (_, builder) => FeeCalculator.additionalFeeTransactionTypes.contains(builder.typeId)
    }

  /**
    * Map of all transaction names to transaction type bytes
    * V1 and V2 postfixes are ignored
    */
  val txConfNameToTypeByte: Map[String, Byte] = {
    val converter = CaseFormat.UPPER_CAMEL.converterTo(CaseFormat.LOWER_HYPHEN)
    txTypesRequiringFees
      .map {
        case (name, p) =>
          converter.convert(
            name
              .replace("V1", "")
              .replace("V2", "")
              .replace("V3", "")
              .replace("Transaction", "")) -> p.typeId
      }
  }

  // TODO 16.11.2020 izhavoronkov: remove when real Atomic Tx fee will be added
  val tempConstantFees: Map[String, Long] = Map(AtomicTransaction.typeId.toString -> 0L)

  def txTypeToMinFee(feesEnabled: FeesEnabled): String = feesEnabled.base.map { case (k, v) => s"$k -> $v" }.mkString(", ")

  implicit val toPrintable: Show[FeeSettings] = {
    case FeesDisabled => "areFeesEnabled: false"
    case fe: FeesEnabled =>
      s"""
         |areFeesEnabled: ${fe.areFeesEnabled}
         |txTypeToMinFee: ${txTypeToMinFee(fe)}
       """.stripMargin
  }
}

case class MainnetRelease120Fees(feeSettings: FeesEnabled, feeSettingsBeforeRelease120: FeesEnabled) extends Fees {

  override def resolveActual(blockchain: Blockchain, currentBlockHeight: Int): FeeSettings = {
    if (blockchain.isFeatureActivated(BlockchainFeature.SponsoredFeesSupport, currentBlockHeight)) {
      feeSettings
    } else {
      feeSettingsBeforeRelease120
    }
  }
}

object MainnetRelease120Fees {

  import FeeSettings.txTypeToMinFee
  implicit val toPrintable: Show[MainnetRelease120Fees] = { x =>
    import x._
    s"""
         |areFeesEnabled: true
         |txTypeToMinFee: ${txTypeToMinFee(feeSettings)}
         |txTypeToMinFeeBeforeRelease120: ${txTypeToMinFee(feeSettingsBeforeRelease120)}
       """.stripMargin
  }
}

case class Custom(functionality: FunctionalitySettings, genesis: GenesisSettings, addressSchemeCharacter: Char)

object Custom extends WEConfigReaders {
  implicit val configReader: ConfigReader[Custom] = deriveReader
}

case class BlockchainSettings(custom: Custom, fees: Fees, consensus: ConsensusSettings) {
  require(
    consensus.consensusType != ConsensusType.PoS || custom.genesis.`type` != GenesisType.SnapshotBased,
    s"Genesis type '${GenesisType.SnapshotBased.entryName}' is not supported for consensus type '${ConsensusType.PoS.value}'"
  )
}

object BlockchainSettings extends WEConfigReaders {
  val mainNetConfResource   = "mainnet.conf"
  val unitTestsConfResource = "test.conf"

  val configPath: String = s"${WESettings.configPath}.blockchain"
  val mainnetConfigPath  = "mainnet.conf"

  val mainnetConfigSource: ConfigSource   = ConfigSource.resources(mainnetConfigPath).at(configPath)
  val unitTestsConfigSource: ConfigSource = ConfigSource.resources(unitTestsConfResource).at(configPath)

  implicit val configReader: ConfigReader[BlockchainSettings] = ConfigReader.fromCursor { cursor =>
    for {
      objectCursor   <- cursor.asObjectCursor
      typeCursor     <- objectCursor.atKey("type")
      blockchainType <- typeCursor.asString
      settings <- BlockchainType.withName(blockchainType) match {
        case BlockchainType.MAINNET =>
          @silent("is never used")
          implicit val feeConfigReader: ConfigReader[Fees]          = Fees.mainnetConfigReader
          val mainnetConfigReader: ConfigReader[BlockchainSettings] = deriveReader[BlockchainSettings]
          mainnetConfigSource.load[BlockchainSettings](Successful(mainnetConfigReader))
        case BlockchainType.DEFAULT => unitTestsConfigSource.load[BlockchainSettings](Successful(deriveReader[BlockchainSettings]))
        case BlockchainType.CUSTOM  => deriveReader[BlockchainSettings].from(objectCursor)
        case unknown                => throw new IllegalArgumentException(s"Unknown blockchain type '$unknown'")
      }
    } yield settings
  }

  implicit val toPrintable: Show[BlockchainSettings] = { x =>
    import x._

    s"""
       |addressSchemeCharacter: ${custom.addressSchemeCharacter}
       |consensusSettings:
       |  ${show"$consensus".replace("\n", "\n--")}
       |functionalitySettings:
       |  ${show"${custom.functionality}".replace("\n", "\n--")}
       |genesisSettings:
       |  ${show"${custom.genesis}".replace("\n", "\n--")}
       |feeSettings:
       |  ${show"$fees".replace("\n", "\n--")}
     """.stripMargin
  }

}

case class PkiGenesisSettings(
    trustedRootFingerprints: List[String],
    certificates: List[X509Certificate],
    strCertificates: List[String],
    crls: List[CrlData] = List.empty,
    strCrls: List[String] = List.empty
)

object PkiGenesisSettings {

  val configPath = s"${WESettings.configPath}.blockchain.custom.genesis.pki"

  def x509CertFromBase64Conf(base64Cert: String): ConfigReader.Result[X509Certificate] = {
    CertUtils.x509CertFromBase64(base64Cert) match {
      case Success(cert) => Right(cert)
      case Failure(err) =>
        Left(
          ConfigReaderFailures(
            CannotParse(
              s"Error occurred when trying to parse base64(der) encoded config certs from " +
                s"'blockchain.custom.genesis.pki.certificates', error message: ${err.getMessage}",
              None
            )))
    }
  }
  def x509CrlFromBase64Conf(base64Crl: String): ConfigReader.Result[X509CRL] =
    com.wavesenterprise.utils.pki
      .x509CrlFromBase64(base64Crl)
      .toEither
      .leftMap(err =>
        ConfigReaderFailures(CannotParse(
          s"Error occurred when trying to parse base64(der) encoded config crls from " +
            s"'blockchain.custom.genesis.pki.crls', error message: ${err.getMessage}",
          None
        )))

  val crlDataReader: ConfigReader[CrlData] = ConfigReader.fromCursor { cursor =>
    for {
      objectCursor <- cursor.asObjectCursor
      pkaCursor    <- objectCursor.atKey("publicKeyBase58")
      pka          <- pkaReader.from(pkaCursor)
      objectCursor <- cursor.asObjectCursor
      s            <- objectCursor.atKey("cdp")
      cdp          <- urlReader.from(s)
      objectCursor <- cursor.asObjectCursor
      crlCursor    <- objectCursor.atKey("crl")
      crlString    <- crlCursor.asString
      crl          <- x509CrlFromBase64Conf(crlString)
    } yield CrlData(crl, pka, cdp)
  }

  val urlReader = ConfigReader.fromString[URL](
    ConvertHelpers.catchReadError(s => new URL(s))
  )

  val pkaReader = ConfigReader.fromString[PublicKeyAccount](
    ConvertHelpers.catchReadError(
      s =>
        PublicKeyAccount
          .fromBase58String(s)
          .leftMap(
            err =>
              CannotParse(
                s"Error occurred when trying to parse base58 encoded publicKey from " +
                  s"'blockchain.custom.genesis.pki.crls', error message: ${err.message}",
                None
            ))
          .explicitGet()))

  implicit val configReader: ConfigReader[PkiGenesisSettings] = ConfigReader.fromCursor { cursor =>
    for {
      objectCursor  <- cursor.asObjectCursor
      trfsKey       <- objectCursor.atKey("trusted-root-fingerprints")
      trfs          <- parseListFromConfigCursor(trfsKey, trf => Right(trf))
      certsKey      <- objectCursor.atKey("certificates")
      parsedCerts   <- parseListFromConfigCursor(certsKey, cert => x509CertFromBase64Conf(cert).map(cert -> _))
      crlsKey       <- objectCursor.atKey("crls")
      parsedCrlData <- parseListFromConfigCursorViaCursor(crlsKey, crlDataReader.from(_))
      (strCrls, crls)   = parsedCrlData.map(crlData => Base64.encode(crlData.crl.getEncoded)) -> parsedCrlData
      (strCerts, certs) = parsedCerts.unzip
    } yield PkiGenesisSettings(trfs, certs, strCerts, crls, strCrls)
  }

  def parseListFromConfigCursor[A](cursor: ConfigCursor, parseFunction: String => ConfigReader.Result[A]): ConfigReader.Result[List[A]] = {
    cursor.asList.flatMap { listCursor =>
      val parsedListCursor = listCursor.map(listEl => listEl.asString.flatMap(parseFunction))
      val result = parsedListCursor
        .foldRight[ConfigReader.Result[List[A]]](Right(List.empty)) {
          case (elementEither, resultEither) =>
            for {
              resultList <- resultEither
              element    <- elementEither
            } yield element :: resultList
        }

      result
    }
  }

  def parseListFromConfigCursorViaCursor[A](cursor: ConfigCursor,
                                            parseFunction: ConfigCursor => ConfigReader.Result[A]): ConfigReader.Result[List[A]] = {
    cursor.asList.flatMap { listCursor =>
      val parsedListCursor = listCursor.map(listEl => parseFunction(listEl))
      val result = parsedListCursor
        .foldRight[ConfigReader.Result[List[A]]](Right(List.empty)) {
          case (elementEither, resultEither) =>
            for {
              resultList <- resultEither
              element    <- elementEither
            } yield element :: resultList
        }

      result
    }
  }

  implicit val toPrintable: Show[PkiGenesisSettings] = { x =>
    import x._

    s"""
       |trustedRootFingerprints: ${trustedRootFingerprints.mkString("[", ", ", "]")}
       |certificates: ${strCertificates.mkString("[", ",\n", "]")}
       |crls: ${strCrls.mkString("[", ",\n", "]")}
       |""".stripMargin
  }
}
