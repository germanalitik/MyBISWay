package com.wavesplatform.history

import java.io.{BufferedOutputStream, FileOutputStream, OutputStream}
import java.nio.charset.StandardCharsets
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.google.common.primitives.{Ints, Shorts}
import com.wavesplatform.Exporter.{createOutputStream, exportBlockToBinary, exportBlockToJson, log, writeFooter, writeHeader, writeString}
import com.wavesplatform.db._
import com.wavesplatform.features.{FeatureProvider, FeaturesProperties}
import com.wavesplatform.settings.{FeaturesSettings, FunctionalitySettings}
import com.wavesplatform.state2._
import com.wavesplatform.utils._
import kamon.Kamon
import org.iq80.leveldb.{DB, WriteBatch}
import scorex.block.{Block, BlockHeader}
import scorex.transaction.History.BlockchainScore
import scorex.transaction.ValidationError.GenericError
import scorex.transaction._
import scorex.utils.Synchronized.WriteLock
import scorex.utils.{NTP, ScorexLogging, Time}
import java.sql.{Connection, DriverManager, ResultSet}
import play.api.libs.json._

import scala.util.{Failure, Success, Try}

class HistoryWriterImpl private(db: DB, val synchronizationToken: ReentrantReadWriteLock,
                                functionalitySettings: FunctionalitySettings, featuresSettings: FeaturesSettings, time: Time)
  extends SubStorage(db, "history") with PropertiesStorage with VersionedStorage with History with FeatureProvider with ScorexLogging {

  override protected val Version = 1

  import HistoryWriterImpl._

  private val BlockAtHeightPrefix = "blocks".getBytes(Charset)
  private val SignatureAtHeightPrefix = "signatures".getBytes(Charset)
  private val HeightBySignaturePrefix = "heights".getBytes(Charset)
  private val ScoreAtHeightPrefix = "scores".getBytes(Charset)
  private val VotesAtHeightPrefix = "votes".getBytes(Charset)
  private val FeatureStatePrefix = "features".getBytes(Charset)
  private val FeaturesIndexKey = makeKey("feature-index".getBytes(Charset), 0)

  private val HeightProperty = "history-height"

  private val featuresProperties = FeaturesProperties(functionalitySettings)

  override def activationWindowSize(h: Int): Int = featuresProperties.featureCheckBlocksPeriodAtHeight(h)

  def minVotesWithinWindowToActivateFeature(h: Int): Int = featuresProperties.blocksForFeatureActivationAtHeight(h)

  private lazy val preAcceptedFeatures = functionalitySettings.preActivatedFeatures.mapValues(h => h - activationWindowSize(h))

  @volatile private var heightInfo = (height(), time.getTimestamp())

  override def approvedFeatures(): Map[Short, Int] = read { implicit lock =>
    preAcceptedFeatures ++ getFeaturesState
  }

  override def featureVotesCountWithinActivationWindow(height: Int): Map[Short, Int] = read { implicit lock =>
    val votingWindowOpening = FeatureProvider.votingWindowOpeningFromHeight(height, activationWindowSize(height))
    get(makeKey(VotesAtHeightPrefix, votingWindowOpening)).map(VotesMapCodec.decode).map(_.explicitGet().value).getOrElse(Map.empty)
  }

  private def alterVotes(height: Int, votes: Set[Short], voteMod: Int, batch: Option[WriteBatch]): Unit = write("alterVotes") { implicit lock =>
    val votingWindowOpening = FeatureProvider.votingWindowOpeningFromHeight(height, activationWindowSize(height))
    val votesWithinWindow = featureVotesCountWithinActivationWindow(height)
    val newVotes = votes.foldLeft(votesWithinWindow)((v, feature) => v + (feature -> (v.getOrElse(feature, 0) + voteMod)))
    put(makeKey(VotesAtHeightPrefix, votingWindowOpening), VotesMapCodec.encode(newVotes), batch)
  }

  def appendBlock(block: Block, acceptedFeatures: Set[Short])(consensusValidation: => Either[ValidationError, BlockDiff]): Either[ValidationError, BlockDiff] =
    write("appendBlock") { implicit lock =>
      val b = createBatch()

      assert(block.signaturesValid().isRight)

      if ((height() == 0) || (this.lastBlock.get.uniqueId == block.reference)) consensusValidation.map { blockDiff =>
        val h = height() + 1
        val score = (if (height() == 0) BigInt(0) else this.score()) + block.blockScore()
        put(makeKey(BlockAtHeightPrefix, h), block.bytes(), b)
        put(makeKey(ScoreAtHeightPrefix, h), score.toByteArray, b)
        put(makeKey(SignatureAtHeightPrefix, h), block.uniqueId.arr, b)
        put(makeKey(HeightBySignaturePrefix, block.uniqueId.arr), Ints.toByteArray(h), b)
        setHeight(h, b)

        val presentFeatures = allFeatures().toSet
        val newFeatures = acceptedFeatures.diff(presentFeatures)
        newFeatures.foreach(f => addFeature(f, h, b))
        alterVotes(h, block.featureVotes, 1, b)

        blockHeightStats.record(h)
        blockSizeStats.record(block.bytes().length)
        transactionsInBlockStats.record(block.transactionData.size)

        commit(b)
        syncToLocalFile(block)
        log.trace(s"Full $block(id=${block.uniqueId} persisted")

        blockDiff

      }
      else Left(GenericError(s"Parent ${block.reference} of block ${block.uniqueId} does not match last block ${this.lastBlock.map(_.uniqueId)}"))
    }

  private def syncToLocalFile(block: Block): Unit = {
    val outputFilename = "bcsync.txt"
    val format = "JSON"
    val usePostgreSql = false

    if (usePostgreSql) {
      addToPostgreDB(block)
      val rs = readFromPostgreDB
      while (rs.next) {
        println(rs.getString("tx_id") + " " + rs.getString("text"))
      }
    }

    if (block.transactionCount > 0) {
      log.info(s"$outputFilename НАЧИНАЕМ ЗАПИСЫВАТЬ")
      createOutputStream(outputFilename) match {
        case Success(output) =>
          var exportedBytes = 0L
          val bos = new BufferedOutputStream(output)
          val start = System.currentTimeMillis()

          exportedBytes += writeHeader(bos, format)
          exportedBytes += exportBlockToJson(bos, block)
          exportedBytes += writeFooter(bos, format)

          val duration = System.currentTimeMillis() - start
          bos.close()
          output.close()
        case Failure(ex) => log.error(s"Failed to create file '$outputFilename': $ex")
      }
      log.info(s"$outputFilename ЗАПИСАН ===========")
    }
  }

  private def readFromPostgreDB(): ResultSet = {
    classOf[org.postgresql.Driver]
    val con_str = "jdbc:postgresql://localhost:5432/BisChain?user=postgres&password=germ"
    val conn = DriverManager.getConnection(con_str)
    println("Postgres connector from readFromPostgreDB")
    try {
      val stm = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      return stm.executeQuery("SELECT * from public.transaction")

      /* пример обработки ответа
      while (rs.next) {
        println(rs.getString("tx_id") + " " + rs.getString("text"))
      }*/
    } finally {
      println("Postgres connector from readFromPostgreDB CLOSE")
      conn.close()
    }
  }

  private def addToPostgreDB(block: Block): Unit = {
    classOf[org.postgresql.Driver]
    val con_str = "jdbc:postgresql://localhost:5432/BisChain?user=postgres&password=germ"
    val conn = DriverManager.getConnection(con_str)
    try {
      println("Postgres connector from addToPostgreDB")

      val transactions = (Json.parse(block.json().toString()) \ "transactions").get
      val ids = transactions \\ "id"
      val attachments = transactions \\ "attachment"

      for (i <- 0 until ids.length) {
        val prep = conn.prepareStatement("INSERT INTO public.transaction(tx_id, text) VALUES (?, ?)")
        prep.setString(1, ids(i).toString().trim.replaceAll("^\"|\"$", ""))
        prep.setString(2, attachments(i).toString().trim.replaceAll("^\"|\"$", ""))
        prep.executeUpdate
        println("SQL INSERT DONE " + prep)
      }
    } finally {
      println("Postgres connector from addToPostgreDB CLOSE")
      conn.close()
    }
  }

  private def createOutputStream(filename: String) =
    Try {
      new FileOutputStream(filename, true)
    }

  private def writeHeader(stream: OutputStream, format: String) =
    if (format == "JSON") writeString(stream, "[\n") else 0

  private def writeFooter(stream: OutputStream, format: String) =
    if (format == "JSON") writeString(stream, "]\n") else 0

  private def writeString(stream: OutputStream, str: String) = {
    val bytes = str.getBytes(StandardCharsets.UTF_8)
    stream.write(bytes)
    bytes.length
  }

  private def exportBlockToJson(stream: OutputStream, block: Block) = {
    val len = if (height != 2) {
      val bytes = ",\n".getBytes(StandardCharsets.UTF_8)
      stream.write(bytes)
      bytes.length
    } else 0
      val bytes = block.json().toString().getBytes(StandardCharsets.UTF_8)
      stream.write(bytes)
      len + bytes.length
  }

  private def allFeatures() = read { implicit lock =>
    get(FeaturesIndexKey).map(ShortSeqCodec.decode).map(_.explicitGet().value).getOrElse(Seq.empty[Short])
  }

  private def addFeature(featureId: Short, height: Int, batch: Option[WriteBatch]): Unit = {
    val features = (allFeatures() :+ featureId).distinct
    put(makeKey(FeatureStatePrefix, Shorts.toByteArray(featureId)), Ints.toByteArray(height), batch)
    put(FeaturesIndexKey, ShortSeqCodec.encode(features), batch)
  }

  private def deleteFeature(featureId: Short, batch: Option[WriteBatch]): Unit = {
    val features = allFeatures().filterNot(f => f == featureId).distinct
    delete(makeKey(FeatureStatePrefix, Shorts.toByteArray(featureId)), batch)
    put(FeaturesIndexKey, ShortSeqCodec.encode(features), batch)
  }

  private def getFeatureHeight(featureId: Short) =
    get(makeKey(FeatureStatePrefix, Shorts.toByteArray(featureId))).flatMap(b => Try(Ints.fromByteArray(b)).toOption)

  private def getFeaturesState() = allFeatures().foldLeft(Map.empty[Short, Int]) { (r, f) =>
    val h = getFeatureHeight(f)
    if (h.isDefined) r.updated(f, h.get) else r
  }

  def discardBlock(): Option[Block] = write("discardBlock") { implicit lock =>
    val h = height()

    val b = createBatch()

    alterVotes(h, blockAt(h).map(b => b.featureVotes).getOrElse(Set.empty), -1, b)

    val key = makeKey(BlockAtHeightPrefix, h)
    val maybeBlockBytes = get(key)
    val tryDiscardedBlock = maybeBlockBytes.map(b => Block.parseBytes(b))
    val maybeDiscardedBlock = tryDiscardedBlock.flatMap(_.toOption)


    delete(key, b)
    delete(makeKey(ScoreAtHeightPrefix, h), b)

    if (h % activationWindowSize(h) == 0) allFeatures().foreach { f =>
      val featureHeight = getFeatureHeight(f)
      if (featureHeight.isDefined && featureHeight.get == h) deleteFeature(f, b)
    }

    val signatureKey = makeKey(SignatureAtHeightPrefix, h)
    get(signatureKey).foreach(a => delete(makeKey(HeightBySignaturePrefix, a), b))
    delete(signatureKey, b)

    setHeight(h - 1, b)

    commit(b)

    maybeDiscardedBlock
  }

  override def lastBlockIds(howMany: Int): Seq[ByteStr] = read { implicit lock =>
    val startHeight = Math.max(1, height - howMany + 1)
    (startHeight to height).flatMap(getBlockSignature).reverse
  }

  override def height(): Int = read { implicit lock =>
    getIntProperty(HeightProperty).getOrElse(0)
  }

  private def setHeight(x: Int, batch: Option[WriteBatch])(implicit lock: WriteLock): Unit = {
    putIntProperty(HeightProperty, x, batch)
    heightInfo = (x, time.getTimestamp())
  }

  override def scoreOf(id: ByteStr): Option[BlockchainScore] = read { implicit lock =>
    val maybeHeight = heightOf(id)
    if (maybeHeight.isDefined) {
      val maybeScoreBytes = get(makeKey(ScoreAtHeightPrefix, maybeHeight.get))
      if (maybeScoreBytes.isDefined) Some(BigInt(maybeScoreBytes.get)) else None
    } else None
  }

  override def heightOf(blockSignature: ByteStr): Option[Int] = read { implicit lock =>
    get(makeKey(HeightBySignaturePrefix, blockSignature.arr)).map(Ints.fromByteArray)
  }

  override def blockBytes(height: Int): Option[Array[Byte]] = read { implicit lock =>
    get(makeKey(BlockAtHeightPrefix, height))
  }

  override def lastBlockTimestamp(): Option[Long] = this.lastBlock.map(_.timestamp)

  override def lastBlockId(): Option[ByteStr] = this.lastBlock.map(_.signerData.signature)

  override def blockAt(height: Int): Option[Block] = blockBytes(height).map(Block.parseBytes(_).get)

  override def blockHeaderAndSizeAt(height: Int): Option[(BlockHeader, Int)] =
    blockBytes(height).map(bytes => (BlockHeader.parseBytes(bytes).get._1, bytes.length))

  override def debugInfo: HeightInfo = heightInfo

  private def getBlockSignature(height: Int) = get(makeKey(SignatureAtHeightPrefix, height)).map(ByteStr.apply)
}

object HistoryWriterImpl extends ScorexLogging {
  def apply(db: DB, synchronizationToken: ReentrantReadWriteLock, functionalitySettings: FunctionalitySettings,
            featuresSettings: FeaturesSettings, time: Time = NTP): Try[HistoryWriterImpl] =
    createWithVerification[HistoryWriterImpl](new HistoryWriterImpl(db, synchronizationToken, functionalitySettings, featuresSettings, time))

  private val blockHeightStats = Kamon.metrics.histogram("block-height")
  private val blockSizeStats = Kamon.metrics.histogram("block-size-bytes")
  private val transactionsInBlockStats = Kamon.metrics.histogram("transactions-in-block")
}
