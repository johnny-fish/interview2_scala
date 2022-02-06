package dataManager

import java.io.{File, FileInputStream, InputStream}
import java.net.URL
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Date
import java.util.zip.GZIPInputStream
import java.nio.file.StandardCopyOption
import utils.{ElemWithIdPageTitle, LoggerSupport}
import scala.collection.{immutable, mutable}
import scala.io.{BufferedSource, Source}
import scala.util.{Success, Try}

/**
 * To manage all raw data operation such download, save, delete
 */
class RawDataManager(storePath:String) extends LoggerSupport{

  val blackListFileSite = "https://s3.amazonaws.com/dd-interview-data/data_engineer/wikipedia/blacklist_domains_and_pages"

  val site = "https://dumps.wikimedia.org/other/pageviews"
  val fileTimePattern = "yyyyMMdd-HH0000"
  val fileDF = new SimpleDateFormat(fileTimePattern)

  val sitePartitionPattern = "yyyy/yyyy-MM"
  val siteDF = new SimpleDateFormat(sitePartitionPattern)

  var blacklistHashSetMemCache:Option[immutable.HashSet[(String,String)]] = None

  def checkIfExist(date: Date):Boolean={
    val d = fileDF.format(date)
    val file = s"/rawData/pageviews-$d.gz"
    Files.exists(Paths.get(s"$storePath/datadogCache$file"))
  }

  def deleteFile(date:Date):Unit={
    val d = fileDF.format(date)
    val file = s"pageviews-$d.gz"
    logger.info(s"Delete raw Data file ${file}")
    new File(s"$storePath/datadogCache/rawData/$file").delete()
  }

  /**
   * Get the source representing the raw data for unitary date(hour)
   * and download it if not present on local
   * @param date unitary date(hour) of request
   * @param reDownload if true, delete existing raw file and re-download it
   * @return source to access raw data
   */
  def getRawDataSource(date: Date, reDownload:Boolean=false):Try[Source] = Try{
    logger.info("Get raw data for date "+date)
    if(!checkIfExist(date) || reDownload){
      downloadRawData(date, reDownload)
    }
    val d = fileDF.format(date)
    val file = s"/rawData/pageviews-$d.gz"
    val source = Source.fromInputStream(new GZIPInputStream(new FileInputStream(s"/tmp/datadogCache$file")), "UTF-8")
    source
  }

  /**
   * Parse blacklist file
   * @param source access to file
   * @return hashSet of blacklist
   */
  def parseBlackListSource(source: Source):immutable.HashSet[(String,String)] = {
    logger.info("Parsing raw black list")
    source.getLines.foldLeft(mutable.HashSet.empty[(String,String)])((acc, line) => {
      val l = line.split(" ")
      if( l.length == 2){
        acc.add((l(0), l(1)))
      }
      else{
        // warning if some record can not be parse
        // but keep process in running
        logger.warn(s"Can not parse line:${line}")
      }
      acc
    }).to(immutable.HashSet)
  }

  /**
   * Get the hashSet representing blacklisted domain, page
   * if is already read once, cache it on variable
   * @param reDownloadRawData if true, delete existing raw file and re-download it
   * @return
   */
  def getBlacklistHashSet(reDownloadRawData:Boolean=false):Try[immutable.HashSet[(String,String)]] = Try{
    logger.info("Get black list")
    val file = s"/blacklist_domaines_and_pages"
    val src = s"/tmp/datadogCache$file"
    if(blacklistHashSetMemCache.isDefined){
      //if already readed once, get it from variable cache
      blacklistHashSetMemCache.get
    }
    else {
      if(reDownloadRawData && Files.exists(Paths.get(src))) {
        new File(s"$storePath/datadogCache$file").delete()
      }
      if(!Files.exists(Paths.get(src))) {
        downloadBlackListFile()
      }
      val source: BufferedSource = Source.fromFile(src, "UTF-8")
      val blacklistHashSet = parseBlackListSource(source)
      source.close()
      blacklistHashSetMemCache = Some(blacklistHashSet)
      blacklistHashSet
    }
  }


  /**
   * Download blacklist file
   */
  def downloadBlackListFile():Unit = {
    if(Files.notExists(Paths.get(s"$storePath/datadogCache/blacklist_domaines_and_pages"))) {
      logger.info(s"Download Black list")
      val file = new File(s"$storePath/datadogCache/blacklist_domaines_and_pages")
      file.getParentFile.mkdirs()
      val in: InputStream = new URL(blackListFileSite).openStream
      Files.copy(in, Paths.get(file.getPath), StandardCopyOption.REPLACE_EXISTING)
      in.close()
    }
  }

  /**
   * Download hour record raw data file
   * @param date unitary date
   * @param reDownload if true, delete existing raw file and re-download it
   */
  def downloadRawData(date: Date, reDownload:Boolean=false): Unit= {
    val partition = siteDF.format(date)
    val d = fileDF.format(date)
    val filename = s"pageviews-$d.gz"

    if (reDownload) {
      deleteFile(date)
    }
    if (!checkIfExist(date)) {
      logger.info(s"Download Raw data:$filename")
      val src = s"$site/$partition/$filename"
      val file = new File(s"$storePath/datadogCache/rawData/$filename")
      file.getParentFile.mkdirs()

      val in: InputStream = new URL(src).openStream
      Files.copy(in, Paths.get(file.getPath), StandardCopyOption.REPLACE_EXISTING)
      in.close()
    }
  }

}

/**
 * Constructor
 */
object RawDataManager{
  def apply(storePath:String): Try[RawDataManager] = Success(new RawDataManager(storePath))
}
