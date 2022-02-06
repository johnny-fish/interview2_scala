package dataManager

import java.io.{File, FileInputStream, FileWriter}
import scala.collection.mutable
import scala.io.Source
import scala.util.Try


/**
 * Utility class to store all page title in the file avoid saving the memory
 * because it s to long and many
 * Using hashcode of each page title
 * convert string to int to save memory
 * @param storePath location of the directory
 */
class PageTitleCache(storePath:String) {

  var count = 0

  val hashMap = mutable.HashMap.empty[Int,Int]

  var out:Option[FileWriter] = None
  var in:Option[Source] = None

  /**
   * Before write on disk, init the cache
   */
  def initCacheFileToWrite(): Try[Unit] = Try{
    val filename = s"/pagetitilecache"
    val file = new File(s"$storePath/datadogCache$filename")
    file.getParentFile.mkdirs()
    file.delete()
    out = Some(new FileWriter(file))
  }

  /**
   * Write the page name with her unique id number
   * if this page name a already cached, just get her id from memory
   * @param pageTitle page title to cache
   * @return the corresponding unique id
   */
  def writePageTitle(pageTitle:String): Int = {
    if(!hashMap.contains(pageTitle.hashCode)){
      hashMap(pageTitle.hashCode) = count
      count += 1
      out.get.write(s"${hashMap(pageTitle.hashCode)} $pageTitle\n")
    }
    hashMap(pageTitle.hashCode)
  }

  /**
   * delete cache file
   */
  def deteleCacheFile(): Unit = {
    val filename = s"/pagetitilecache"
    val file = new File(s"$storePath/datadogCache$filename")
    file.delete()
  }

  /**
   * close cache file
   */
  def closeAfterWrite(): Try[Unit] = Try{
    if(out.isDefined){
      out.get.close()
      out = None
    }
  }

  /**
   * Before read from cache, init it
   */
  def initCacheFileToRead(): Try[Unit] = Try {
    val filename = s"/pagetitilecache"
    in = Some(Source.fromInputStream(new FileInputStream(s"${storePath}/datadogCache$filename"), "UTF-8"))
  }

  /**
   * Parse each line of cache
   * @param line record
   * @param separator separator
   * @return pair of (id, page title)
   */
  def parseLine(line:String, separator:Char): (Int,String) = {
    val result = Array.ofDim[String](2)
    val sb = new StringBuilder()
    var i = 0
    line.foreach(c => {
      if (c == separator) {
        if (sb.nonEmpty) {
          result(i) = sb.toString()
          i += 1
          sb.clear()
        }
      }
      else {
        sb.addOne(c)
      }
    })
    result(1) = sb.toString()
    (result(0).toInt, result(1))
  }

  /**
   * Get the iterator to process one line at time
   * and so save memory
   */
  def readLine(): Iterator[(Int,String)] = {
    in.get.getLines().map(x => parseLine(x, ' '))
  }

  /**
   * Close after read
   */
  def closeAfterRead():Try[Unit] = Try{
    if(in.isDefined){
      in.get.close()
      in = None
      deteleCacheFile()
    }
  }
}
