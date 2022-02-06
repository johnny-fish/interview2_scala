package dataManager.result

import java.io.{File, FileWriter}
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat

import utils.{Elem, Request, ResultData}

import scala.collection.mutable
import scala.io.{BufferedSource, Source}
import scala.util.Try

class LocalResultManager(storePath:String) extends ResultDataManager {

  val fileTimePattern = "yyyyMMdd-HH0000"
  val fileDF = new SimpleDateFormat(fileTimePattern)

  /**
   * Check if the result file of input request exist or not
   * @param request input request
   * @return true if exist
   */
  override def checkIfExist(request: Request): Boolean = {
    val d = fileDF.format(request.date)
    val h = request.hour.toString
    val file = s"/resultData/result-$d-$h.csv"
    Files.exists(Paths.get(s"$storePath/datadogCache$file"))
  }

  /**
   * Delete the result file of input request
   * @param request input request
   */
  def deleteFile(request: Request): Unit = {
    val d = fileDF.format(request.date)
    val h = request.hour.toString
    val file = s"/resultData/result-$d-$h.csv"
    new File(s"$storePath/datadogCache$file").delete()
  }

  /**
   * Parsing each line of result file to build the result
   * It will keep the order
   * @param request input request
   * @param source source give access to file
   * @return the result
   */
  def parseResultDataSource(request: Request, source: Source): ResultData = {
    val result = source.getLines.foldLeft[(Option[String],
      mutable.ListBuffer[(String, mutable.ListBuffer[Elem])])]((None,
      mutable.ListBuffer.empty[(String, mutable.ListBuffer[Elem])]))((acc, line) => {
      val l = line.split(" ")
      if (l.length == 4) {
        val e = Elem(l(0), l(1), l(2).toInt)
        if (acc._1 != Some(e.domain)) {
          acc._2.addOne((e.domain, mutable.ListBuffer(e)))
        }
        else {
          acc._2.last._2.addOne(e)
        }
        (Some(e.domain), acc._2)
      }
      else {
        acc
      }
    })._2.map(x => (x._1, x._2.toList)).toList
    ResultData(request, result)
  }

  /**
   * Read the CSV result file
   * @param request input request
   * @return result for the request
   */
  override def readResult(request: Request): Try[ResultData] = Try {
    logger.info("Read existing result")
    val d = fileDF.format(request.date)
    val h = request.hour.toString
    val file = s"/resultData/result-$d-$h.csv"
    val source: BufferedSource = Source.fromFile(s"$storePath/datadogCache$file", "UTF-8")
    val result = parseResultDataSource(request, source)
    source.close()
    result
  }

  /**
   * Write result in CSV file
   * It will keep the order
   * @param request input request
   * @param result result data structure
   * @param overwrite if true, delete the result file if is already exist an write the result
   */
  override def writeResult(request: Request, result: ResultData, overwrite: Boolean = false): Try[Unit] = Try {
    logger.info(s"Write result for ${request.date} and hour ${request.hour}")
    if (checkIfExist(request) && overwrite) {
      deleteFile(request)
    }
    if (!checkIfExist(request)) {
      val d = fileDF.format(request.date)
      val h = request.hour.toString
      val filename = s"/resultData/result-$d-$h.csv"
      val file = new File(s"$storePath/datadogCache$filename")
      file.getParentFile.mkdirs()
      val out = new FileWriter(file)
      result.perDomListElem.foreach({ case (k, v) =>
        val towrite = v.map(e => s"${e.domain} ${e.pageTitle} ${e.nbView}\n").mkString("")
        out.write(towrite)
      })
      out.close()
    }
  }
}
