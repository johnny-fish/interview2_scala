
import java.text.SimpleDateFormat
import java.util.Date

import compute.ResultDataOps
import dataManager.result.ResultDataManager
import utils.{LoggerSupport, Request, ResultData}
import dataManager.{PageTitleCache, RawDataManager, WriteModes}

import scala.util.{Failure, Success, Try}
import org.rogach.scallop._

object MainApp extends App with LoggerSupport {

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val reDownload = opt[Boolean](name = "redownload", default = Some(false))
    val rerun = opt[Boolean](name = "rerun", default = Some(false))
    val writeMode = opt[String](name = "writemode", default = Some(WriteModes.Local),
      validate = m => WriteModes.modes.contains(m),
      descr = "Local or S3, use Local for the moment"
    )
    val requests = opt[List[String]](name = "requests", descr = "yyyy/MM/dd/HH:hour yyyy/MM/dd/HH:hour ...")
    val storePath = opt[String](name = "storepath",
      descr = "folder that store all output file, not content '/' at the end", default = Some("/tmp"))

    validateOpt(writeMode) {
      case Some(v) => {
        if (WriteModes.modes.contains(writeMode())) Right(())
        else Left(s"writemode should be one of ${WriteModes.modes.mkString(",")}")
      }
      case _ => Right(())
    }
    verify()
  }

  val conf = new Conf(args.toSeq)
  println(conf.summary)

  val fileDF = new SimpleDateFormat("yyyy/MM/dd/HH")

  def parseRequestDate(string: String): Try[Request] = Try {
    val s = string.split(":")
    val date = fileDF.parse(s(0))
    val hour = s(1).toInt
    println("Request", s(0), s(1))
    Request(date, hour)
  }

  // default date and hour for the input request
  val hour = 24
  val nowDate = new Date()
  val writeMode = conf.writeMode()
  val rerun = conf.rerun()
  val reDownload = conf.reDownload()

  //if none of request or requests is define, get default
  //if one of two is define, parse it, raise exception if parse fail
  val requests: List[Request] = if (conf.requests.isEmpty) {
    List[Request](Request(nowDate, hour))
  }
  else {
    conf.requests().map(x => parseRequestDate(x)).map {
      case Success(r) => r
      case Failure(_) => throw new IllegalArgumentException("not valide requests")
    }
  }


  // for each of request, call action to process
  // stop if one is fail and raise the exception
  requests.foldLeft[Try[Unit]](Success(())) {
    (result, request) =>
      if (result.isSuccess) {
        action(request)
      }
      else {
        result
      }
  }.failed.foreach({ exception =>
    throw exception
  })

  /**
   * Process the request
   *
   * @param request input request
   * @return Try
   */
  def action(request: Request): Try[Unit] = {
    logger.info(s"Compute for the request ${request.date} and hour ${request.hour}")
    val resultData: Try[ResultData] =
      for {
        resultDataManager <- ResultDataManager(conf.storePath(), writeMode)
        rawDataManager = new RawDataManager(conf.storePath())
        pageTitleCache = new PageTitleCache(conf.storePath())
        resultData <- ResultDataOps.compute(request, rawDataManager, pageTitleCache, resultDataManager, rerun, reDownload)
      } yield resultData
    logger.info(s"End for for the request ${request.date} and hour ${request.hour}")
    logger.info("-------------------------------------------------------------------------")
    resultData match {
      case Success(data) => {
        logger.info(s"There no print because the result is too long: " +
          s"the request ${request.date} and hour ${request.hour}, go to check in the ${conf.storePath()}")
        Success(())
      }
      case Failure(e) => Failure(e)
    }
  }

}

