package dataManager.result

import dataManager.WriteModes
import utils.{LoggerSupport, Request, ResultData}
import scala.util.{Failure, Success, Try}

/**
 * Trait for different result saving way
 */
trait ResultDataManager extends LoggerSupport {
  def readResult(request: Request):Try[ResultData]
  def writeResult(request: Request, result: ResultData, overwrite:Boolean=false):Try[Unit]
  def checkIfExist(request: Request):Boolean
  def deleteFile(request: Request):Unit
}

/**
 * Constructor
 */
object ResultDataManager {
  def apply(storePath:String, mode: String): Try[ResultDataManager] = {
    mode match {
      case WriteModes.S3 => Success(new S3ResultManager(storePath))
      case WriteModes.Local => Success(new LocalResultManager(storePath))
      case _ => Failure(new IllegalArgumentException(s"Unable to find $mode write mode"))
    }
  }
}


