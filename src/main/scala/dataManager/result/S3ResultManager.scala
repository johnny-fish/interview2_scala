package dataManager.result
import java.io.File
import java.text.SimpleDateFormat

import utils.{Request, ResultData}

import scala.util.Try

class S3ResultManager(storePath:String) extends ResultDataManager{
  override def readResult(request: Request): Try[ResultData] = ???

  override def writeResult(request: Request, result: ResultData, overwrite: Boolean): Try[Unit] = ???

  override def checkIfExist(request: Request): Boolean = ???

  override def deleteFile(request: Request): Unit = ???
}

/*
import utils.{Request, ResultData}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.transfer.{TransferManager, TransferManagerBuilder}
import com.amazonaws.regions.Regions

import scala.util.Try

class S3ResultManager(storePath:String) extends ResultDataManager {
  val fileTimePattern = "yyyyMMdd-HH0000"
  val fileDF = new SimpleDateFormat(fileTimePattern)

  val clientRegion: Regions = Regions.DEFAULT_REGION
  val bucketName = "*****"

  val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard()
    .withRegion(clientRegion)
    .withCredentials(new ProfileCredentialsProvider())
    .build()
  val tm: TransferManager = TransferManagerBuilder.standard().withS3Client(s3Client).build()

  val localResultManager = new LocalResultManager(storePath)

  override def checkIfExist(request: Request): Boolean = {
    val d = fileDF.format(request.date)
    val h = request.hour.toString
    val filename = s"/resultData/result-$d-$h.csv"
    s3Client.doesObjectExist(bucketName,filename)
  }

  override def readResult(request: Request): Try[ResultData] = {
    Try {
      val d = fileDF.format(request.date)
      val h = request.hour.toString
      val filename = s"/resultData/result-$d-$h.csv"
      val file = new File(s"$storePath/datadogCache$filename")
      tm.download(bucketName, filename, file).waitForCompletion()
    }
    localResultManager.readResult(request)
  }

  override def writeResult(request: Request, result: ResultData,overwrite:Boolean=false): Try[Unit] = Try{
    val d = fileDF.format(request.date)
    val h = request.hour.toString
    val filename = s"/resultData/result-$d-$h.csv"
    val file = new File(s"$storePath/datadogCache$filename")
    localResultManager.writeResult(request,result)
    tm.upload(bucketName, filename, file)
  }

  override def deleteFile(request: Request): Unit = {
    val d = fileDF.format(request.date)
    val h = request.hour.toString
    val filename = s"/resultData/result-$d-$h.csv"
    s3Client.deleteObject(bucketName,filename)
  }
}
*/