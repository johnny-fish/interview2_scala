package compute

import java.util.Date

import dataManager.{PageTitleCache, RawDataManager}
import dataManager.result.ResultDataManager
import utils.{Elem, ElemWithIdPageTitle, LoggerSupport, Request, ResultData}

import scala.collection.{immutable, mutable}
import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
 * Object regrouping all function to process the result
 */
object ResultDataOps extends LoggerSupport{

  /**
   * Parsing the final map structure who have all count
   * to get a result with top 25 pages for earch sub-domains
   * @param request the request in input
   * @param pageTitleCache page title disk cache to save memory
   * @param resultMap the map structure who have all count for each domain-page
   * @return
   */
  def parseResultMap(request: Request, pageTitleCache: PageTitleCache,
                     resultMap: mutable.Map[String, mutable.Map[Int, Int]]): Try[ResultData] = Try{
    logger.info("Parsing final result to ResultData")

    // min heap
    implicit val compareElem = new Ordering[ElemWithIdPageTitle] {
      override def compare(x:ElemWithIdPageTitle, y:ElemWithIdPageTitle): Int = {
        if(x.nbView > y.nbView || (x.nbView == y.nbView && x.domain > y.domain)) -1 else 1
      }
    }

    // use priority queue to save memory, keep only top 25
    val perDomListElem: Seq[(String, List[ElemWithIdPageTitle])] = resultMap.map({
      case (dom, m) => {
        val priorityQueue = mutable.PriorityQueue.empty[ElemWithIdPageTitle](compareElem)
        m.foreach({
          case (idPageTitle, count) => {
            priorityQueue.enqueue(ElemWithIdPageTitle(dom, idPageTitle, count))
            if (priorityQueue.length > 25) priorityQueue.dequeue() //get top 25 viewed page
          }
        })
        (dom, priorityQueue.toList.sortBy(_.nbView).reverse) //sort by view inside on domain
      }
    }).toList.sortBy(_._1) //sort by domain

    //find all final page title
    val idPageTitlehashSet = mutable.HashSet.empty[Int]
    perDomListElem.foreach(x => x._2.foreach(e=> idPageTitlehashSet.add(e.idPageTitle)))

    //find matching between id and page title
    val idPageTitle2PageTitle = mutable.HashMap.empty[Int,String]
    pageTitleCache.readLine().foreach({case (id, pageTitle) =>
      if(idPageTitlehashSet.contains(id)) idPageTitle2PageTitle.addOne((id,pageTitle)) })

    //replace id by her page titil string
    val finalPerDomListElem = perDomListElem.map({ case (str, value) =>
      str -> value.map(x=> Elem(x.domain, idPageTitle2PageTitle(x.idPageTitle), x.nbView))}).toList

    ResultData(request, finalPerDomListElem)
  }

  /**
   * Using home made parser and not use String.split to save memory
   * because split alloc array of char to process
   * Parsing each record from raw file and filter if present in black list
   * @param line line record
   * @return None if can not parse
   */
  def parseLineAndFilter(line:String,pageTitleCache: PageTitleCache,
                         blacklistHashSet: immutable.HashSet[(String,String)],
                         separator:Char):Option[ElemWithIdPageTitle] = {
    val result = Array.ofDim[String](3)
    val sb = new StringBuilder()
    var i = 0
    line.foreach( c => {
      if(c == separator){
        if(sb.nonEmpty){
          result(i) = sb.toString()
          i += 1
          sb.clear()
        }
      }
      else{
        sb.addOne(c)
      }
    })
    // ignore last element

    // filter with blacklist
    if(i == 3){
      if(blacklistHashSet.contains((result(0),result(1)))){
        None
      }
      else{
        val idPageTitle = pageTitleCache.writePageTitle(result(1))
        Some(ElemWithIdPageTitle(result(0), idPageTitle, result(2).toInt))
      }
    }
    else{
      logger.warn(s"Can not parse the line:${line}")
      None
    }
  }

  /**
   * Compute the result for the request in input
   *
   * @param request the request in input
   * @param rawDataManager manager(read dowaload...) of all raw data such blacklist or hour view data
   * @param resultDataManager manager(read, write...) of result
   * @param rerun if true, re-process with raw data even we already in past done with this request
   * @param reDownloadRawData if true, delete all raw data used for this request en re-download it
   * @return final Result for the request
   */
  def compute(request: Request, rawDataManager: RawDataManager, pageTitleCache:PageTitleCache,
              resultDataManager: ResultDataManager, rerun:Boolean=false,
              reDownloadRawData:Boolean=false): Try[ResultData] = {

      if (resultDataManager.checkIfExist(request) && !rerun) {
        // if request done in past, load it
        resultDataManager.readResult(request)
      }
      else {
        try {
          pageTitleCache.initCacheFileToWrite()
          for {
            resultMap <- request
              .decomposeRequest //decompose the request to unitary date
              .foldLeft[Try[mutable.Map[String, mutable.Map[Int, Int]]]](
                Success(mutable.Map.empty[String, mutable.Map[Int, Int]]))({ (accPartialResult, date) =>
                // for each date, process it and include it to accumulator
                // if one of the step fail in, stop the process
                for {
                  partialResult <- accPartialResult //if accumulator is a failure, stop the process
                  _ = logger.info(s"**Process data for date:${date}")
                  blacklistHashSet <- rawDataManager.getBlacklistHashSet(reDownloadRawData)
                  rawDataSource <- rawDataManager.getRawDataSource(date, reDownloadRawData)
                  _ <- integrate2partialResult(date, rawDataSource, pageTitleCache, blacklistHashSet, partialResult)
                  _ = rawDataSource.close()
                } yield (partialResult)
              })
            _ <- pageTitleCache.closeAfterWrite()
            _ <- pageTitleCache.initCacheFileToRead()
            // parse the accumulator who have all count
            resultData <- ResultDataOps.parseResultMap(request, pageTitleCache, resultMap)
            _ = pageTitleCache.closeAfterRead()
            // save the result for the request
            _ <- resultDataManager.writeResult(request, resultData, rerun)
          } yield (resultData)
        } finally {
          pageTitleCache.closeAfterWrite()
          pageTitleCache.closeAfterRead()
        }
      }
  }

  /**
   * Integrate each record of unitary hour to the accumulator
   *
   * @param date unitary hour to process
   * @param rawDataSource list of raw data (each record) waiting to integrate to partialResult
   * @param blacklistHashSet set of blacklisted page (domain, pageTitle)
   * @param partialResult the accumulator, save all count of domain page
   */
  def integrate2partialResult(date: Date, rawDataSource:Source, pageTitleCache: PageTitleCache, blacklistHashSet: immutable.HashSet[(String,String)],
                 partialResult: mutable.Map[String, mutable.Map[Int, Int]]): Try[Unit] = Try {
    for(line <- rawDataSource.getLines()) {

      parseLineAndFilter(line, pageTitleCache,blacklistHashSet, ' ') match {
        case Some(elem) => {
         if (partialResult.contains(elem.domain)) {
            if (partialResult(elem.domain).contains(elem.idPageTitle)) {
              partialResult(elem.domain)(elem.idPageTitle) += elem.nbView
            }
            else {
              partialResult(elem.domain)(elem.idPageTitle) = elem.nbView
            }
          }
          else {
            partialResult(elem.domain) = mutable.Map[Int, Int]((elem.idPageTitle, elem.nbView))
          }
        }
        case _ =>
      }
    }
  }

}
