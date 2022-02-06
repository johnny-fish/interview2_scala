package utils

import java.util.Date

/** Represente each request on input
 *
 *  @param date date with hour
 *  @param hour number of last hour to analyze
 */
case class Request(date:Date, hour:Int){

  /** Decompose request to each hour
   *  because pageviews projet have there data partition with hour
   *
   *  @return List of date with hour that we will process
   */
  def decomposeRequest:List[Date]={
    if(hour == 1){
      List[Date](date)
    }
    else {
      List[Date](date) ++ List.range(1, hour).map(i => new Date(date.getTime-3600*1000*i))
    }
  }
}
