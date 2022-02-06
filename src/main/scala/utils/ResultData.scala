package utils

/** Case class to have better representation of result
 *
 *  @param request the request for this result
 *  @param perDomListElem list of tuple with domain and the list of page
 */
case class ResultData(request:Request, perDomListElem: List[(String,List[Elem])])
