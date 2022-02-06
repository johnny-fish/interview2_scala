package utils

/** Case class for more easy representation
 *
 *  @param domain sub-domains of wikipedia
 *  @param idPageTitle id representing the title of the page
 *  @param nbView number of view on this page
 */
case class ElemWithIdPageTitle(domain:String, idPageTitle:Int, nbView:Int)