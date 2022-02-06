package utils

/** Case class for more easy representation
 *
 *  @param domain sub-domains of wikipedia
 *  @param pageTitle page name
 *  @param nbView number of view on this page
 */
case class Elem(domain:String, pageTitle:String, nbView:Int)