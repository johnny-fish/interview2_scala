package utils

import org.slf4j._

/**
 *  logger
 */
trait LoggerSupport {

  lazy val logger = LoggerFactory.getLogger(getClass)

}
