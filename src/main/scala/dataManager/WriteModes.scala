package dataManager

/**
 * For more easy representation of write modes
 */
object WriteModes {
  val Local = "Local"
  val S3 = "S3"

  val modes = Set(Local) //, S3) //exclure S3 for uber-jar
}
