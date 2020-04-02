package ksb.csle.didentification.exceptions

import java.io._

case class DDIException(
    message: String = "",
    cause: Throwable = None.orNull) extends Exception(message, cause) {
}

case class DDIIllegalArgumentException(
    message: String = "",
    cause: Throwable = None.orNull) extends IllegalArgumentException(message, cause) {
}

case class DDIFileIOException(
    message: String = "",
    cause: Throwable = None.orNull) extends IOException(message, cause) {
}

case class DDIFileNotFoundException(
    message: String = "",
    cause: Throwable = None.orNull) extends FileNotFoundException(message) {
}

case class DDITypeMismatchException(
    message: String = "",
    cause: Throwable = None.orNull) extends IOException(message, cause) {
}
