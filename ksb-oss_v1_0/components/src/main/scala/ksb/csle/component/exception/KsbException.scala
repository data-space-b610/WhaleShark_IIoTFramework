package ksb.csle.component.exception

case class KsbException(
    message: String = "",
    cause: Throwable = None.orNull) extends Exception(message, cause) {
}