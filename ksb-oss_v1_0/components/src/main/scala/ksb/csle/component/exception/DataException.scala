package ksb.csle.component.exception

class DataException(
    message: String = "",
    cause: Throwable = None.orNull) extends KsbException(message, cause) {
}