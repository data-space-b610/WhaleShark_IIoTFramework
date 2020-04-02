package ksb.csle.component.exception

class ProcessException(
    message: String = "",
    cause: Throwable = None.orNull) extends KsbException(message, cause) {
}