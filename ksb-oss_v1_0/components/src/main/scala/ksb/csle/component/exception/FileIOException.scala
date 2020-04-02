package ksb.csle.component.exception

class FileIOException(
    message: String = "",
    cause: Throwable = None.orNull) extends KsbException(message, cause) {
}