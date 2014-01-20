package com.blinkboxbooks.mimir.export

/**
 * Exception class for data exporter service.
 */
class ExportException(message: String = null, cause: Throwable = null)
  extends Exception(message, cause) {
}
