package com.blinkboxbooks.mimir.export

import org.apache.commons.dbcp.BasicDataSource
import org.squeryl.{ SessionFactory, Session }
import org.squeryl.adapters.MySQLAdapter
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.Logging

object DataExportingService extends App with Logging {

  logger.info("Starting")

  val config = ConfigFactory.load("data-exporter-service")

  // Configure reporting database that we'll be writing to.
  val ds = new BasicDataSource
  ds.setDriverClassName(config.getString("exporter.jdbc.driver"))
  ds.setUrl(config.getString("exporter.jdbc.url"))
  ds.setUsername(config.getString("exporter.jdbc.username"))
  ds.setPassword(config.getString("exporter.jdbc.password"))
  ds.setValidationQuery("SELECT 1")
  configureThreadPoolParameters(ds)
  // Fail early if there's a problem with the config.
  ds.getConnection.close

  // The global singleton session factory refers to the output database.
  SessionFactory.concreteFactory =
    Some(() => Session.create(ds.getConnection(), new MySQLAdapter))

  // TODO: Configure database(s) that we'll be querying.

  implicit val dbTimeout = config.getInt("exporter.jdbc.timeout.s")

  // DAOs for input data.
  // TODO

  // DAOs for writing output data.
  // TODO

  // Kick things off.
  // TODO: schedule synch action with configured interval.

  logger.info("Started")

  private def configureThreadPoolParameters(datasource: BasicDataSource) = {
    datasource.setMaxActive(10)
    datasource.setMaxIdle(5)
    datasource.setInitialSize(5)
  }

}
