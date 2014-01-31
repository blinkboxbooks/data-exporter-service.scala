package com.blinkboxbooks.mimir.export

import org.apache.commons.dbcp.BasicDataSource
import javax.sql.DataSource
import org.squeryl.Session
import org.squeryl.adapters.MySQLAdapter
import com.typesafe.config.Config
import scala.util.control.NonFatal

object DbUtils {

  /**
   * Create a datasource for the given MySQL DB.
   */
  def createDatasource(prefix: String, config: Config) = {
    val datasource = new BasicDataSource
    datasource.setUrl(config.getString(s"$prefix.jdbc.url"))
    datasource.setUsername(config.getString(s"$prefix.jdbc.username"))
    datasource.setPassword(config.getString(s"$prefix.jdbc.password"))
    datasource.setDriverClassName(config.getString(s"$prefix.jdbc.driver"))
    datasource.setValidationQuery("SELECT 1")
    // Check it works OK before handing it out.
    datasource.getConnection.close
    datasource
  }

  /**
   * Run the given action, providing it a session for the database, and treating any
   * changes as a single transaction. On error, any changes will be rolled back.
   */
  def withSession(datasource: DataSource)(fn: (Session) => Unit) = {
    val connection = datasource.getConnection()
    try {
      connection.setAutoCommit(false)
      val session: Session = Session.create(connection, new MySQLAdapter)
      fn(session)
      connection.commit()
    } catch {
      case NonFatal(e) => {
        connection.rollback()
        throw e
      }
    } finally {
      connection.close()
    }
  }

  /**
   * Run the given action, providing it a session for the database.
   */
  def withReadOnlySession(datasource: DataSource)(fn: (Session) => Unit) = {
    val connection = datasource.getConnection()
    try {
      val session: Session = Session.create(connection, new MySQLAdapter)
      fn(session)
    } finally {
      connection.close()
    }
  }

}