package com.blinkboxbooks.mimir.export

import org.apache.commons.dbcp.BasicDataSource
import javax.sql.DataSource
import org.squeryl.Session
import org.squeryl.adapters.MySQLAdapter

object DbUtils {

  /**
   * Create a datasource for the given MySQL DB.
   */
  def createDatasource(url: String, username: String, password: String) = {
    val datasource = new BasicDataSource
    datasource.setUrl(url)
    datasource.setUsername(username)
    datasource.setPassword(password)
    datasource.setDriverClassName("com.mysql.jdbc.Driver")
    datasource.setValidationQuery("SELECT 1")
    // Check it works OK before handing it out.
    datasource.getConnection.close
    datasource
  }

  /** Helper function that creates a session and performs the given action in a single transaction. */
  def withSession(datasource: DataSource)(fn: (Session) => Unit) = {
    val connection = datasource.getConnection()
    try {
      connection.setAutoCommit(false)
      val session: Session = Session.create(connection, new MySQLAdapter)
      fn(session)
      connection.commit()
    } catch {
      case t: Throwable => {
        connection.rollback()
        throw t
      }
    } finally {
      connection.close()
    }
  }

}