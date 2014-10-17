package com.blinkboxbooks.mimir.export

import com.typesafe.config.Config
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import javax.sql.DataSource
import org.apache.commons.dbcp.BasicDataSource
import org.squeryl.Session
import org.squeryl.adapters.MySQLAdapter
import scala.util.control.NonFatal

object DbUtils {

  /**
   * Create a datasource for the given MySQL DB.
   */
  def createDatasource(config: Config) = {
    val datasource = new BasicDataSource
    datasource.setUrl(config.getString("url"))
    datasource.setUsername(config.getString("username"))
    datasource.setPassword(config.getString("password"))
    datasource.setDriverClassName(config.getString("driver"))
    datasource.setValidationQuery("SELECT 1")
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
      val session: Session = Session.create(connection, new StreamingMySqlAdapter(None))
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
  def withReadOnlySession(datasource: DataSource, fetchSize: Option[Int])(fn: (Session) => Unit) = {
    val connection = datasource.getConnection()
    try {
      connection.setAutoCommit(false)
      val session: Session = Session.create(connection, new StreamingMySqlAdapter(fetchSize))
      fn(session)
    } finally {
      connection.close()
    }
  }

  class StreamingMySqlAdapter(fetchSize: Option[Int]) extends MySQLAdapter {

    override def prepareStatement(conn: Connection, statement: String): PreparedStatement = {
      val stmt = conn.prepareStatement(statement, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      fetchSize.foreach(s => stmt.setFetchSize(s))
      stmt
    }

    override def createStatement(conn: Connection): Statement = {
      val stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      fetchSize.foreach(s => stmt.setFetchSize(s))
      stmt
    }

  }

}