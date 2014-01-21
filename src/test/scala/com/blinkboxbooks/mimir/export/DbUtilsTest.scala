package com.blinkboxbooks.mimir.export

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.mockito.Mockito._
import org.mockito.Matchers._
import javax.sql.DataSource
import org.scalatest.mock.MockitoSugar
import org.squeryl.Session
import java.sql.Connection
import java.sql.SQLException
import org.scalatest.FlatSpec

@RunWith(classOf[JUnitRunner])
class DbUtilsTest extends FlatSpec with MockitoSugar {

  import DbUtils._

  trait TransactionFixture {
    val datasource = mock[DataSource]
    val connection = mock[Connection]
    doReturn(connection).when(datasource).getConnection
    val fn = mock[(Session) => Unit]
  }

  "Given action" should "run in a managed transaction" in new TransactionFixture {
    DbUtils.withSession(datasource)(fn)
    verify(fn).apply(any[Session])
    verify(connection).setAutoCommit(false)
    verify(connection).commit()
    verify(connection).close()
    verifyNoMoreInteractions(fn, connection)
  }

  "Managed transaction" should "roll back connection when action fails" in new TransactionFixture {
    val ex = new RuntimeException("Test exception")
    doThrow(ex).when(fn).apply(any[Session])

    val thrown = intercept[Exception] { DbUtils.withSession(datasource)(fn) }
    assert(thrown eq ex)

    verify(fn).apply(any[Session])
    verify(connection).setAutoCommit(false)
    verify(connection).rollback()
    verify(connection).close()
    verifyNoMoreInteractions(fn, connection)
  }

  "Managed transaction" should "roll back connection when commit fails" in new TransactionFixture {
    val ex = new SQLException("Test exception")
    doThrow(ex).when(connection).commit()

    val thrown = intercept[Exception] { DbUtils.withSession(datasource)(fn) }
    assert(thrown eq ex)

    verify(fn).apply(any[Session])
    verify(connection).setAutoCommit(false)
    verify(connection).commit()
    verify(connection).rollback()
    verify(connection).close()
    verifyNoMoreInteractions(fn, connection)
  }

}
