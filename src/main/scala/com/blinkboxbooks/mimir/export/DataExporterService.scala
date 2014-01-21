package com.blinkboxbooks.mimir.export

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.commons.dbcp.BasicDataSource
import org.squeryl.{ SessionFactory, Session, Schema, Query, Table }
import org.squeryl.adapters.MySQLAdapter
import org.squeryl.PrimitiveTypeMode._
import rx.lang.scala.Observable
import scala.concurrent.{ Await, promise }
import scala.concurrent.duration._
import java.sql.Date
import javax.sql.DataSource
import java.util.concurrent.TimeUnit

object DataExportingService extends App with Logging {

  import DbUtils._
  import Schemas._

  logger.info("Starting")

  val config = ConfigFactory.load("data-exporter-service")
  val bufferSize = config.getInt("exporter.jdbc.batchsize")
  implicit val timeout = Duration.create(config.getInt("exporter.jdbc.timeout.s"), TimeUnit.SECONDS)

  // Configure datasources for reading and writing.
  val shopDatasource = createDatasource("jdbc:mysql://localhost/shop", "gospoken", "gospoken")
  val outputDatasource = createDatasource("jdbc:mysql://localhost/reporting", "gospoken", "gospoken")

  // The global singleton session factory (yuck) refers to the shop database.
  // TODO: review this.
  SessionFactory.concreteFactory =
    Some(() => Session.create(shopDatasource.getConnection(), new MySQLAdapter))
  SessionFactory.newSession.bindToCurrentThread

  withSession(outputDatasource)(implicit outputSession => {

    // Clear old snapshots.
    using(outputSession) {
      publishersOutput.deleteWhere(p => p.id isNotNull)
      booksOutput.deleteWhere(b => b.id isNotNull)
    }

    // Write new snapshots.
    copy(from(publisherData)(publisher => select(publisher)),
      (pub: Publisher) => new PublisherInfo(pub.id, pub.name, pub.ebookDiscount,
        pub.implementsAgencyPricingModel, pub.countryCode), publishersOutput)

    copy(from(bookData)(book => select(book)),
      (b: Book) => {
        new BookInfo(b.id, b.publisherId, b.publicationDate, b.title,
          b.description, b.languageCode, b.numberOfSections)
      }, booksOutput)

  })

  /**
   * Copy the results from the given Query to the given output table, converting objects
   * using the given converter.
   *
   * Objects from the input query are streamed and written to the output database using
   * batched writes, for performance.
   */
  def copy[T1, T2](query: Iterable[T1], converter: (T1) => T2,
    output: Table[T2])(implicit outputSession: Session, timeout: Duration) = {
    val p = promise[Unit]
    val observable = Observable.from(query)
      .map(converter)
      .buffer(bufferSize)
      .doOnEach(entities => using(outputSession) { output.insert(entities) })
      .doOnError(e => { p.failure(e); e.printStackTrace(System.err) })
      .doOnCompleted(() => { p.success() })

    logger.info(s"Executing data export to table ${output.name}")
    observable.subscribe
    Await.result(p.future, timeout)
    logger.info(s"Completed data export to table ${output.name}")
  }

  logger.info("Completed all tasks")

}
