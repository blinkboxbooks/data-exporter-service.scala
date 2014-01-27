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
import it.sauronsoftware.cron4j.Scheduler

object DataExporterService extends App with Logging {

  import ShopSchema._
  import ClubcardSchema._
  import ReportingSchema._
  import DbUtils._

  val config = ConfigFactory.load("data-exporter-service")
  val batchSize = config.getInt("exporter.jdbc.batchsize")
  implicit val timeout = Duration.create(config.getInt("exporter.jdbc.timeout.s"), TimeUnit.SECONDS)

  // Configure datasources for reading and writing.
  val shopDatasource = createDatasource("shop", config)
  val clubcardDatasource = createDatasource("clubcard", config)
  val outputDatasource = createDatasource("reporting", config)

  if (args.contains("--now")) {
    logger.info("Starting one-off export")
    runDataExport(shopDatasource, clubcardDatasource, outputDatasource, batchSize)
    logger.info("Completed export")
  } else {
    val cronStr = config.getString("exporter.schedule")
    logger.info(s"Scheduling data export with configuration: $cronStr")
    val scheduler = new Scheduler()
    scheduler.schedule(cronStr, new Runnable() {
      override def run() {
        logger.info("Starting scheduled export")
        runDataExport(shopDatasource, clubcardDatasource, outputDatasource, batchSize)
        logger.info("Completed scheduled export")
      }
    })
    scheduler.setDaemon(false)
    scheduler.start()
  }

  /**
   * Perform all the data export jobs.
   */
  def runDataExport(shopDatasource: DataSource, clubcardDatasource: DataSource, outputDatasource: DataSource,
    bufferSize: Int)(implicit timeout: Duration) = {

    // The default session factory refers to the shop database.
    SessionFactory.concreteFactory =
      Some(() => Session.create(shopDatasource.getConnection(), new MySQLAdapter))
    SessionFactory.newSession.bindToCurrentThread

    withSession(outputDatasource)(implicit outputSession => {

      // Clear old snapshots.
      using(outputSession) {
        publishersOutput.deleteWhere(r => 1 === 1)
        booksOutput.deleteWhere(r => 1 === 1)
        userClubcardsOutput.deleteWhere(r => 1 === 1)
        currencyRatesOutput.deleteWhere(r => 1 === 1)
        contributorsOutput.deleteWhere(r => 1 === 1)
        contributorRolesOutput.deleteWhere(r => 1 === 1)
        genresOutput.deleteWhere(r => 1 === 1)
        bookGenresOutput.deleteWhere(r => 1 === 1)
      }

      // Write new snapshots. Copy these sequentially, in the same transaction. 
      copy(from(bookData)(select(_)), booksOutput, identity[Book])
      copy(from(publisherData)(select(_)), publishersOutput, identity[Publisher])
      copy(from(contributorData)(select(_)), contributorsOutput, identity[Contributor])
      copy(from(mapBookContributorData)(select(_)), contributorRolesOutput, identity[MapBookToContributor])
      copy(from(genreData)(select(_)), genresOutput, identity[Genre])
      copy(from(bookGenreData)(select(_)), bookGenresOutput, identity[MapBookToGenre])
      copy(from(currencyRateData)(select(_)), currencyRatesOutput, identity[CurrencyRate])

      withReadOnlySession(clubcardDatasource)(clubcardSession => {
        using(clubcardSession) {
          val clubcardResults =
            from(clubcards, users, clubcardUsers)((clubcard, user, link) =>
              where(clubcard.id === link.cardId and user.id === link.userId)
                select (clubcard, user))
          val converter = (cu: (Clubcard, ClubcardUser)) =>
            new UserClubcardInfo(cu._1.cardNumber, Integer.parseInt(cu._2.userId))
          copy(clubcardResults, userClubcardsOutput, converter)
        }
      })
    })
  }

  /**
   * Copy the results from the given Query to the given output table, converting objects
   * using the given converter.
   *
   * Objects from the input query are streamed and written to the output database using
   * batched writes, for performance. The overall copy job is synchronous, in that it will wait for
   * the copying to complete before returning.
   */
  def copy[T1, T2](input: Iterable[T1], output: Table[T2], converter: T1 => T2)(
    implicit outputSession: Session, timeout: Duration) = {

    logger.info(s"Executing data export to table ${output.name}")

    var count = 0
    val p = promise[Unit]
    val observable = Observable.from(input)
      .map(converter)
    // .buffer(bufferSize)

    observable.subscribe(
      entities => using(outputSession) { output.insert(entities); count += 1 /*entities.size*/ },
      e => { p.failure(e) },
      () => p.success())

    Await.result(p.future, timeout)
    logger.info(s"Completed export of $count rows")
  }

}
