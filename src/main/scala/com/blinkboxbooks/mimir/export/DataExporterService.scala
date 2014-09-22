package com.blinkboxbooks.mimir.export

import com.blinkbox.books.logging.Loggers
import com.blinkbox.books.config.Configuration
import com.typesafe.scalalogging.slf4j.Logging
import it.sauronsoftware.cron4j.Scheduler
import java.sql.Date
import java.util.concurrent.TimeUnit
import javax.sql.DataSource
import org.apache.commons.dbcp.BasicDataSource
import org.squeryl.{ SessionFactory, Session, Schema, Query, Table }
import org.squeryl.adapters.MySQLAdapter
import org.squeryl.PrimitiveTypeMode._
import rx.lang.scala.Observable
import scala.concurrent.{ Await, promise }
import scala.concurrent.duration._

object DataExporterService extends App with Configuration with Logging with Loggers {

  import ShopSchema._
  import ClubcardSchema._
  import ReportingSchema._
  import DbUtils._

  val serviceConfig = config.getConfig("service.dataExporter")

  val batchSize = serviceConfig.getInt("jdbcBatchsize")
  val timeout = serviceConfig.getDuration("jdbcTimeout", TimeUnit.MILLISECONDS).millis
  val authorBaseUrl = serviceConfig.getString("authorBaseUrl")

  // Configure datasources for reading and writing.
  val shopDatasource = createDatasource(serviceConfig.getConfig("shopDb"))
  val clubcardDatasource = createDatasource(serviceConfig.getConfig("clubcardDb"))
  val outputDatasource = createDatasource(serviceConfig.getConfig("reportingDb"))

  if (args.contains("--now")) {
    logger.info("Starting one-off export")
    runDataExport(shopDatasource, clubcardDatasource, outputDatasource, batchSize, timeout)
    logger.info("Completed export")
  } else {
    val cronStr = serviceConfig.getString("schedule")
    logger.info(s"Scheduling data export with configuration: $cronStr")
    val scheduler = new Scheduler()
    scheduler.schedule(cronStr, new Runnable() {
      override def run() {
        logger.info("Starting scheduled export")
        try {
          runDataExport(shopDatasource, clubcardDatasource, outputDatasource, batchSize, timeout)
          logger.info("Completed scheduled export")
        } catch {
          case e: Exception => logger.error("Failed data export", e)
        }
      }
    })
    scheduler.setDaemon(false)
    scheduler.start()
  }

  /**
   * Perform all the data export jobs.
   */
  def runDataExport(shopDatasource: DataSource, clubcardDatasource: DataSource, outputDatasource: DataSource,
    batchSize: Int, timeout: Duration, authorBaseUrl: String = authorBaseUrl) = {

    implicit val t = timeout
    implicit val b = batchSize

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
      withReadOnlySession(shopDatasource)(shopSession => {
        using(shopSession) {

          copy(from(publisherData)(select(_)), publishersOutput, identity[Publisher])
          copy(from(mapBookContributorData)(select(_)), contributorRolesOutput, identity[MapBookToContributor])
          copy(from(genreData)(select(_)), genresOutput, identity[Genre])
          copy(from(bookGenreData)(select(_)), bookGenresOutput, identity[MapBookToGenre])
          copy(from(currencyRateData)(select(_)), currencyRatesOutput, identity[CurrencyRate])

          val bookResults =
            join(bookData, bookMediaData.leftOuter)((book, media) =>
              where(media.map(_.kind) === BookMedia.BOOK_COVER_MEDIA_ID or (media.map(_.kind).isNull))
                select (book, media.getOrElse(new BookMedia(1, book.id, Some(""), BookMedia.BOOK_COVER_MEDIA_ID)))
                on (book.id === media.map(_.isbn).get))
          val bookConverter = (b: (Book, BookMedia)) =>
            new OutputBook(b._1.id, b._1.publisherId, b._1.discount, b._1.publicationDate, b._1.title, b._1.description.map({ _.take(ReportingSchema.MAX_DESCRIPTION_LENGTH) }),
              b._1.languageCode, b._1.numberOfSections, BookMedia.fullsizeJpgUrl(b._2.url))
          copy(bookResults, booksOutput, bookConverter)

          val contributorConverter = (c: Contributor) =>
            new OutputContributor(c.id, c.fullName, c.firstName, c.lastName, c.guid, BookMedia.fullsizeJpgUrl(c.imageUrl), Contributor.generateContributorUrl(authorBaseUrl, c.guid, c.fullName))
          copy(from(contributorData)(select(_)), contributorsOutput, contributorConverter)
        }
      })

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
    implicit bufferSize: Int, outputSession: Session, timeout: Duration) = {

    logger.info(s"Executing data export to table ${output.name}")

    var count = 0
    val p = promise[Unit]
    val observable = Observable.from(input)
      .map(converter)
      .buffer(bufferSize)

    observable.subscribe(
      entities => using(outputSession) { output.insert(entities); count += entities.size },
      e => { p.failure(e) },
      () => p.success())

    Await.result(p.future, timeout)
    logger.info(s"Completed export of $count rows")
  }

  /**
   *  Limit description field as that's potentially bigger in the incoming database - the Shop DB version allows
   *  longer varchars than the reporting database allows.
   */
  def truncate(book: Book): Book = book.copy(description = book.description.map(_.take(ReportingSchema.MAX_DESCRIPTION_LENGTH)))

}
