package com.blinkboxbooks.mimir.export

import java.util.Calendar
import java.sql.{ DriverManager, Date, SQLException }
import javax.sql.DataSource
import scala.concurrent.duration.DurationInt
import org.junit.runner.RunWith
import org.scalatest.{ FunSuite, BeforeAndAfter, BeforeAndAfterAll }
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.squeryl.{ Session, SessionFactory, Schema }
import org.squeryl.adapters.H2Adapter
import org.squeryl.PrimitiveTypeMode._
import org.apache.commons.dbcp.BasicDataSource
import org.mockito.Mockito._
import org.mockito.Matchers._

/**
 * Functional tests for export functionality.
 */
@RunWith(classOf[JUnitRunner])
class DataExporterServiceTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter with MockitoSugar {

  import ShopSchema._
  import ClubcardSchema._
  import ReportingSchema._

  implicit val timeout = 10 seconds

  var shopDbSession: Session = _
  var clubcardDbSession: Session = _
  var reportingDbSession: Session = _

  // Test data.
  val book1 = book("isbn1", "pub1")
  val book2 = book("isbn2", "pub2").copy(description = Some("descr 2"))
  val publisher1 = publisher(1, book1.publisherId)
  val publisher2 = publisher(2, book2.publisherId)
  val currencyRates = List(new CurrencyRate("GBP", "EUR", 1.2315),
    new CurrencyRate("GBP", "USD", 1.3069),
    new CurrencyRate("GBP", "JPY", 132.4146))

  val contributors = List(new Contributor(11, "Bill Bryson", Some("Bill"), Some("Bryson")),
    new Contributor(22, "Leo", Some("Leo"), Some("Tolstoy")))

  val genres = List(new Genre(1, None, Some("FIC00000"), Some("Fiction")),
    new Genre(2, Some(1), Some("FIC00002"), Some("Pulp Fiction")),
    new Genre(3, Some(1), Some("FIC00003"), Some("Highbrow Fiction")))
  val bookGenres = List(new MapBookToGenre(book1.id, genres(0).id),
    new MapBookToGenre(book2.id, genres(1).id))
  val bookContributors = List(new MapBookToContributor(contributors(0).id, book1.id, 0),
    new MapBookToContributor(contributors(1).id, book2.id, 1))

  before {
    initOutputDb()
  }

  override def afterAll {
    // Closing last connections will drop H2 databases.
    List(shopDbSession, clubcardDbSession, reportingDbSession).foreach(_.close)
  }

  test("Successful export") {
    val shopDatasource = testDatasource("shop")
    val clubcardDatasource = testDatasource("clubcard")
    val outputDatasource = testDatasource("reporting")

    DataExportingService.runDataExport(shopDatasource, clubcardDatasource, outputDatasource, 100)

    checkSuccessfulExport()
  }

  def checkSuccessfulExport() {
    using(reportingDbSession) {
      assert(from(booksOutput)(select(_)).toList === List(book1, book2))
      assert(from(publishersOutput)(select(_)).toList === List(publisher1, publisher2))
      assert(from(contributorsOutput)(select(_)).toList === contributors)
      assert(from(contributorRolesOutput)(select(_)).toList === bookContributors)

      assert(from(userClubcardsOutput)(select(_)).toList === List(
        new UserClubcardInfo("card1", 101), new UserClubcardInfo("card2", 102)))

      assert(from(currencyRatesOutput)(select(_)).toList === currencyRates)

      assert(from(genresOutput)(select(_)).toList === genres)
      assert(from(bookGenresOutput)(select(_)).toList === bookGenres)
    }
  }

  test("Buffer size of 1 should produce same results as with larger batches") {
    val shopDatasource = testDatasource("shop")
    val clubcardDatasource = testDatasource("clubcard")
    val outputDatasource = testDatasource("reporting")

    DataExportingService.runDataExport(shopDatasource, clubcardDatasource, outputDatasource, 1)

    checkSuccessfulExport()
  }

  test("Can't access input database") {
    checkOutputUnchanged()

    val shopDatasource = mock[DataSource]
    val ex = new SQLException("Test exception")
    doThrow(ex).when(shopDatasource).getConnection()
    val clubcardDatasource = testDatasource("clubcard")
    val outputDatasource = testDatasource("reporting")

    checkOutputUnchanged()

    val thrown = intercept[Exception] {
      DataExportingService.runDataExport(shopDatasource, clubcardDatasource, outputDatasource, 100)
    }
    assert(thrown eq ex, s"Should get original exception back, got: $thrown")

    checkOutputUnchanged()
  }

  test("Can't write to output database") {
    checkOutputUnchanged()

    val shopDatasource = testDatasource("shop")
    val clubcardDatasource = testDatasource("clubcard")
    val outputDatasource = mock[DataSource]
    val ex = new SQLException("Test exception")
    doThrow(ex).when(outputDatasource).getConnection()

    checkOutputUnchanged()

    val thrown = intercept[Exception] {
      DataExportingService.runDataExport(shopDatasource, clubcardDatasource, outputDatasource, 100)
    }
    assert(thrown eq ex, s"Should get original exception back, got: $thrown")

    checkOutputUnchanged()
  }

  private def initOutputDb() = {
    shopDbSession = createSession("shop") { ShopSchema.drop; ShopSchema.create }
    clubcardDbSession = createSession("clubcard") { ClubcardSchema.drop; ClubcardSchema.create }
    reportingDbSession = createSession("reporting") { ReportingSchema.drop; ReportingSchema.create }

    // Add interesting content to input tables.
    using(shopDbSession) {
      List(book1, book2).foreach { bookData.insert(_) }
      List(publisher1, publisher2)
        .foreach { publisherData.insert(_) }
      contributors.foreach { contributorData.insert(_) }
      bookContributors.foreach { mapBookContributorData.insert(_) }
      genres.foreach { genreData.insert(_) }
      bookGenres.foreach { bookGenreData.insert(_) }
      currencyRates.foreach { currencyRateData.insert(_) }
    }
    using(clubcardDbSession) {
      insertClubcardForUser(101, 1001, "card1")
      insertClubcardForUser(102, 1002, "card2")
    }
    using(reportingDbSession) {
      // Insert some existing data into each output table, 
      // so we can check that these get cleared on export.
      booksOutput.insert(new Book())
      publishersOutput.insert(new Publisher())
      userClubcardsOutput.insert(new UserClubcardInfo())
      currencyRatesOutput.insert(new CurrencyRate())
      contributorsOutput.insert(new Contributor())
      contributorRolesOutput.insert(new MapBookToContributor())
      genresOutput.insert(new Genre())
      bookGenresOutput.insert(new MapBookToGenre())
    }
  }

  private def publisher(id: Int, name: String, ebookDiscount: Int = 20,
    implementsAgencyPricingModel: Boolean = false, countryCode: Option[String] = None) =
    new Publisher(id, name, ebookDiscount, implementsAgencyPricingModel, countryCode)

  private def book(id: String, publisherId: String, publicationDate: Date = date(),
    title: String = "title", description: Option[String] = None, languageCode: Option[String] = None,
    numberOfSections: Int = 42) =
    new Book(id, publisherId, publicationDate, title, description, languageCode, numberOfSections)

  private def insertClubcardForUser(userId: Int, cardId: Int, cardNumber: String) {
    clubcards.insert(new Clubcard(cardId, cardNumber))
    users.insert(new ClubcardUser(userId, userId.toString))
    clubcardUsers.insert(new ClubcardForUser(cardId, userId))
  }

  private def checkOutputUnchanged() {
    using(reportingDbSession) {
      assert(from(booksOutput)(b => select(b)).toList.size == 1)
      assert(from(publishersOutput)(b => select(b)).toList.size == 1, s"Got: ${from(publishersOutput)(b => select(b)).toList.size}")
      assert(from(userClubcardsOutput)(b => select(b)).toList.size == 1, s"Got: ${from(userClubcardsOutput)(b => select(b)).toList.size}")
    }
  }

  /** Create a datasource that points at the right H2 database. */
  private def testDatasource(name: String) = {
    val datasource = new BasicDataSource
    datasource.setUrl(url(name))
    datasource.setDriverClassName("org.h2.Driver")
    datasource
  }

  /** Create session and initialise the content of its DB by running the given function. */
  private def createSession(dbName: String)(init: => Unit) = {
    val session = Session.create(DriverManager.getConnection(url(dbName)), new H2Adapter)
    using(session) { init }
    session
  }

  private def url(dbName: String) = s"jdbc:h2:mem:${dbName};MODE=MYSQL"

  private def date() = {
    val cal = Calendar.getInstance
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    new Date(cal.getTime().getTime())
  }

}
