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
  implicit val batchSize = 100

  var shopDbSession: Session = _
  var clubcardDbSession: Session = _
  var reportingDbSession: Session = _

  // Test data.
  val books = List(book("isbn1", "pub1"), book("isbn2", "pub2").copy(description = Some("descr 2")))
  val publishers = List(publisher(1, books(0).publisherId), publisher(2, books(1).publisherId))
  val contributors = List(new Contributor(11, "Bill Bryson", Some("Bill"), Some("Bryson")),
    new Contributor(22, "Leo", Some("Leo"), Some("Tolstoy")))

  val genres = List(new Genre(1, None, Some("FIC00000"), Some("Fiction")),
    new Genre(2, Some(1), Some("FIC00002"), Some("Pulp Fiction")),
    new Genre(3, Some(1), Some("FIC00003"), Some("Highbrow Fiction")))
  val bookGenres = List(new MapBookToGenre(books(0).id, genres(0).id),
    new MapBookToGenre(books(1).id, genres(1).id))

  val bookContributors = List(new MapBookToContributor(contributors(0).id, books(0).id, 0),
    new MapBookToContributor(contributors(1).id, books(1).id, 1))

  val currencyRates = List(new CurrencyRate("GBP", "EUR", 1.2315),
    new CurrencyRate("GBP", "USD", 1.3069),
    new CurrencyRate("GBP", "JPY", 132.4146))

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

    DataExporterService.runDataExport(shopDatasource, clubcardDatasource, outputDatasource, batchSize, timeout)

    checkSuccessfulExport()
  }

  test("Batch size of 1 should produce same results as with larger batches") {
    val shopDatasource = testDatasource("shop")
    val clubcardDatasource = testDatasource("clubcard")
    val outputDatasource = testDatasource("reporting")

    DataExporterService.runDataExport(shopDatasource, clubcardDatasource, outputDatasource, 1, timeout)

    checkSuccessfulExport()
  }

  def checkSuccessfulExport() {
    using(reportingDbSession) {
      assert(from(booksOutput)(select(_)).toList === books)
      assert(from(publishersOutput)(select(_)).toList === publishers)
      assert(from(contributorsOutput)(select(_)).toList === contributors)
      assert(from(contributorRolesOutput)(select(_)).toList === bookContributors)

      assert(from(userClubcardsOutput)(select(_)).toList === List(
        new UserClubcardInfo("card1", 101), new UserClubcardInfo("card2", 102)))

      assert(from(currencyRatesOutput)(select(_)).toList === currencyRates)

      assert(from(genresOutput)(select(_)).toList === genres)
      assert(from(bookGenresOutput)(select(_)).toList === bookGenres)
    }
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
      DataExporterService.runDataExport(shopDatasource, clubcardDatasource, outputDatasource, batchSize, timeout)
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
      DataExporterService.runDataExport(shopDatasource, clubcardDatasource, outputDatasource, batchSize, timeout)
    }
    assert(thrown eq ex, s"Should get original exception back, got: $thrown")

    checkOutputUnchanged()
  }

  test("Successful copy to table with batch size 1") {
    testCopyToTable(1)
  }

  test("Successful copy to table with batch size 100") {
    testCopyToTable(100)
  }

  def testCopyToTable(batchSize: Int) {
    using(reportingDbSession) {
      currencyRatesOutput.deleteWhere(r => 1 === 1)
      DataExporterService.copy(currencyRates, currencyRatesOutput, identity[CurrencyRate])(
        batchSize, reportingDbSession, timeout)
      assert(from(currencyRatesOutput)(select(_)).toList === currencyRates)
    }
  }

  test("Copy to table when input throws exception") {
    using(reportingDbSession) {
      currencyRatesOutput.deleteWhere(r => 1 === 1)
      // For input, use a stream of objects that throws an exception after the first couple of results.
      val ex = new SQLException("Test exception")
      def failure[T]: T = throw ex
      val failingSequence = currencyRates(0) #:: failure[CurrencyRate] #:: currencyRates(1) #:: Stream.empty
      val thrown = intercept[SQLException] {
        DataExporterService.copy(failingSequence, currencyRatesOutput, identity[CurrencyRate])(
          1, reportingDbSession, timeout)
      }
      assert(thrown eq ex, "Should pass on underlying exception")
      assert(from(currencyRatesOutput)(select(_)).toList ===
        List(currencyRates(0)), "Should have stopped updates on failure")
    }
  }

  test("Truncate book details") {
    val b = Book("1234567890123", "123", date(), "Book with big description", Some("12345678990" * 10000), Some("uk"), 10)
    val t = DataExporterService.truncate(b)
    assert(b.description.get.size > ReportingSchema.MAX_DESCRIPTION_LENGTH)
    assert(t.description.get.size == ReportingSchema.MAX_DESCRIPTION_LENGTH)
    assert(t.id == b.id)
    assert(t.publisherId == b.publisherId)
    assert(t.title == b.title)
    assert(t.languageCode == b.languageCode)
    assert(t.numberOfSections == b.numberOfSections)
    assert(t.publicationDate == b.publicationDate)
  }

  test("Save book with an enormous description") {
    val book = Book("1234567890123", "123", date(), "Book with big description", Some("X" * 65534), Some("uk"), 10)
    using(shopDbSession) {
      bookData.insert(book)
    }
    val shopDatasource = testDatasource("shop")
    val clubcardDatasource = testDatasource("clubcard")
    val outputDatasource = testDatasource("reporting")

    DataExporterService.runDataExport(shopDatasource, clubcardDatasource, outputDatasource, 1, timeout)

    using(reportingDbSession) {
      assert(from(booksOutput)(select(_)).toList.size == books.size + 1)
    }
  }

  private def initOutputDb() = {
    shopDbSession = createSession("shop") { ShopSchema.drop; ShopSchema.create }
    clubcardDbSession = createSession("clubcard") { ClubcardSchema.drop; ClubcardSchema.create }
    reportingDbSession = createSession("reporting") { ReportingSchema.drop; ReportingSchema.create }

    // Add interesting content to input tables.
    using(shopDbSession) {
      books.foreach { bookData.insert(_) }
      publishers.foreach { publisherData.insert(_) }
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

  private def publisher(id: Int, name: String, ebookDiscount: Float = 0.2f,
    implementsAgencyPricingModel: Boolean = false, countryCode: Option[String] = None) =
    new Publisher(id, name, ebookDiscount, implementsAgencyPricingModel, countryCode)

  private def book(id: String, publisherId: String, coverUrl: String = "http://media.bbb.com/test.png", publicationDate: Date = date(),
    title: String = "title", description: Option[String] = None, languageCode: Option[String] = None,
    numberOfSections: Int = 42) =
    new Book(id, publisherId, publicationDate, title, description, languageCode, numberOfSections, coverUrl)

  private def insertClubcardForUser(userId: Int, cardId: Int, cardNumber: String) {
    clubcards.insert(new Clubcard(cardId, cardNumber))
    users.insert(new ClubcardUser(userId, userId.toString))
    clubcardUsers.insert(new ClubcardForUser(cardId, userId))
  }

  private def checkOutputUnchanged() {
    using(reportingDbSession) {
      assert(from(booksOutput)(select(_)).toList.size == 1)
      assert(from(publishersOutput)(select(_)).toList.size == 1)
      assert(from(userClubcardsOutput)(select(_)).toList.size == 1)
      assert(from(currencyRatesOutput)(select(_)).toList.size == 1)
      assert(from(contributorsOutput)(select(_)).toList.size == 1)
      assert(from(contributorRolesOutput)(select(_)).toList.size == 1)
      assert(from(genresOutput)(select(_)).toList.size == 1)
      assert(from(bookGenresOutput)(select(_)).toList.size == 1)
    }
  }

  /** Create a datasource that points at a named H2 database. */
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
