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
      assert(from(booksOutput)(b => select(b)).toList ===
        List(new BookInfo(book1.id, book1.publisherId, book1.publicationDate,
          book1.title, book1.description, book1.languageCode, book1.numberOfSections),
          new BookInfo(book2.id, book2.publisherId, book2.publicationDate,
            book2.title, book2.description, book2.languageCode, book2.numberOfSections)))

      assert(from(publishersOutput)(b => select(b)).toList === List(
        new PublisherInfo(publisher1.id, publisher1.name, publisher1.ebookDiscount,
          publisher1.implementsAgencyPricingModel, publisher1.countryCode),
        new PublisherInfo(publisher2.id, publisher2.name, publisher2.ebookDiscount,
          publisher2.implementsAgencyPricingModel, publisher2.countryCode)))

      assert(from(userClubcardsOutput)(b => select(b)).toList === List(
        new UserClubcardInfo("card1", 101), new UserClubcardInfo("card2", 102)))

      assert(from(currencyRatesOutput)(r => select(r)).toList === currencyRates)
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
      currencyRates.foreach { currencyRateData.insert(_) }
    }
    using(clubcardDbSession) {
      insertClubcardForUser(101, 1001, "card1")
      insertClubcardForUser(102, 1002, "card2")
    }
    using(reportingDbSession) {
      // Insert some existing data into each output table, so we can check that this gets cleared.
      booksOutput.insert(new BookInfo())
      publishersOutput.insert(new PublisherInfo())
      userClubcardsOutput.insert(new UserClubcardInfo())
      currencyRatesOutput.insert(new CurrencyRate())
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
