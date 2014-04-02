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
import com.typesafe.scalalogging.slf4j.Logging

/**
 * Functional tests for export functionality.
 */
@RunWith(classOf[JUnitRunner])
class DataExporterServiceTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter with MockitoSugar with Logging {

  import ShopSchema._
  import ClubcardSchema._
  import ReportingSchema._

  implicit val timeout = 10 seconds
  implicit val defaultBatchSize = 100

  var shopDbSession: Session = _
  var clubcardDbSession: Session = _
  var reportingDbSession: Session = _

  // Test data.
  val books = List(book("isbn1", "pub1"), book("isbn2", "pub2").copy(description = Some("descr 2")))
  val publishers = List(publisher(1, books(0).publisherId), publisher(2, books(1).publisherId))
  val contributors = List(
    new Contributor(11, "Bill Bryson", Some("Bill"), Some("Bryson"), "cf6134830c918d056a9d8292fdebd5293ea41901", Some("https://media.blinkboxbooks.com/c540/ebb5/2ad3/e8f6/ebfe/5fa9/2f76/f43f.jpg")),
    new Contributor(22, "Leo Tolstoy", Some("Leo"), Some("Tolstoy"), "eee5db331fff59dc80d4d3698145f8304ef56ec8", Some("https://media.blinkboxbooks.com/c540/ebb5/2ad3/e8f6/ebfe/5fa9/2f76/f43f.jpg"))
  )

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

  val bookMedia = List(bookMediaCover(0, books(0).id), bookMediaCover(1, books(1).id))

  // We will output books and contributors enriched with data...
  val contributorsWithUrls = List(addUrlToContributor(contributors(0), Some("https://www.blinkboxbooks.com/#!/author/cf6134830c918d056a9d8292fdebd5293ea41901/bill-bryson")),
    addUrlToContributor(contributors(1), Some("https://www.blinkboxbooks.com/#!/author/eee5db331fff59dc80d4d3698145f8304ef56ec8/leo-tolstoy")))

  val booksWithCovers = books.map({book =>
    bookWithCover(book)
  })

  before {
    initOutputDb()
  }

  override def afterAll {
    // Closing last connections will drop H2 databases.
    List(shopDbSession, clubcardDbSession, reportingDbSession).foreach(_.close)
  }

  test("Successful export") {
    runDataExporter()
    checkSuccessfulExport()
  }

  test("Batch size of 1 should produce same results as with larger batches") {
    runDataExporter(1)
    checkSuccessfulExport()
  }

  def checkSuccessfulExport() {
    using(reportingDbSession) {
      assert(from(booksOutput)(select(_)).toSet === booksWithCovers.toSet)
      assert(from(publishersOutput)(select(_)).toSet === publishers.toSet)
      assert(from(contributorsOutput)(select(_)).toSet === contributorsWithUrls.toSet)
      assert(from(contributorRolesOutput)(select(_)).toSet === bookContributors.toSet)
      assert(from(userClubcardsOutput)(select(_)).toSet === Set(
        new UserClubcardInfo("card1", 101), new UserClubcardInfo("card2", 102)))
      assert(from(currencyRatesOutput)(select(_)).toSet === currencyRates.toSet)
      assert(from(genresOutput)(select(_)).toSet === genres.toSet)
      assert(from(bookGenresOutput)(select(_)).toSet === bookGenres.toSet)
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
      DataExporterService.runDataExport(shopDatasource, clubcardDatasource, outputDatasource, defaultBatchSize, timeout)
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
      DataExporterService.runDataExport(shopDatasource, clubcardDatasource, outputDatasource, defaultBatchSize, timeout)
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
    runDataExporter(2)
    using(reportingDbSession) {
      assert(from(booksOutput)(select(_)).toList.size == books.size + 1)
    }
  }

  test("png Book cover URLs are converted to fullsize jpg URLs"){
    val book = Book("1234567890123", "123", date(), "Book with a png cover url", Some("description"), Some("uk"),
      10)
    val bookMedia = BookMedia(4, book.id, Some("http://media.bbb.com/foobar.png"), 0)

    using(shopDbSession) {
      bookData.insert(book)
      bookMediaData.insert(bookMedia)
    }
    runDataExporter(2)
    using(reportingDbSession) {
      assert(from(booksOutput)(select(_)).toList.size == books.size + 1)
      val theBook = from(booksOutput)(b => where(b.id === book.id) select(b)).head
      assert(theBook.coverUrl.get == "http://media.bbb.com/params;v=0/foobar.png.jpg")
    }
  }

  test("Save a book with no cover url record"){
    // save and export a book without the cover_url row in dat_book_media
    // "This should never happen" - JP

    val book = Book("1234567890123", "123", date(), "Book with big description", Some("Book with no cover url record"), Some("uk"),
      10)
    using(shopDbSession){
      bookData.insert(book)
    }
    runDataExporter(2)
    using(reportingDbSession) {
      val actual = from(booksOutput)(select(_)).toList
      assert(actual.size == books.size + 1)
      assert(actual.contains(bookWithCover(book, None)))
    }

  }

  test("Save a book with a null cover url"){
    // this should also never happen - shop db schema says url field must not be null
    val book = Book("1234567890123", "123", date(), "Book with big description", Some("Book with no cover url record"), Some("uk"),
      10)
    val expected = bookWithCover(book, None)
    val badBookMedia = BookMedia(42, book.id, None, 0)
    using(shopDbSession){
      bookData.insert(book)
      bookMediaData.insert(badBookMedia)
    }
    runDataExporter(2)
    using(reportingDbSession) {
      val actual = from(booksOutput)(select(_)).toList
      assert(actual.size == books.size + 1)
      assert(actual.contains(expected))
      val theBook = from(booksOutput)(b => where(b.id === book.id) select(b)).head
      assert(theBook.coverUrl == None)
    }

  }

  test("Save a contributor with various fields missing"){
    val c1 = newContributor(42, "I.C. Weiner", "guid42", Some("https://media.blinkboxbooks.com/guid42.jpg"))
    val c2 = newContributor(43, "Suq Madiiq", "guid43", None)
    using(shopDbSession){ contributorData.insert(Set(c1, c2)) }
    runDataExporter(2)
    using(reportingDbSession) {
      val actual = from(contributorsOutput)(select(_)).toSet
      assert(actual.size == contributors.size + 2)
      val ic =  from(contributorsOutput)(cont => where(cont.id === c1.id) select(cont)).head
      assert(ic.url == Some("https://www.blinkboxbooks.com/#!/author/guid42/ic-weiner"))
      assert(ic.imageUrl == Some("https://media.blinkboxbooks.com/guid42.jpg"))
      assert(ic.guid == "guid42")
      val suq = from(contributorsOutput)(cont => where(cont.id === c2.id) select(cont)).head
      assert(suq.url == Some("https://www.blinkboxbooks.com/#!/author/guid43/suq-madiiq"))
      assert(suq.imageUrl == None)
      assert(suq.guid == "guid43")
    }
  }

  test("Contributor url is constructed from the contributor's guid and full name"){
    val inShopDb = newContributor(43, "T.S. McTést Face", "guid43")
    val contWithEmptyFullName = newContributor(44, "", "guid44", None)
    using(shopDbSession){ contributorData.insert(Set(inShopDb, contWithEmptyFullName))}
    runDataExporter()
    using(reportingDbSession) {
      val actual = from(contributorsOutput)(cont => where(cont.id === inShopDb.id) select(cont)).head
      val cleanedName = "ts-mctest-face"
      val expected_url = Some("https://www.blinkboxbooks.com/#!/author/" + inShopDb.guid + "/" + cleanedName)
      assert(actual.url == expected_url)
      val badDataContributor = from(contributorsOutput)(cont => where(cont.id === contWithEmptyFullName.id) select(cont)).head
      assert(badDataContributor.url == Some("https://www.blinkboxbooks.com/#!/author/guid44/details"))
    }
  }

  private def runDataExporter(batchSize: Int = defaultBatchSize) = {
    val shopDatasource = testDatasource("shop")
    val clubcardDatasource = testDatasource("clubcard")
    val outputDatasource = testDatasource("reporting")
    DataExporterService.runDataExport(shopDatasource, clubcardDatasource, outputDatasource, batchSize, timeout)
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
      bookMedia.foreach { bookMediaData.insert(_) }
    }
    using(clubcardDbSession) {
      insertClubcardForUser(101, 1001, "card1")
      insertClubcardForUser(102, 1002, "card2")
    }
    using(reportingDbSession) {
      // Insert some existing data into each output table, 
      // so we can check that these get cleared on export.
      booksOutput.insert(new OutputBook())
      publishersOutput.insert(new Publisher())
      userClubcardsOutput.insert(new UserClubcardInfo())
      currencyRatesOutput.insert(new CurrencyRate())
      contributorsOutput.insert(new OutputContributor())
      contributorRolesOutput.insert(new MapBookToContributor())
      genresOutput.insert(new Genre())
      bookGenresOutput.insert(new MapBookToGenre())
    }
  }

  private def newContributor(id: Int, fullname: String, guid: String, imageUrl: Option[String] = None) = {
    Contributor(id, fullname, None, None, guid, imageUrl)
  }

  private def publisher(id: Int, name: String, ebookDiscount: Float = 0.2f,
    implementsAgencyPricingModel: Boolean = false, countryCode: Option[String] = None) =
    new Publisher(id, name, ebookDiscount, implementsAgencyPricingModel, countryCode)

  private def book(id: String, publisherId: String, publicationDate: Date = date(),
    title: String = "title", description: Option[String] = None, languageCode: Option[String] = None,
    numberOfSections: Int = 42) =
    new Book(id, publisherId, publicationDate, title, description, languageCode, numberOfSections)

  private def bookWithCover(sourceBook: Book, coverUrl: Option[String] = Some("http://media.bbb.com/params;v=0/test.png.jpg")) =
    new OutputBook(sourceBook.id, sourceBook.publisherId, sourceBook.publicationDate, sourceBook.title,
      sourceBook.description, sourceBook.languageCode, sourceBook.numberOfSections, coverUrl)

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

  private def bookMediaCover(id: Int, isbn: String) = {
    //dat_book_media type 0 = cover, 1 = full epub, 2 = sample epub
    new BookMedia(id, isbn, Some("http://media.bbb.com/test.png"), 0)
  }

  def addUrlToContributor(c: Contributor, url: Option[String]): OutputContributor = {
    OutputContributor(c.id, c.fullName, c.firstName, c.lastName, c.guid, c.imageUrl, url)
  }

}
