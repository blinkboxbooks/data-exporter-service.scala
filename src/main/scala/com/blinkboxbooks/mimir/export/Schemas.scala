package com.blinkboxbooks.mimir.export

import java.sql.Date
import org.squeryl.Schema
import org.squeryl.PrimitiveTypeMode._

// 
// Objects for schemas.
//
case class Book(id: String, publisherId: String, publicationDate: Date,
  title: String, description: Option[String], languageCode: Option[String], numberOfSections: Int) {
  def this() = this("", "", new Date(0), "", None, None, 0)
}
case class Publisher(id: Int, name: String, ebookDiscount: Float,
  implementsAgencyPricingModel: Boolean, countryCode: Option[String]) {
  def this() = this(0, "", 0, false, None)
}
case class Genre(id: Int, parentId: Option[Int], bisacCode: Option[String], name: Option[String]) {
  def this() = this(0, None, None, None)
}
case class MapBookToGenre(isbn: String, genreId: Int) {
  def this() = this("", 0)
}
case class CurrencyRate(fromCurrency: String, toCurrency: String, rate: BigDecimal) {
  def this() = this("", "", 0)
}
case class Contributor(id: Int, fullName: String, firstName: Option[String], lastName: Option[String], guid: String, imageUrl: Option[String]) {
  def this() = this(0, "", None, None, "", None)
}
case class MapBookToContributor(contributorId: Int, isbn: String, role: Int) {
  def this() = this(0, "", 0)
}
case class BookMedia(id: Int, isbn: String, url: Option[String], kind: Int){
  def this() = this(0, "", Some(""), 0)
}

// Enriched Output Classes
// They contain additional fields that are not in the source data.

case class BookWithCover(id: String, publisherId: String, publicationDate: Date, title: String, description: Option[String],
                         languageCode: Option[String], numberOfSections: Int, coverUrl: Option[String]) {
  def this() = this("", "", new Date(0), "", None, None, 0, None)
}
case class ContributorWithUrls(id: Int, fullName: String, firstName: Option[String], lastName: Option[String], guid: String, imageUrl: Option[String], url: Option[String]) {
  def this() = this(0, "", None, None, "", None, None)
}


// Database enum values
object BookMedia {
  val BOOK_COVER_MEDIA_ID = 0
  val FULL_EPUB_MEDIA_ID = 1
  val SAMPLE_EPUB_MEDIA_ID = 2

  def fullsize_jpg_url(mediaUrl: Option[String]):Option[String] = mediaUrl.map { url =>
    try { new java.net.URL(url) } catch {
      case ex: Exception =>
        return None
    }
    url.takeRight(4) match {
      case ".jpg" => url
      case _ => url.replaceFirst("([^/])/([^/])", "$1/params;v=0/$2") + ".jpg"
    }
  }
}

object Contributor {
  import com.typesafe.config.ConfigFactory
  val config = ConfigFactory.load("data-exporter-service")
  val AUTHOR_BASE_URL = config.getString("author.base.url")

  def generate_url(guid: String, fullName: String): Option[String] = {
    val normalizedName = java.text.Normalizer.normalize(fullName, java.text.Normalizer.Form.NFD).toLowerCase().replaceAll(" ","-").replaceAll("[^a-z-]+", "")
    val normalizedNameWithDefault = if (normalizedName.isEmpty) "details" else normalizedName
    Some(Array(AUTHOR_BASE_URL, guid, normalizedNameWithDefault).mkString("/"))
  }
}

// 
// Input schema definitions. 
//
// These are not full definitions of the corresponding tables, they just specify the columns
// that we read and the info needed to read them (e.g. column name but not details about column types).
//

object ShopSchema extends Schema {

  val bookData = table[Book]("dat_book")
  on(bookData)(b => declare(
    b.id is (named("isbn")),
    b.publisherId is (named("publisher_id")),
    b.publicationDate is (named("publication_date")),
    // Shop DB has longer size of this field than the reporting DB, hence replicate this in tests.
    b.description is dbType(s"varchar(65535)"),
    b.languageCode is (named("language_code")),
    b.numberOfSections is (named("num_sections"))))

  val publisherData = table[Publisher]("dat_publisher")
  on(publisherData)(p => declare(
    p.ebookDiscount is (named("ebook_discount")),
    p.implementsAgencyPricingModel is (named("implements_agency_pricing_model")),
    p.ebookDiscount is (named("ebook_discount")),
    p.countryCode is (named("country_code"))))

  val contributorData = table[Contributor]("dat_contributor")
  on(contributorData)(c => declare(
    c.guid is (named("guid")),
    c.fullName is (named("full_name")),
    c.imageUrl is (named("photo")),
    c.firstName is (named("first_name")),
    c.lastName is (named("last_name"))))

  val mapBookContributorData = table[MapBookToContributor]("map_book_contributor")
  on(mapBookContributorData)(m => declare(
    m.contributorId is (named("contributor_id"))))

  val genreData = table[Genre]("dat_genre")
  on(genreData)(g => declare(
    g.parentId is (named("parent_id")),
    g.bisacCode is (named("bisac_code"))))

  val bookGenreData = table[MapBookToGenre]("map_book_genre")
  on(bookGenreData)(g => declare(
    g.genreId is (named("genre_id"))))

  val currencyRateData = table[CurrencyRate]("dat_currency_rate")
  on(currencyRateData)(e => declare(
    e.fromCurrency is (named("from_currency")),
    e.toCurrency is (named("to_currency"))))

  val bookMediaData = table[BookMedia]("dat_book_media")
  on(bookMediaData)(m => declare(
    m.id is named("id"),
    m.isbn is named("isbn"),
    m.url is named("url"),
    m.kind is named("type")
  ))

}

case class Clubcard(id: Int, cardNumber: String) {
  def this() = this(0, "")
}
case class ClubcardUser(id: Int, userId: String) {
  def this() = this(0, "")
}
case class ClubcardForUser(cardId: Int, userId: Int) {
  def this() = this(0, 0)
}

object ClubcardSchema extends Schema {

  val clubcards = table[Clubcard]("club_card")
  on(clubcards)(c => declare(
    c.cardNumber is (named("card_number"))))

  val users = table[ClubcardUser]("users")
  on(users)(u => declare(
    u.userId is (named("user_id"))))

  val clubcardUsers = table[ClubcardForUser]("club_card_users")
  on(clubcardUsers)(cu => declare(
    cu.cardId is (named("card_id")),
    cu.userId is (named("user_id"))))

}

//
// Output schema definitions.
//

case class UserClubcardInfo(cardId: String, userId: Int) {
  def this() = this("", 0)
}

object ReportingSchema extends Schema {

  val MAX_DESCRIPTION_LENGTH = 30000

  val booksOutput = table[BookWithCover]("books")
  on(booksOutput)(b => declare(
    b.id is (named("isbn")),
    b.publisherId is (named("publisher_id")),
    b.publicationDate is (named("publication_date"), dbType("DATE")),
    b.title is dbType("VARCHAR(255)"),
    b.description is dbType(s"varchar($MAX_DESCRIPTION_LENGTH)"),
    b.languageCode is (named("language_code"), dbType("CHAR(2)")),
    b.numberOfSections is (named("number_of_sections")),
    b.coverUrl is named("cover_url")))

  val publishersOutput = table[Publisher]("publishers")
  on(publishersOutput)(p => declare(
    p.implementsAgencyPricingModel is (named("implements_agency_pricing_model")),
    p.name is (dbType("VARCHAR(128)")),
    p.ebookDiscount is (named("ebook_discount")),
    p.countryCode is (named("country_code"), dbType("VARCHAR(4)"))))

  val userClubcardsOutput = table[UserClubcardInfo]("user_clubcards")
  on(userClubcardsOutput)(c => declare(
    c.cardId is (named("clubcard_id"), dbType("VARCHAR(20)")),
    c.userId is (named("user_id"))))

  val currencyRatesOutput = table[CurrencyRate]("currency_rates")
  on(currencyRatesOutput)(e => declare(
    e.fromCurrency is (named("from_currency"), dbType("VARCHAR(5)")),
    e.toCurrency is (named("to_currency"), dbType("VARCHAR(5)"))))

  val contributorsOutput = table[ContributorWithUrls]("contributors")
  on(contributorsOutput)(c => declare(
    c.fullName is (named("full_name"), dbType("VARCHAR(256)")),
    c.firstName is (named("first_name"), dbType("VARCHAR(256)")),
    c.lastName is (named("last_name"), dbType("VARCHAR(256)")),
    c.guid is (named("guid"), dbType("VARCHAR(256)")),
    c.url is (named("url"), dbType("VARCHAR(256)")),
    c.imageUrl is (named("image_url"), dbType("VARCHAR(256)"))))

  val contributorRolesOutput = table[MapBookToContributor]("contributor_roles")
  on(contributorRolesOutput)(m => declare(
    m.contributorId is (named("contributor_id"))))

  val genresOutput = table[Genre]("genres")
  on(genresOutput)(g => declare(
    g.parentId is (named("parent_id")),
    g.bisacCode is (named("bisac_code"), dbType("VARCHAR(8)"))))

  val bookGenresOutput = table[MapBookToGenre]("book_genres")
  on(bookGenresOutput)(g => declare(
    g.genreId is (named("genre_id")),
    g.isbn is (dbType("VARCHAR(13)"))))

  //  printDdl(str => println(str))

}
