package com.blinkboxbooks.mimir.export

import java.sql.Date
import org.squeryl.Schema
import org.squeryl.PrimitiveTypeMode._

// 
// Objects for input schemas.
//
case class Book(id: String, publisherId: String, publicationDate: Date,
  title: String, description: Option[String], languageCode: Option[String], numberOfSections: Int) {
  def this() = this("", "", new Date(0), "", None, None, 0)
}
case class Publisher(id: Int, name: String, ebookDiscount: Int,
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
case class Contributor(id: Int, fullName: String, firstName: Option[String], lastName: Option[String]) {
  def this() = this(0, "", None, None)
}
case class MapBookToContributor(contributorId: Int, isbn: String, role: Int) {
  def this() = this(0, "", 0)
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
    c.fullName is (named("full_name")),
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

  val booksOutput = table[Book]("books")
  on(booksOutput)(b => declare(
    b.id is (named("isbn")),
    b.publisherId is (named("publisher_id")),
    b.publicationDate is (named("publication_date"), dbType("DATE")),
    b.title is (dbType("VARCHAR(255)")),
    b.description is (dbType("VARCHAR(30000)")),
    b.languageCode is (named("language_code"), dbType("CHAR(2)")),
    b.numberOfSections is (named("number_of_sections"))))

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

  val contributorsOutput = table[Contributor]("contributors")
  on(contributorsOutput)(c => declare(
    c.fullName is (named("full_name"), dbType("VARCHAR(256)")),
    c.firstName is (named("first_name"), dbType("VARCHAR(256)")),
    c.lastName is (named("last_name"), dbType("VARCHAR(256)"))))

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

  printDdl(str => println(str))

}
