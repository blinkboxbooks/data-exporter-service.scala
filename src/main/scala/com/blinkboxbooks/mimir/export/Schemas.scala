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

case class BookInfo(id: String, publisherId: String, publicationDate: Date,
  title: String, description: Option[String], languageCode: Option[String], numberOfSections: Int) {
  def this() = this("", "", new Date(0), "", None, None, 0)
}
case class PublisherInfo(id: Int, name: String, ebookDiscount: Int,
  implementsAgencyPricingModel: Boolean, countryCode: Option[String]) {
  def this() = this(0, "", 0, false, None)
}
case class UserClubcardInfo(cardId: String, userId: Int) {
  def this() = this("", 0)
}

object ReportingSchema extends Schema {

  val booksOutput = table[BookInfo]("books")

  on(booksOutput)(b => declare(
    b.id is (named("isbn")),
    b.publisherId is (named("publisher_id")),
    b.publicationDate is (named("publication_date"), dbType("DATE")),
    b.title is (dbType("VARCHAR(255)")),
    b.description is (dbType("VARCHAR(65535)")),
    b.languageCode is (named("language_code"), dbType("CHAR(2)")),
    b.numberOfSections is (named("number_of_sections"))))

  val publishersOutput = table[PublisherInfo]("publishers")
  on(publishersOutput)(p => declare(
    p.implementsAgencyPricingModel is (named("implements_agency_pricing_model")),
    p.ebookDiscount is (named("ebook_discount")),
    p.countryCode is (named("country_code"), dbType("VARCHAR(4)"))))

  val userClubcardsOutput = table[UserClubcardInfo]("user_clubcards")
  on(userClubcardsOutput)(c => declare(
    c.cardId is (named("clubcard_id"), dbType("VARCHAR(20)")),
    c.userId is (named("user_id"))))

  //  printDdl(str => println(str))

}
