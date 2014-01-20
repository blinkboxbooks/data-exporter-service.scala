package com.blinkboxbooks.mimir.export

import java.sql.Date
import org.squeryl.Schema
import org.squeryl.PrimitiveTypeMode._

// Input objects.
case class Book(id: String, publisherId: String, publicationDate: Date,
  title: String, description: Option[String], languageCode: Option[String], numberOfSections: Int) {
  def this() = this("", "", new Date(0), "", None, None, 0)
}
case class Publisher(id: Int, name: String, ebookDiscount: Int,
  implementsAgencyPricingModel: Boolean, countryCode: Option[String]) {
  def this() = this(0, "", 0, false, None)
}

// Output objects.
case class BookInfo(id: String, publisherId: String, publicationDate: Date,
  title: String, description: Option[String], languageCode: Option[String], numberOfSections: Int) {
  def this() = this("", "", new Date(0), "", None, None, 0)
}
case class PublisherInfo(id: Int, name: String, ebookDiscount: Int,
  implementsAgencyPricingModel: Boolean, countryCode: Option[String]) {
  def this() = this(0, "", 0, false, None)
}

object Schemas extends Schema {

  //  printDdl(str => println(str))

  // 
  // Input schema definitions
  //
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

  //
  // Output schema definitions.
  //
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

}