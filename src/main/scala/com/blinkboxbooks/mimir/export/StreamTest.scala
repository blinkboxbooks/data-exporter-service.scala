package com.blinkboxbooks.mimir.export

import org.apache.commons.dbcp.BasicDataSource
import com.typesafe.scalalogging.slf4j.Logging
import org.squeryl.{ SessionFactory, Session, Schema }
import org.squeryl.adapters.MySQLAdapter
import org.squeryl.PrimitiveTypeMode._
import rx.lang.scala.Observable
import scala.concurrent.{ Await, promise }
import scala.concurrent.duration._

// Input schemas.
case class Publisher(id: Int, name: String, ebookDiscount: Int,
  implementsAgencyPricingModel: Boolean, countryCode: Option[String]) {
  def this() = this(-1, "", -1, false, None)
}

// Output schemas.
case class PublisherInfo(id: Int, name: String, ebookDiscount: Int,
  implementsAgencyPricingModel: Boolean, countryCode: Option[String]) {
  def this() = this(-1, "", -1, false, None)
}

object StreamTest extends App with Schema with Logging {

  val bufferSize = 5

  // Connect to a DB.
  logger.info("Starting")

  // Input schema defs.
  val publisherData = table[Publisher]("dat_publisher")
  on(publisherData)(p => declare(
    p.ebookDiscount is (named("ebook_discount")),
    p.implementsAgencyPricingModel is (named("implements_agency_pricing_model")),
    p.ebookDiscount is (named("ebook_discount")),
    p.countryCode is (named("country_code"), dbType("VARCHAR(4)"))))

  // Output schema defs.
  val publishersOutput = table[PublisherInfo]("publishers")
  on(publishersOutput)(p => declare(
    p.ebookDiscount is (named("ebook_discount")),
    p.implementsAgencyPricingModel is (named("implements_agency_pricing_model")),
    p.ebookDiscount is (named("ebook_discount")),
    p.countryCode is (named("country_code"), dbType("VARCHAR(4)"))))

  // Configure reporting database that we'll be writing to.
  val outputDatasource = new BasicDataSource
  outputDatasource.setDriverClassName("com.mysql.jdbc.Driver")
  outputDatasource.setUrl("jdbc:mysql://localhost/reporting")
  outputDatasource.setUsername("gospoken")
  outputDatasource.setPassword("gospoken")
  configureThreadPoolParameters(outputDatasource)
  outputDatasource.getConnection.close

  // Configure shop database that we'll get data from.
  val shopDatasource = new BasicDataSource
  shopDatasource.setDriverClassName("com.mysql.jdbc.Driver")
  shopDatasource.setUrl("jdbc:mysql://localhost/shop")
  shopDatasource.setUsername("gospoken")
  shopDatasource.setPassword("gospoken")
  configureThreadPoolParameters(shopDatasource)
  shopDatasource.getConnection.close

  // The global singleton session factory (yuck) refers to the shop database.
  SessionFactory.concreteFactory =
    Some(() => Session.create(shopDatasource.getConnection(), new MySQLAdapter))
  SessionFactory.newSession.bindToCurrentThread

  //printDdl(str => println(str))

  val connection = outputDatasource.getConnection()
  try {
    val outputSession: Session = Session.create(connection, new MySQLAdapter)
    connection.setAutoCommit(false)

    // Clear old data.
    using(outputSession) { publishersOutput.deleteWhere(p => p.id gte 0) }

    //throw new RuntimeException("TESTING TESTING")

    val results = from(publisherData)(p => where(p.id gte 0) select (p))

    val prom = promise[Unit]
    val o = Observable.from(results)
      .map(pub => new PublisherInfo(pub.id, pub.name, pub.ebookDiscount, pub.implementsAgencyPricingModel, pub.countryCode))
      .buffer(bufferSize)
      .doOnEach(pubs => using(outputSession) { publishersOutput.insert(pubs) })
      .doOnError(e => { prom.failure(e); e.printStackTrace(System.err) })
      .doOnCompleted(() => { prom.success() })

    logger.info("Executing copy")

    o.subscribe

    // Wait, with timeout.
    Await.result(prom.future, 1 minute)

    connection.commit()
  } catch {
    case t: Throwable => {
      logger.error("Error while executing database job", t)
      connection.rollback()
    }
  } finally {
    connection.close()
  }

  logger.info("Completed")

  private def configureThreadPoolParameters(datasource: BasicDataSource) = {
    datasource.setMaxActive(10)
    datasource.setMaxIdle(5)
    datasource.setInitialSize(5)
    datasource.setValidationQuery("SELECT 1")
  }

}
