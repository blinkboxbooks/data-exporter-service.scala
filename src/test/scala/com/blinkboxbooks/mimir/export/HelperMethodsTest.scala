package com.blinkboxbooks.mimir.export

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BookMediaTest extends FunSuite {
  test("Return full jpg URL from base resource server URL") {
    val expectedConversions = Map(
      "https://media.blinkboxboks.com/path/to/image.png" -> "https://media.blinkboxboks.com/params;v=0/path/to/image.png.jpg",
      "https://media.blinkboxboks.com/path/to/image.jpg" -> "https://media.blinkboxboks.com/path/to/image.jpg"
    )
    val converted = expectedConversions.keySet.map(baseUrl => BookMedia.fullsizeJpgUrl(Some(baseUrl))).flatten
    assert(converted === expectedConversions.values.toSet)
  }

  test("Return None on non-URLs being converted to full jpg URL") {
    val converted = BookMedia.fullsizeJpgUrl(Some("Not a URL"))
    assert(converted === None)
  }
}

@RunWith(classOf[JUnitRunner])
class ContributorTest extends FunSuite {
  test("Generates author URLs") {
    val actualUrl = Contributor.generateContributorUrl("guid","F. Honda Dïcks")
    val expectedUrl = Some("https://www.blinkboxbooks.com/#!/author/guid/f-honda-dicks")
    assert(actualUrl === expectedUrl)
  }

  test("Deals with empty name") {
    val actualUrl = Contributor.generateContributorUrl("guid","")
    val expectedUrl = Some("https://www.blinkboxbooks.com/#!/author/guid/details")
    assert(actualUrl === expectedUrl)
  }
}
