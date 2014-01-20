package com.blinkboxbooks.mimir.reporting

import scala.xml.XML
import scala.concurrent.{ Promise, future }
import scala.concurrent.duration.DurationInt
import scala.util.{ Try, Success, Failure }
import akka.actor.{ ActorLogging, Actor, ActorRef }
import akka.util.Timeout
import scala.concurrent.ExecutionContext.Implicits.global

object test {

  // new String(UserEventHandlerTest.userUpdatedMessage(101, new java.util.Date(), "Bob", "Jones", "Bobby", "Jonesy").body, "UTF-8")
  // new String(UserEventHandlerTest.userCreatedMessage(101, new java.util.Date(), "Bob", "Jones").body, "UTF-8")

  val doc =
/*    <?xml version="1.0" encoding="UTF-8"?> */
    <userCreated xmlns="http://schemas.blinkboxbooks.com/events/users/v1" xmlns:r="http://schemas.blinkboxbooks.com/messaging/routing/v1" xmlns:v="http://schemas.blinkboxbooks.com/messaging/versioning" r:originator="zuul" v:version="1.0">
      <timestamp>2013-12-30T19:15:23Z</timestamp>
      <user>
        <id>123</id>
        <username>user@domain.com</username>
        <firstName>First Name</firstName>
        <lastName>Last Name</lastName>
        <allowMarketingCommunications>true</allowMarketingCommunications>
			  <r:foo>yeah</r:foo>
      </user>
    </userCreated>                                //> doc  : scala.xml.Elem = <userCreated r:originator="zuul" v:version="1.0" xm
                                                  //| lns:v="http://schemas.blinkboxbooks.com/messaging/versioning" xmlns:r="http
                                                  //| ://schemas.blinkboxbooks.com/messaging/routing/v1" xmlns="http://schemas.bl
                                                  //| inkboxbooks.com/events/users/v1">
                                                  //|       <timestamp>2013-12-30T19:15:23Z</timestamp>
                                                  //|       <user>
                                                  //|         <id>123</id>
                                                  //|         <username>user@domain.com</username>
                                                  //|         <firstName>First Name</firstName>
                                                  //|         <lastName>Last Name</lastName>
                                                  //|         <allowMarketingCommunications>true</allowMarketingCommunications>
                                                  //| 			  <r:foo>yeah</r:foo>
                                                  //|       </user>
                                                  //|     </userCreated>

  doc \ "user" \ "foo"                            //> res0: scala.xml.NodeSeq = NodeSeq(<r:foo xmlns:v="http://schemas.blinkboxbo
                                                  //| oks.com/messaging/versioning" xmlns:r="http://schemas.blinkboxbooks.com/mes
                                                  //| saging/routing/v1" xmlns="http://schemas.blinkboxbooks.com/events/users/v1"
                                                  //| >yeah</r:foo>)

  val knownNamespaces = Set("http://schemas.blinkboxbooks.com/events/users/v1",
  	"http://schemas.blinkboxbooks.com/messaging/routing/v1",
  	"http://schemas.blinkboxbooks.com/messaging/versioning")
                                                  //> knownNamespaces  : scala.collection.immutable.Set[String] = Set(http://sche
                                                  //| mas.blinkboxbooks.com/events/users/v1, http://schemas.blinkboxbooks.com/mes
                                                  //| saging/routing/v1, http://schemas.blinkboxbooks.com/messaging/versioning)

  val someNamespaces = Set("http://schemas.blinkboxbooks.com/events/users/v1")
                                                  //> someNamespaces  : scala.collection.immutable.Set[String] = Set(http://schem
                                                  //| as.blinkboxbooks.com/events/users/v1)

  val limitedNamespaces = Set("http://schemas.blinkboxbooks.com/events/users/v1")
                                                  //> limitedNamespaces  : scala.collection.immutable.Set[String] = Set(http://sc
                                                  //| hemas.blinkboxbooks.com/events/users/v1)
  
  (doc \ "user").filter(e => limitedNamespaces.contains(e.namespace))
                                                  //> res1: scala.xml.NodeSeq = NodeSeq(<user xmlns:v="http://schemas.blinkboxboo
                                                  //| ks.com/messaging/versioning" xmlns:r="http://schemas.blinkboxbooks.com/mess
                                                  //| aging/routing/v1" xmlns="http://schemas.blinkboxbooks.com/events/users/v1">
                                                  //| 
                                                  //|         <id>123</id>
                                                  //|         <username>user@domain.com</username>
                                                  //|         <firstName>First Name</firstName>
                                                  //|         <lastName>Last Name</lastName>
                                                  //|         <allowMarketingCommunications>true</allowMarketingCommunications>
                                                  //| 			  <r:foo>yeah</r:foo>
                                                  //|       </user>)

  (doc \ "user").map(e => e.namespace)            //> res2: scala.collection.immutable.Seq[String] = List(http://schemas.blinkbox
                                                  //| books.com/events/users/v1)
}