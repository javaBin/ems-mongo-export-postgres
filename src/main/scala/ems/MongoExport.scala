package ems

import com.mongodb.casbah.Imports._
import java.util.{Date => JDate}
import org.joda.time._
import argonaut._, Argonaut._

object MongoExport extends App {

  val db = MongoClient()("ems")
  val noNullParams = PrettyParams.nospace.copy(dropNullKeys = true)

  def getDateTimeString(m: MongoDBObject, name: String): String = {
    new DateTime(m.getAsOrElse[JDate](name, new JDate())).withZone(DateTimeZone.UTC).toString()
  }


  val events = db("event").find().sort(MongoDBObject("name" -> 1)).toVector

  val insertEvents = events.map(e =>
    "INSERT INTO event(id, name, slug, venue, lastModified) VALUES('%s','%s','%s','%s','%s')".format(
      e.as[String]("_id"),
      e.as[String]("name"),
      e.as[String]("slug"),
      e.as[String]("venue"),
      getDateTimeString(e, "last-modified")
    )
  )

  val insertRooms = events.flatMap{e =>
    val eventId = e.as[String]("_id")
    e.as[List[DBObject]]("rooms").map(r =>
      "INSERT INTO room(id, eventId, name, lastModified) VALUES('%s','%s','%s','%s')".format(
        r.as[String]("_id"),
        eventId,
        r.as[String]("name"),
        getDateTimeString(r, "last-modified")
      )
    )
  }

  val slots = db("slot").find().sort(MongoDBObject("eventId" -> 1)).toVector

  val insertSlots = slots.map(s =>
      "INSERT INTO slot(id, eventId, parentId, start, duration, lastModified) VALUES('%s','%s',%s,'%s',%s,'%s')".format(
      s.as[String]("_id"),
      s.getAs[String]("eventId").getOrElse("null"),
      s.getAs[String]("parentId").map(s => "'%s'".format(s)).getOrElse("null"),
      getDateTimeString(s, "start"),
      s.getAsOrElse[Double]("duration", 0D).toInt,
      getDateTimeString(s, "last-modified")
    )
  )

  val publishedSessions = db("session").find(MongoDBObject("published" -> true)).sort(MongoDBObject("title" -> 1)).filterNot(_.getAs[String]("slug").exists(_.isEmpty)).toVector
  val draftSessions = db("session").find(MongoDBObject("published" -> false)).sort(MongoDBObject("title" -> 1)).filterNot(_.getAs[String]("slug").exists(_.isEmpty)).toVector

  val insertSessions = toInsertSessions(publishedSessions) ++ toInsertSessions(draftSessions)

  def toInsertSessions(sessions: Vector[DBObject]) = {

    sessions.map{s =>
      val json = Json.obj(
        "title"     := s.as[String]("title"),
        "summary"   := s.getAs[String]("summary"),
        "body"      := s.getAs[String]("body"),
        "audience"  := s.getAs[String]("audience"),
        "outline"   := s.getAs[String]("outline"),
        "equipment" := s.getAs[String]("equipment"),
        "language"  := s.getAsOrElse[String]("language", "no"),
        "level"     := s.getAsOrElse[String]("level", "beginner"),
        "format"    := s.getAsOrElse[String]("format", "presentation"),
        "labels"      := Json.obj(
          "tags"      := s.getAsOrElse[List[String]]("tags", Nil),
          "keywords"  := s.getAsOrElse[List[String]]("keywords", Nil)
        )
      ).pretty(noNullParams).replace("'", "''")

      val att = s.as[List[DBObject]]("attachments")
      val video = att.
        find(obj => obj.getAs[String]("href").exists(_.contains("vimeo.com"))).
        flatMap(obj => obj.getAs[String]("href"))

      "INSERT INTO session(id, eventId, slug, state, abstract, video, published, roomId, slotId, lastModified) VALUES ('%s','%s','%s','%s','%s',%s,%s,%s,%s,'%s')"format(
        s.as[String]("_id"),
        s.as[String]("eventId"),
        s.as[String]("slug"),
        s.as[String]("state"),
        json,
        video.filterNot(_ == "null").map(v => "'%s'".format(v)).getOrElse("null"),
        s.getAsOrElse[Boolean]("published", false),
        s.getAs[String]("roomId").filterNot(_ == "null").map(v => "'%s'".format(v)).getOrElse("null"),
        s.getAs[String]("slotId").filterNot(_ == "null").map(v => "'%s'".format(v)).getOrElse("null"),
        getDateTimeString(s, "last-modified")
        )
    }
  }

  def toInsertSpeakers(sessions: Vector[DBObject]) = {
    sessions.flatMap{ sess =>
      val sessionId = sess.as[String]("_id")
      sess.as[List[DBObject]]("speakers").map{s =>
        val json = Json.obj(
          "name"      := s.as[String]("name"),
          "zipcode"   := s.getAs[String]("zip-code").filterNot(_ == null).filterNot(_ == "null").filterNot(_.isEmpty),
          "bio"       := s.getAs[String]("bio"),
          "labels"    := Json.obj(
             "tags"   := s.getAsOrElse[List[String]]("tags", Nil).filterNot(_ == null).filterNot(_ == "null").filterNot(_.isEmpty)
          )
        ).pretty(noNullParams).replace("'", "''")
        "INSERT INTO speaker(id, sessionId, email, photo, attributes, lastModified) VALUES ('%s','%s','%s',%s,'%s','%s')".format(
          s.as[String]("_id"),
          sessionId,
          s.as[String]("email"),
          s.getAs[String]("photo").filterNot(_ == "null").map(v => "'%s'".format(v)).getOrElse("null"),
          json,
          getDateTimeString(s, "last-modified")
        )
      }
    }
  }

  val insertSpeakers = toInsertSpeakers(publishedSessions) ++ toInsertSpeakers(draftSessions)

  writeToFile(insertEvents, "events")
  writeToFile(insertSlots, "slots")
  writeToFile(insertRooms, "rooms")
  writeToFile(insertSessions, "sessions")
  writeToFile(insertSpeakers, "speakers")


  def writeToFile(v: Vector[String], name: String) = {
    val export = new java.io.File("/tmp/ems-export")
    if (!export.exists) {
      export.mkdirs()
    }
    borrow(new java.io.FileWriter(new java.io.File(export, s"$name.sql"))){ writer =>
      v.foreach{line => writer.write(line + ";\n")}
      writer.flush()
    }
  }

  def borrow[A <: java.io.Closeable, B](a: => A)(f: A => B): B = {
    val cached = a
    try {
      f(a)
    } finally {
      if (cached != null) cached.close
    }
  }
}
