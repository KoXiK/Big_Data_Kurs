package ru.philit.bigdata.vsu.other.Airline_2.Domain

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Route extends Enumeration {

  private val DELIMITER = ","

  val IATA, AIRLINE_ID, SOURCE_AIRPORT, SOURCE_AIRPORT_ID, TARGET_AIRPORT, TARGET_AIRPORT_ID, _CODESHARE, STOPS, EQUIPMENT_ = Value

  val structType = StructType(
    Seq(
      StructField(IATA.toString, StringType),
      StructField(AIRLINE_ID.toString, IntegerType),
      StructField(SOURCE_AIRPORT.toString, StringType),
      StructField(SOURCE_AIRPORT_ID.toString, IntegerType),
      StructField(TARGET_AIRPORT.toString, StringType),
      StructField(TARGET_AIRPORT_ID.toString, IntegerType),
      StructField(_CODESHARE.toString, StringType),
      StructField(STOPS.toString, StringType),
      StructField(EQUIPMENT_.toString, StringType)
    )
  )

  def apply(row: String): Route = {
    val array = row.replace("\"", "").split(DELIMITER)
    Route(
      array(IATA.id),
      array(AIRLINE_ID.id).toInt,
      array(SOURCE_AIRPORT.id),
      array(SOURCE_AIRPORT_ID.id).toInt,
      array(TARGET_AIRPORT.id),
      array(TARGET_AIRPORT_ID.id).toInt,
      array(_CODESHARE.id),
      array(STOPS.id),
      array(EQUIPMENT_.id)
    )
  }
}

case class Route(iata: String,
                 airlineID: Int,
                 sourceAirline: String,
                 sourceAirlineID: Int,
                 targetAirport: String,
                 targetAirportID: Int,
                 codeshare: String,
                 stops: String,
                 equipment: String)
