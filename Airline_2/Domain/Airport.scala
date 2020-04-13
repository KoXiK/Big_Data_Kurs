package ru.philit.bigdata.vsu.other.Airline_2.Domain

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Airport extends Enumeration {

  private val DELIMITER = ","

  val AIRPORT_ID, NAME, CITY, COUNTRY,
  IATA, ICAO, LAT, LON, ALTITUDE,
  TIMEZONE, DST, TZ, TYPE, SOURCE = Value

  val structType = StructType(
    Seq(
      StructField(AIRPORT_ID.toString, IntegerType),
      StructField(NAME.toString, StringType),
      StructField(CITY.toString, StringType),
      StructField(COUNTRY.toString, StringType),
      StructField(IATA.toString, StringType),
      StructField(ICAO.toString, StringType),
      StructField(LAT.toString, StringType),
      StructField(LON.toString, StringType),
      StructField(ALTITUDE.toString, StringType),
      StructField(TIMEZONE.toString, StringType),
      StructField(DST.toString, StringType),
      StructField(TZ.toString, StringType),
      StructField(TYPE.toString, StringType),
      StructField(SOURCE.toString, StringType)
    )
  )

  def apply(row: String): Airport = {
    val array = row.replace("\"", "").split(DELIMITER)
    Airport(
      array(AIRPORT_ID.id).toInt,
      array(NAME.id),
      array(CITY.id),
      array(COUNTRY.id),
      array(IATA.id),
      array(ICAO.id),
      array(LAT.id),
      array(LON.id),
      array(ALTITUDE.id),
      array(TIMEZONE.id),
      array(DST.id),
      array(TZ.id),
      array(TYPE.id),
      array(SOURCE.id)
    )
  }
}

case class Airport(airportID: Int,
                   airportName: String,
                   city: String,
                   country: String,
                   iata: String,
                   icao: String,
                   lat: String,
                   lon: String,
                   altitude: String,
                   timezone: String,
                   dst: String,
                   tz: String,
                   typeAir: String,
                   source: String
                  )
