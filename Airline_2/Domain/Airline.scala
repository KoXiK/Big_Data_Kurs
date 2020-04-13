package ru.philit.bigdata.vsu.other.Airline_2.Domain

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Airline extends Enumeration {

  private val DELIMITER = ","

  val AIRLINE_ID, NAME, ALIAS, IATA, IC, CALL, COUNTRY, ACTIVE = Value

  val structType = StructType(
    Seq(
      StructField(AIRLINE_ID.toString, IntegerType),
      StructField(NAME.toString, StringType),
      StructField(ALIAS.toString, StringType),
      StructField(IATA.toString, StringType),
      StructField(IC.toString, StringType),
      StructField(CALL.toString, StringType),
      StructField(COUNTRY.toString, StringType),
      StructField(ACTIVE.toString, StringType)
    )
  )

  def apply(row: String): Airline = {
    val array = row.replace("\"", "").split(DELIMITER)
    Airline(
      array(AIRLINE_ID.id).toInt,
      array(NAME.id),
      array(ALIAS.id),
      array(IATA.id),
      array(IC.id),
      array(CALL.id),
      array(COUNTRY.id),
      array(ACTIVE.id)
    )
  }
}

case class Airline(airlineID: Int,
                 airlineName: String,
                 alias: String,
                 iata: String,
                 ic: String,
                 call: String,
                 country: String,
                 active: String)
