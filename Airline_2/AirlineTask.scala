package ru.philit.bigdata.vsu.other.Airline_2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import ru.philit.bigdata.vsu.other.Airline_2.AirlineTask.{getAirline, getAirports, getRoutes, path_airlines, path_airports, path_routes}
import ru.philit.bigdata.vsu.other.Airline_2.Domain.{Airline, Airport, Route}

import scala.util.Try



trait AirlineTrait {

  case class Parameters(countryAirline: String, setCountry: Set[String], setAirport: Set[String])

  val path_airlines: String = "./datasource/avia/airlines.dat.txt"
  val path_routes: String = "./datasource/avia/routes.dat.txt"
  val path_airports: String = "./datasource/avia/airports.dat.txt"
//: Map[String, (Int, Set[String])]


  def countryStat(airline: String)(implicit sc: SparkContext) = {

    val joinAirlineAndRoute = getAirline(path_airlines).filter(_.active.equals("Y"))
      .map {
        case Airline(airlineID, airlineName, _, _, _, _, country, _) => (airlineID, (country, airlineName))
      }.join(getRoutes(path_routes).map {
      case Route(_, airlineID, _, _, _, targetAirportID, _, _, _) => (airlineID, targetAirportID)
      }).map {
      case (airlineID, ((country, airlineName), targetAirportID)) =>
        (targetAirportID,(airlineID, country, airlineName))
      }.join(getAirports(path_airports).map {
      case Airport(airportID, airportName, _, country, _, _, _, _, _, _, _, _, _, _) =>
        (airportID, (airportName, country))
      }).map {
      case (_,((airlineID, countryAirline, airlineName),(airportName, countryAirport))) =>
        (airlineName, (countryAirline, countryAirport, airlineID, airportName))
      }.groupBy {
      case (airlineName, _) => airlineName
      }.map {
      case (key, group) => (key, group.filter(x => x._2._1 != x._2._2).toSeq)
      }.map {
      case (key, group) => (key, group.map {
        case (_, (countryAirline, countryAirport, _, airportName)) => (countryAirline, Set(countryAirport), Set(airportName))
      })
    }

    


  }



  def getAirline(pathAirline: String)(implicit sc: SparkContext): RDD[Airline] = {
    sc.textFile(pathAirline)
      .map {
        case str => Try(Airline(str)).toOption
        case _ => None
      }.filter(_.isDefined)
      .map {
        case Some(x) => x
      }
  }

  def getRoutes(pathRoute: String)(implicit sc: SparkContext): RDD[Route] = {
      sc.textFile(pathRoute)
        .map {
          case str => Try(Route(str)).toOption
          case _ => None
        }
        .filter(_.isDefined)
        .map {
          case Some(x) => x
        }
    }

  def getAirports(pathAirport: String)(implicit sc: SparkContext): RDD[Airport] = {
    sc.textFile(pathAirport)
      .map {
        case str => Try(Airport(str)).toOption
        case _ => None
      }.filter(_.isDefined)
      .map {
        case Some(x) => x
      }
  }

}

object AirlineTask  extends App with AirlineTrait {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("netty").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
    .setAppName("spark-example")
    .setMaster("local[*]")
  implicit val sc = new SparkContext(sparkConf)

  countryStat("Adria Airways")



}
