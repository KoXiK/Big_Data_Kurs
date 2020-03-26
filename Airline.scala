package bigData

import scala.io.Source

case class Route(airline_id: Int, source_air: String, source_air_id: Int, target_air: String, target_air_id: Int)
case class Airport(airport_id: Int, name_airport: String, country: String)
case class Airline(airline_id: Int, name_airline: String, country: String, active: Boolean)
case class StatCountry[A](count: Seq[A], airport: Seq[A])



object Airline extends App
{

  def countryStat(airline: String)  = //
    {

      val contentAirline = Source.fromFile("/home/vladimir/BigDataKurs/MyHome/master/datasource/avia/airlines.dat.txt")
        .getLines().map(line =>
        line.split(",",-1)).toSeq.map{ case Array(id_airline,name,_,_,_,_,country,active,_*) =>
        Airline(if(!id_airline.contains("N")) id_airline.toInt else 2020, name.substring(1, name.length() - 1), country.substring(1, country.length() - 1), active.substring(1, active.length() - 1) == "Y")
      }

      val contentAirport = Source.fromFile("/home/vladimir/BigDataKurs/MyHome/master/datasource/avia/airports.dat.txt")
        .getLines().map(line =>
        line.split(",",-1)).toSeq.map{ case Array(id_airline,name,_,country,_,_,_,_,_,_,_,_,_,_*) =>
        Airport(if(!id_airline.contains("N")) id_airline.toInt else 2020, name.substring(1, name.length() - 1), country.substring(1, country.length() - 1))
      }


      val contentRoutes = Source.fromFile("/home/vladimir/BigDataKurs/MyHome/master/datasource/avia/routes.dat.txt")
        .getLines().map(line =>
        line.split(",",-1)).toSeq.map{ case Array(_,id,sourceAir,idSource,targetAir,idTarget,_,_,_*) =>
        Route(if(!id.contains("N")) id.toInt else 2020,sourceAir,if(!idSource.contains("N")) idSource.toInt else 2020,targetAir,if(!idTarget.contains("N")) idTarget.toInt else 2020)
      }

      //contentRoutes.foreach(x => println(x.source_air_id))

      val current_airline = contentAirline.filter(y => y.name_airline == airline && y.active)

      val current_airline_id: Int = current_airline.map(x => x.airline_id).head

      val current_airline_country: String = current_airline.map(x => x.country).head

      val sort_airport = contentAirport.filter(y => y.country == current_airline_country).map(x => x.airport_id).flatten{
        x => Seq(x)
      }

      val sort_route = contentRoutes.filter(y => y.airline_id == current_airline_id
        && !sort_airport.contains(y.target_air_id))

      val sort_route_Airport_id = sort_route.map(x => x.target_air_id).flatten{
        x => Seq(x)
      }

      val sort_route_Airport = sort_route.map(x => x.target_air).flatten{
        x => Seq(x)
      }.distinct

      //sort_route_Airport.foreach(x => println(x))

      val sort_air_country = contentAirport.filter(y => sort_route_Airport_id.contains(y.airport_id)).map(x => x.country).flatten{
        x => Seq(x)
      }.distinct

      //sort_air_country.foreach(x => println(x))

      Map(current_airline_country -> StatCountry(sort_air_country, sort_route_Airport))
    }

    val myMap = countryStat("Aerocondor")
    val mapp = myMap("Portugal")

    println("Посещённые страны: ")
    mapp.count.foreach(x => println(x + " "))
    
    println("Посещённые аэропорты")
    mapp.airport.foreach(x => print(x + " "))

}
