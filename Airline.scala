package bigData
import scala.io.Source

case class Route(
                  idAirline: Int,
                  nameSourceAirport: String,
                  idSourceAirport: Int,
                  nameTargetAirport: String,
                  idTargetAirport: Int
                )

case class Airport(
                    idAirport: Int,
                    nameAirport: String,
                    country: String
                  )
case class Airline(
                    idAirline: Int,
                    nameAirline: String,
                    country: String,
                    active: Boolean
                  )
case class StatCountry[A](allVisitedCountry: Seq[A], airport: Seq[A])

object Airline extends App
{

  def countryStat(airline: String): Map[String,StatCountry[String]] = {
    val contentAirline = Source.fromFile("./datasource/avia/airlines.dat.txt")
      .getLines().map(
        line =>
          line.split(",",-1)).toSeq.map {
            case Array(idAirline,nameAirline,_,_,_,_,country,active,_*) =>
              Airline(
                if(!idAirline.contains("N")) idAirline.toInt else 2020,
                nameAirline.substring(1, nameAirline.length() - 1),
                country.substring(1, country.length() - 1),
                active.substring(1, active.length() - 1) == "Y"
              )
          }

    val contentAirport = Source.fromFile("./datasource/avia/airports.dat.txt")
      .getLines().map(
        line =>
          line.split(",",-1)).toSeq.map {
            case Array(id_airline,name,_,country,_,_,_,_,_,_,_,_,_,_*) =>
              Airport(
                if(!id_airline.contains("N")) id_airline.toInt else 2020,
                name.substring(1, name.length() - 1),
                country.substring(1, country.length() - 1)
              )
          }


    val contentRoutes = Source.fromFile("./datasource/avia/routes.dat.txt")
      .getLines().map(
        line =>
          line.split(",",-1)).toSeq.map {
            case Array(_,id,sourceAir,idSource,targetAir,idTarget,_,_,_*) =>
              Route(
                if(!id.contains("N")) id.toInt else 2020,
                sourceAir,if(!idSource.contains("N")) idSource.toInt else 2020,
                targetAir,if(!idTarget.contains("N")) idTarget.toInt else 2020
              )
          }

    val currentAirline = contentAirline.filter(y => y.nameAirline == airline && y.active)

    val idCurrentAirline: Int = currentAirline.map(x => x.idAirline).head

    val countryCurrentAirline: String = currentAirline.map(x => x.country).head

    val sortedAirport = contentAirport.filter(y => y.country == countryCurrentAirline).map(
      x => x.idAirport).flatten(x => Seq(x))

    val sortedRoute = contentRoutes.filter(
      y => y.idAirline == idCurrentAirline && !sortedAirport.contains(y.idTargetAirport)
    )

    val sortedRouteOnIdAirport = sortedRoute.map(
      x => x.idTargetAirport
    ).flatten(x => Seq(x))

    val sortedRouteOnAirports = sortedRoute.map(
      x => x.nameTargetAirport
    ).flatten(
      x => Seq(x)
    ).distinct



      val sortedAirportOnCountry = contentAirport.filter(
        y => sortedRouteOnIdAirport.contains(y.idAirport)
      ).map(
        x => x.country
      ).flatten(
        x => Seq(x)
      ).distinct

      Map(countryCurrentAirline -> StatCountry(sortedAirportOnCountry, sortedRouteOnAirports))
    }

  val MapCountry = countryStat("Aerocondor")

  println("Посещённые страны: ")
  MapCountry("Portugal").allVisitedCountry.foreach(x => println(x + " "))
    
  println("Посещённые аэропорты")
  MapCountry("Portugal").airport.foreach(x => print(x + " "))

}
