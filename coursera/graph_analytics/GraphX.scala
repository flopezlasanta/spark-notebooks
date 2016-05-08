// Databricks notebook source exported at Sun, 8 May 2016 12:38:05 UTC
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import scala.io.Source
 
Logger.getLogger("org").setLevel(Level.ERROR)
Logger.getLogger("akka").setLevel(Level.ERROR)


// COMMAND ----------

//upload CSV files via Tables option in the left panel, then move contents to a more user-friendly folder for further operations

import java.nio.file.{Path, Paths, Files}
def moveToTarget(str:String):Unit = {
  val path = Paths.get(str)
  if (Files.exists(path)) {
    dbutils.fs.mv(path.toString, "/FileStore/tables/graphx/" + path.getFileName)
    dbutils.fs.rm(path.getParent.toString)
  }
}
//moveToTarget("/FileStore/tables/i1kzjjyx1462604201728/country_continent.csv")

display(dbutils.fs.ls("/FileStore/tables/graphx"))

// COMMAND ----------

def isHeader(str:String):Boolean = str.startsWith("#")

// COMMAND ----------

// class definition
class PlaceNode(val name:String) extends Serializable
case class Metro(override val name:String, population:Int) extends PlaceNode(name)
case class Country(override val name:String) extends PlaceNode(name)
case class Continent(override val name: String) extends PlaceNode(name)

// when creating tuples for vertices and edges differentiate by adding a certain amount to their ids: +0:metros; +100:countries; +200:continents / +0:metro-country; +100:country-continent

// load metros and countries, then set connections between them, then create the metros graph
val metros:RDD[(VertexId, PlaceNode)] = sc.textFile("/FileStore/tables/graphx/metro.csv").filter(!isHeader(_)).map(_.split(",")).map(x => (0L + x(0).toInt, Metro(x(1), x(2).toInt)))
val countries:RDD[(VertexId, PlaceNode)] = sc.textFile("/FileStore/tables/graphx/country.csv").filter(!isHeader(_)).map(_.split(",")).map(x => (100L + x(0).toInt, Country(x(1))))
val mclinks:RDD[Edge[Int]] = sc.textFile("/FileStore/tables/graphx/metro_country.csv").filter(!isHeader(_)).map(_.split(",")).map(x => Edge(0L + x(0).toInt, 100L + x(1).toInt, 1))
val mcnodes = metros ++ countries
val mcGraph = Graph(mcnodes, mclinks)

// load continents and connections with countries, then create the countries Graph
val continents:RDD[(VertexId, PlaceNode)] = sc.textFile("/FileStore/tables/graphx/continent.csv").filter(!isHeader(_)).map(_.split(",")).map(x => (200L + x(0).toInt, Continent(x(1))))
val cclinks:RDD[Edge[Int]] = sc.textFile("/FileStore/tables/graphx/country_continent.csv").filter(!isHeader(_)).map(_.split(",")).map(x => Edge(100L + x(0).toInt, 200L + x(1).toInt, 1))
val mcclinks = mclinks ++ cclinks
val mccnodes = metros ++ countries ++ continents
val mccGraph = Graph(mccnodes, mcclinks)

// note: somehow I cannot join metros, countries and continents unless they are created under the same widget (...)

// COMMAND ----------

val v5 = mcGraph.vertices.take(5) // take 5 vertices
val e5 = mcGraph.edges.take(5) // take 5 edges
val dst1 = mcGraph.edges.filter(_.srcId == 1).map(_.dstId).collect() // get destination vertices reached from vertex 1
val src103 = mcGraph.edges.filter(_.dstId == 103).map(_.srcId).collect() // get source vertices that reach vertex 103


// COMMAND ----------

mcGraph.numEdges // print number of edges
mcGraph.numVertices // print number of vertices (metros + countries)

// define utility type and functions
type VertexDegree = (VertexId, Int)
def max(a: VertexDegree, b: VertexDegree) = if (a._2 > b._2) a else b
def min(a: VertexDegree, b: VertexDegree) = if (a._2 <= b._2) a else b

mcGraph.outDegrees.reduce(max) // every metropolitis has one country
mcGraph.vertices.filter(_._1 == 43).collect()

mcGraph.inDegrees.reduce(max) // get the country with more metropolis
mcGraph.vertices.filter(_._1 == 108).collect()

mcGraph.outDegrees.filter(_._2 <= 1).count() // number of vertices with one outgoing edge is equals to number of metropolis (65)
mcGraph.degrees.reduce(max) // get the vertex with more connections (inbound + outbound): connectedness

// build a degree histogram for the countries to get number of countries (x) with 1 metro (1, x), number of countries (y) with 2 metros (2, y), ... where x, y are the degree
val metrosHist = mcGraph.degrees.filter(_._1 >= 100).map(x => (x._2, x._1)).groupByKey.map(x => (x._1, x._2.size)).sortBy(_._1).collect()

// COMMAND ----------

//import breeze.linalg._ // not really used, see below
//import breeze.plot._ // not working in Databricks thus another approach is used for plotting, see below

// define a function to calculate the degree histogram (same as before thus no need to recalculate, just use metrosHist instead)
//def degreeHistogram(net: Graph[PlaceNode, Int]): Array[(Int, Int)] = net.degrees.filter(_._1 >= 100).map(x => (x._2, x._1)).groupByKey.map(x => (x._1, x._2.size)).sortBy(_._1).collect()

// calculate probability distribution                  
val totalCountries = countries.count() // total number of countries
val degreeDistribution = metrosHist.map { case (degree, degreeCountries) => (degree, degreeCountries.toDouble / totalCountries) }

//val x = new DenseVector(degreeDistribution map (_._1.toDouble))
//val y = new DenseVector(degreeDistribution map (_._2))

// plot degree distribution by transforming to a DataFrame so that I can use Databricks plotting capabilities
val ddLast = degreeDistribution.last._1 // take last degree
val ddMap = degreeDistribution.toMap
val ddHist = (for (i <- 0 to ddLast) yield { if (ddMap.contains(i)) (i, ddMap(i)) else (i, 0.0) }).toArray

case class DegreeDistribution(degree: Int, distribution: Double)
val ddDF = sc.parallelize(ddHist.map { case (degree, distribution) => DegreeDistribution(degree, distribution)}).toDF()
ddDF.registerTempTable("degree_distribution_table")
display(sqlContext.sql("select * from degree_distribution_table"))

// COMMAND ----------

import java.util.Locale

var countries:Map[String, String] = Map()
for (iso <- Locale.getISOCountries()) countries = countries ++ Map(new Locale("", iso).getDisplayCountry() -> new Locale("", iso).getISO3Country())

def iso(country:String) = countries(country)

// COMMAND ----------

case class Vertex(name:String, population:Int, entity:String)

// override previous definitions; common structure now: id, Vertex(name, population, type (metro, country, continent))
// by default population in countries and continents is zero... in the next widget they will be calculated
val metros = sc.textFile("/FileStore/tables/graphx/metro.csv").filter(!isHeader(_)).map(_.split(",")).map(x => (0L + x(0).toInt, Vertex(x(1), x(2).toInt, "metro")))
val countries = sc.textFile("/FileStore/tables/graphx/country.csv").filter(!isHeader(_)).map(_.split(",")).map(x => (100L + x(0).toInt, Vertex(iso(x(1)), 0, "country")))
val mclinks = sc.textFile("/FileStore/tables/graphx/metro_country.csv").filter(!isHeader(_)).map(_.split(",")).map(x => Edge(0L + x(0).toInt, 100L + x(1).toInt, 1))
val continents = sc.textFile("/FileStore/tables/graphx/continent.csv").filter(!isHeader(_)).map(_.split(",")).map(x => (200L + x(0).toInt, Vertex(x(1), 0, "continent")))
val cclinks = sc.textFile("/FileStore/tables/graphx/country_continent.csv").filter(!isHeader(_)).map(_.split(",")).map(x => Edge(100L + x(0).toInt, 200L + x(1).toInt, 1))

val mccGraph = Graph(metros ++ countries ++ continents, mclinks ++ cclinks)

// goal: aggregate population from metros to countries
// to destination vertex sends a message with 3 values: Vertex = (dst name, src population, dst entity)
val countriesAgg: VertexRDD[Vertex] = mccGraph.aggregateMessages[Vertex](
  t => { if (t.dstAttr.entity == "country") t.sendToDst(Vertex(t.dstAttr.name, t.srcAttr.population, t.dstAttr.entity)) },
  (a, b) => Vertex(a.name, a.population + b.population, a.entity)
)
val mccGraphMC = mccGraph.joinVertices(countriesAgg)((id, a, b) => b)

// goal: aggregate population from countries to continents
// to destination vertex sends a message with 3 values: Vertex = (dst name, src population, dst entity)
val continentsAgg: VertexRDD[Vertex] = mccGraph.aggregateMessages[Vertex](
  t => { if (t.dstAttr.entity == "continent") t.sendToDst(Vertex(t.dstAttr.name, t.srcAttr.population, t.dstAttr.entity)) },
  (a, b) => Vertex(a.name, a.population + b.population, a.entity)
)
val mccGraphMCC = mccGraph.joinVertices(continentsAgg)((id, a, b) => b)

val mccGF = GraphFrame.fromGraphX(mccGraphMCC)
display(mccGF.vertices)

// COMMAND ----------


