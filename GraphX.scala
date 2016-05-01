// Databricks notebook source exported at Sun, 1 May 2016 20:14:00 UTC
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import scala.io.Source

Logger.getLogger("org").setLevel(Level.ERROR)
Logger.getLogger("akka").setLevel(Level.ERROR)


// COMMAND ----------

//upload CSV files via Tables option in the left panel, then move contents to a more user-friendly folder for further operations
//display(dbutils.fs.ls("/FileStore/tables/"))
//dbutils.fs.mv("/FileStore/tables/axef2d901462124137319/metro_country.csv", "/FileStore/tables/graphx/metro_country.csv")
//dbutils.fs.rm("/FileStore/tables/axef2d901462124137319")

// COMMAND ----------

// class definition
class PlaceNode(val name:String) extends Serializable
case class Metro(override val name:String, population:Int) extends PlaceNode(name)
case class Country(override val name:String) extends PlaceNode(name)

// create RDDs (note: add 0L for metros vs add 100L for countries so that vertex ids are unique between both datasets)
val metros:RDD[(VertexId, PlaceNode)] = sc.textFile("/FileStore/tables/graphx/metro.csv").filter(! _.startsWith("#")).map(_.split(",")).map(x => (0L + x(0).toLong, Metro(x(1), x(2).toInt)))
val countries:RDD[(VertexId, PlaceNode)] = sc.textFile("/FileStore/tables/graphx/country.csv").filter(! _.startsWith("#")).map(_.split(",")).map(x => (100L + x(0).toLong, Country(x(1))))
val mclinks:RDD[Edge[Int]] = sc.textFile("/FileStore/tables/graphx/metro_country.csv").filter(! _.startsWith("#")).map(_.split(",")).map(x => Edge(0L + x(0).toInt, 100L + x(1).toInt, 1))

// create Graph
val nodes = metros ++ countries
val metrosGraph = Graph(nodes, mclinks)

// COMMAND ----------

val v5 = metrosGraph.vertices.take(5) // take 5 vertices
val e5 = metrosGraph.edges.take(5) // take 5 edges
val dst1 = metrosGraph.edges.filter(_.srcId == 1).map(_.dstId).collect() // get destination vertices reached from vertex 1
val src103 = metrosGraph.edges.filter(_.dstId == 103).map(_.srcId).collect() // get source vertices that reach vertex 103


// COMMAND ----------

metrosGraph.numEdges // print number of edges
metrosGraph.numVertices // print number of vertices (metros + countries)

// define utility type and functions
type VertexDegree = (VertexId, Int)
def max(a: VertexDegree, b: VertexDegree) = if (a._2 > b._2) a else b
def min(a: VertexDegree, b: VertexDegree) = if (a._2 <= b._2) a else b

metrosGraph.outDegrees.reduce(max) // every metropolitis has one country
metrosGraph.vertices.filter(_._1 == 43).collect()

metrosGraph.inDegrees.reduce(max) // get the country with more metropolis
metrosGraph.vertices.filter(_._1 == 108).collect()

metrosGraph.outDegrees.filter(_._2 <= 1).count() // number of vertices with one outgoing edge is equals to number of metropolis (65)
metrosGraph.degrees.reduce(max) // get the vertex with more connections (inbound + outbound): connectedness

// build a degree histogram for the countries to get number of countries with 1 metro, number of countries with 2 metros, ...
metrosGraph.degrees.filter(_._1 >= 100).map(x => (x._2, x._1)).groupByKey.map(x => (x._1, x._2.size)).sortBy(_._1).collect()

// COMMAND ----------


