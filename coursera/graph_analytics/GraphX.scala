// Databricks notebook source exported at Sun, 1 May 2016 18:18:21 UTC
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


