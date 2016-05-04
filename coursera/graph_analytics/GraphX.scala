// Databricks notebook source exported at Wed, 4 May 2016 09:45:58 UTC
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

// build a degree histogram for the countries to get number of countries (x) with 1 metro (1, x), number of countries (y) with 2 metros (2, y), ... where x, y are the degree
val net = metrosGraph.degrees.filter(_._1 >= 100).map(x => (x._2, x._1)).groupByKey.map(x => (x._1, x._2.size)).sortBy(_._1).collect()

// COMMAND ----------

//import breeze.linalg._ // not really used, see below
//import breeze.plot._ // not working in Databricks thus another approach is used for plotting, see below

// define a function to calculate the degree histogram (same as before thus no need to recalculate, just use net)
def degreeHistogram(net: Graph[PlaceNode, Int]): Array[(Int, Int)] = net.degrees.filter(_._1 >= 100).map(x => (x._2, x._1)).groupByKey.map(x => (x._1, x._2.size)).sortBy(_._1).collect()

// calculate probability distribution                  
val nn = metrosGraph.vertices.filter(_._1 >= 100).count() // total number of countries
val degreeDistribution = net.map { case (degree, n) => (degree, n.toDouble / nn) }

//val x = new DenseVector(degreeDistribution map (_._1.toDouble))
//val y = new DenseVector(degreeDistribution map (_._2))

// plot degree distribution by transforming to a dataframe to use Databricks plotting capabilities
val ddLast = degreeDistribution.last._1 // take last degree
val ddMap = degreeDistribution.toMap
val ddHist = (for (i <- 0 to ddLast) yield { if (ddMap.contains(i)) (i, ddMap(i)) else (i, 0.0) }).toArray

case class DegreeDistribution(degree: Int, distribution: Double)
val ddDF = sc.parallelize(ddHist.map { case (degree, distribution) => DegreeDistribution(degree, distribution)}).toDF()
ddDF.registerTempTable("degree_distribution_table")
display(sqlContext.sql("select * from degree_distribution_table"))

// COMMAND ----------


