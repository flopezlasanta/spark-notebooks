## Tips and Tricks

Here are some notes I took while working on this notebook.

### How To: Upload CSVs

Just go to Tables > Create Table > Data Import (File) and then drop the file. After that you will get the path to the location in which the file is stored. Next step moves the file from that location to a more user-friendly location. To do that run the following code, replacing the input path with the proper one:
 
```scala
import java.nio.file.{Path, Paths, Files}

def moveToGraphX(str:String):Unit = {
  if (Files.exists(path)) {
   val path = Paths.get(str)
   dbutils.fs.mv(path.toString, "/FileStore/tables/graphx/" + path.getFileName)
   dbutils.fs.rm(path.getParent.toString)
  }
}

moveToGraphX("/FileStore/tables/i1kzjjyx1462604201728/country_continent.csv")
```

### How To: Install GraphFrames

Just follow the instructions described here: [Databricks > Setup GraphFrames package](http://cdn2.hubspot.net/hubfs/438089/notebooks/help/Setup_graphframes_package.html). After that just import the proper library in the notebook and start enjoying it:

```scala
import org.graphframes._

val graphFrame = GraphFrame.fromGraphX(graph)
```

*Note: At the end I decided not to use this API and instead of that relied on GraphX for everything*

### How To: Aggregate Population (metropolits > country > continent)

In the example mentioned there are 3 vertex types: metropolis, country, and continent. Population is given at metropolis level only. However each metro belongs to a country (is linked to), and each country to a continent (is linked to). Therefore it is feasible to obtain the total population at country level by aggregating the contribution from the metro level. Once calculated the same can be done to obtain the total population at continent level (this time feeding from the values previously calculated at country level).

To do that it is just required to rely on the Graph´s API `aggregateMessages` and after that on `joinVertices` to obtain a new graph with the attribute population properly updated, see:

```scala
val countriesAgg: VertexRDD[Vertex] = graph.aggregateMessages[Vertex](
  t => { if (t.dstAttr.entity == "country") t.sendToDst(Vertex(t.dstAttr.name, t.srcAttr.population, t.dstAttr.entity)) },
  (a, b) => Vertex(a.name, a.population + b.population, a.entity)
)
val graphCountriesAgg = graph.joinVertices(countriesAgg)((id, a, b) => b) // here we are making a "replacement"
```

*Disclaimer: To be honest I don´t know if this is the best way to calculate such aggregations thus in case you are aware of a better approach please make a comment, thanks!*

### How To: Visualize Countries in a Map

First I generated the ISO3 codes for each country name:

```scala
import java.util.Locale

import java.util.Locale

var countries:Map[String, String] = Map()
for (iso <- Locale.getISOCountries())
    countries ++= Map(new Locale("", iso).getDisplayCountry() -> new Locale("", iso).getISO3Country())

def iso(country:String) = countries(country)
```

Function `iso(String):String` is used later one to replace country names by country codes when building the RDD for countries.

Once the graph is built, and the population for the countries is calculated, retrieve country information and with a custom schema create a new Dataframe to be displayed:

```scala
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType,LongType}

val countryRows = mccGraphAggCC.vertices.
  filter{ case (id, Vertex(name, population, entity)) => entity == "country" }.
  map{ case (id, Vertex(name, population, entity)) => Row(name, population.toLong) }.collect()

object countrySchema {
      val countryCode = StructField("countryCode", StringType)
      val population = StructField("population", LongType)
      val struct = StructType(Array(countryCode, population))
}

val worldDF = sqlContext.createDataFrame(sc.parallelize(countryRows), countrySchema.struct)
display(worldDF)
```

## TODO

- [ ] Try graph visualization with [D3.js](https://d3js.org/), see [Spark GraphX / D3 Graph Visualization with Ripple Exchange Rates](https://docs.cloud.databricks.com/docs/latest/databricks_guide/09%20Spark%20GraphX/1%20D3%20Graph%20Visualization%20with%20Ripple%20Exchange%20Rates.html) 
