## Tips and Tricks

 To **upload the source CSV files** just go to Tables > Create Table > Data Import (File) and then drop the file. After that you will get the path to the location in which the file is stored. Next step moves the file from that location to a more user-friendly location. To do that run the following code, replacing the input path with the proper one:
 
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

To **install GraphFrames** Spark Package follow the instructions described here: [Databricks > Setup GraphFrames package](http://cdn2.hubspot.net/hubfs/438089/notebooks/help/Setup_graphframes_package.html). After that just import the proper library in the notebook and start enjoying it:

```scala
import org.graphframes._

val metroCountryContinentGF = GraphFrame.fromGraphX(metroCountryContinentGraph)
```

### How to aggregate population: from metropolits to country and then to continent level

In the example mentioned there are 3 vertex types: metropolis, country, and continent. Population is given at metropolis level only. However each metro belongs to a country (is linked to), and each country to a continent (is linked to). Therefore it is feasible to obtain the total population at country level by aggregating the contribution from the metro level. Once calculated the same can be done to obtain the total population at continent level (this time feeding from the values previously calculated at country level).

To do that it is just required to rely on the Graph´s API ---aggregateMessages--- and after that on ---joinVertices--- to obtain a new graph with the attribute population properly updated, see:

```scala
val countryLevelAggregated: VertexRDD[Vertex] = metroCountryContinentGraph.aggregateMessages[Vertex](
  t => { if (t.dstAttr.entity == "country") t.sendToDst(Vertex(t.dstAttr.name, t.srcAttr.population, t.dstAttr.entity)) },
  (a, b) => Vertex(a.name, a.population + b.population, a.entity)
)
val metroCountryAggContinentGraph = metroCountryContinentGraph.joinVertices(countryLevelAggregated)((id, a, b) => b)
```

## TODO

[ ] Figure out how to display a map based on country codes (ISO 3); somehow Databricks reports the codes are not valid, but when running with PySpark everything works fine (fails when running with Scala)
