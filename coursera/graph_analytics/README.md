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
```
