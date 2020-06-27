import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable
import org.apache.spark.rdd.RDD

object Graph {
  def main(args: Array[ String ]) {
      val conf = new SparkConf().setAppName("Graph")
    val sc = new SparkContext(conf)
    var graph = sc.textFile(args(0)).map(line => {val a = line.split(",")                           
    val vertex = new Array[Long](a.length - 1)
    for (i <- 1 to a.length - 1){ vertex(i-1) = a(i).toLong }
          (a(0).toLong, a(0).toLong, vertex) })

    for (j <- 1 to 5){
        val flatmapop = Flatmap(graph);
        var reducebykeyop = Reducebykey(flatmapop).join(graph.map{case(a) => (a._1, a)}).map{case(a,b) => val adjacent=b._2
        																								var connectedvertex = (a,b._1,adjacent._3)
        																								connectedvertex };
        graph=reducebykeyop
    }

    val res = graph.map(graph => (graph._2,1)).reduceByKey((a,b) => a + b).sortByKey(true,0) 
    val res1 = res.map{case (a,b) => a + " " + b}
    res1.collect().foreach(println)
    sc.stop()
  }

  def Reducebykey(Min:RDD[(Long,Long)]):RDD[(Long,Long)]={
    val mingroup = Min.reduceByKey((a, b) => { 
    var minvalue: Long =0
    if (a <= b)
      minvalue = a
      else
      minvalue = b
      minvalue })
    return mingroup
 }  

  def Flatmap(Graph:RDD[(Long, Long, Array[Long])] ): RDD[(Long,Long)] = {
    val map = Graph.flatMap{case(a,b,c) => 
            val vertex = new Array[(Long,Long)](c.length+1)
            val adjvertex: Array[Long] = c
            vertex(0) = (a,b)
            for(k <- 0 to c.length-1){
                vertex(k+1) = (adjvertex(k), b)
            }
            vertex
        }
    return map
  }
}