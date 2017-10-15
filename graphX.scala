package cn.it.pagerank

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx._
import java.io._

object graphX {


  def Hashvaofpage(title: String): VertexId = {
    title.toLowerCase.replace(" ", "").hashCode.toLong
  }

  def main(args: Array[String]) {


    
    val sparkConf = new SparkConf().setAppName("graphXpagerank")
    val sc = new SparkContext(sparkConf)
    
    val dataWex: RDD[String] = sc.textFile(args(0))
    case class Article(val id: Int, val title: String, val body: String)

    
    val articles = dataWex.map(_.split('\t')).
     filter(line => line.length > 1).
     map(line => new Article(line(0).trim.toInt, line(1).trim.toString, line(3).trim.toString))
     
    val vertices = articles.map(b => (Hashvaofpage(b.title), b.title))

     
    val chracfind = "<target>.+?<\\/target>".r

    // edges creation
    val edges: RDD[Edge[Double]] = articles.flatMap { b =>
      val originalid = Hashvaofpage(b.title)
      chracfind.findAllIn(b.body).map { link =>
        val aimId = Hashvaofpage(link.replace("<target>", "").replace("</target>", ""))
        Edge(originalid, aimId, 1.0)
      }
    }
   
   
    val grhx = Graph(vertices, edges, "").subgraph(vpred = { (v, d) => d.nonEmpty })

  
    val pagerkgraph = grhx.pageRank(0.01)

    val namePluspagerk = grhx.outerJoinVertices(pagerkgraph.vertices) {
      (v, title, rank) => (rank.getOrElse(0.0), title)
    }


    val result = namePluspagerk.vertices.top(100) {
      Ordering.by((ety: (VertexId, (Double, String))) => ety._2._1)
    }.map((x: (VertexId, (Double, String))) => (x._2._2,x._2._1))
    
//    result.foreach(t => (println(t._1 + ": " + t._2)))
    
    sc.parallelize(result).coalesce(1,true).sortBy(x => x._2,false).saveAsTextFile(args(1))

    sc.stop()
 
  }
}

