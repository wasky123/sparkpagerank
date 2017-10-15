package cn.it.pagerank

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import java.io._

object sparkpagerank {
  
   val iterations = 10


  //Hash function to generate hash value based on title
  def Hashvaofpage(title: String) = {
    title.toLowerCase.replace(" ", "").hashCode.toLong
  }

  def main(args: Array[String]) { 

    //Setting spark context
    val sparkConf = new SparkConf().setAppName("sparkpagerank")
    val sc = new SparkContext(sparkConf)
    
    //Reading the datawex data set 
    val datawex = sc.textFile(args(0).toString.trim)
    
    
    val characfind = "<target>.+?<\\/target>".r
    
    val links = datawex.flatMap { a =>
      val pp = a.split('\t')
      val orginalid = Hashvaofpage(pp(1))
      characfind.findAllIn(pp(3)).map { link =>
        val aimid = Hashvaofpage(link.replace("<target>", "").replace("</target>", ""))
        (orginalid, aimid)
      }
    }.distinct().groupByKey().cache()

    val nameorigin = datawex.map { a =>
      val pp = a.split('\t')
      val orginalid = Hashvaofpage(pp(1))
      (orginalid, pp(1))
    }.cache()



    //Set initial rank of every page to 1
    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 until iterations) {
      val contribs = links.join(ranks).flatMap ({
      	case(urls,(links,rank))=>
          links.map(dest=>(dest,rank/links.size))
      })
      ranks =contribs.reduceByKey((x,y)=>x+y).mapValues(v => 0.15+0.85*v)
    }

    val result = nameorigin.join(ranks).sortBy(x => x._2._2).take(100).reverse.map(x =>(x._2._1,x._2._2))

    
    sc.parallelize(result).coalesce(1,true).saveAsTextFile(args(1))


    sc.stop()
  }
}
