package wc

import org.apache.log4j.LogManager
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession


object PageRankMod {

  // Creating a graph for k linear chains, each for k vertices.
  // The following function would take k as a parameter and synthesize the graph
  // and return the graph.
  def createGraph(args: Int): List[(Int, Int)] = {
    var graph: List[(Int, Int)] = List.empty
    for (vertex <- 1 to args * args) {
      if (vertex % args != 0) {
        graph = graph :+ (vertex, vertex + 1)
      }
      else {
        graph = graph :+ (vertex, 0)
      }
    }
    return graph
  }


  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path("ranks"), true)
    } catch {
      case _: Throwable => {}
    }


    val spark = SparkSession.builder()
      .master("local[*]") // for local dev remove on aws
      .appName("SparkPageRank")
      .getOrCreate()


    val k = 100
    val iters = 10
    val alpha = 0.15D
    val G = (k*k).toDouble
    val single_start = spark.sparkContext.parallelize(List((0, 0.0))).partitionBy(new HashPartitioner(1))
    val graph = spark.sparkContext.parallelize(createGraph(k)).distinct()
                     .groupByKey().partitionBy(new HashPartitioner(1)).cache()

    // Initializing PR value to 1.0/(k^2)
    var ranks = graph.mapValues(v => 1.0 / (k * k).toDouble).cache()

    ranks = ranks.union(single_start)

    var pr_init: Double = 0.0

    for (i <- 1 to iters) {
//      logger.info("++++++++++++++++++< Iteration "+ iters.toString+ ">++++++++++++++++++")
      var contribs = (graph.join(ranks).flatMap { case (_, (urls, rank)) =>
        val size = urls.size
        urls.map(p => (p, rank / size))
      }).reduceByKey(_ + _)

      if (contribs.lookup(0).isEmpty){
        // If no mass has been transferred, we would not add any page rank mass.
        pr_init = 0.0
      } else{
        pr_init = contribs.lookup(0).head / G
      }

      val mass_transfer = ranks.leftOuterJoin(contribs).filter(_._1 != 0)
        .mapValues { case (v, new_v) => new_v.getOrElse(0.0) }

      //  alpha * (1/|G|) + (1-alpha) * (mass_transferred to single dummy/|G|+v)
      ranks = mass_transfer.mapValues(v => ((alpha/G) + (1-alpha) * (pr_init + v)))
      println("start of lineage after iteration " + i)
      println(ranks.toDebugString)
      println("end of lineage after iteration " + i)

    }
//    logger.info(ranks.toDebugString)
    ranks.sortBy(_._1, true, numPartitions = 1).saveAsTextFile("ranks")
  }
}
