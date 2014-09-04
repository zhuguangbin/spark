package org.apache.spark.graphx.lib

import org.jblas.DoubleMatrix

import scala.util.Random
import org.apache.spark.rdd._
import org.apache.spark.graphx._

/**
 * Created by darlwen on 14-8-23.
 */
object LDA {

  /** Configuration Parameters for LDA */
  class Conf (
    var topicNum : Int,
    var vocSize  : Int,
    var alpha    : Double,
    var beta     : Double,
    var maxIter  : Int)
  extends Serializable

  def run(edges: RDD[Edge[Int]], conf: Conf) : (Graph[DoubleMatrix, DoubleMatrix]) = {
      //Generate default vertex attribute
      def defaultF(topicNum: Int): DoubleMatrix = {
        val v1 = new DoubleMatrix(topicNum)
        for(i <- 0 until topicNum) {
          v1.put(i, 0.0)
        }
        v1
      }

      //get a topic from multinomial distribution
      def getRandFromMultinomial(topicDist: Array[Double]): Int = {
        val rand = Random.nextDouble()
        val s = doubleArrayOps(topicDist).sum
        val arrNormalized = doubleArrayOps(topicDist).map{ e => e / s}
        var localSum = 0.0
        val cumArr = doubleArrayOps(arrNormalized).map{ dist =>
            localSum = localSum + dist
            localSum
        }
        doubleArrayOps(cumArr).indexWhere(cumDist => cumDist >= rand)
      }

      def initMapFunc(conf: Conf)
          (et: EdgeTriplet[DoubleMatrix, Int])
      : DoubleMatrix = {
        val tokenTopic = new DoubleMatrix(et.attr)
        for(i <- 0 until et.attr) {
          tokenTopic.put(i, Random.nextInt(conf.topicNum).toDouble)
        }
        tokenTopic
      }

      def vertexUpdateFunc(conf: Conf)
          (et: EdgeTriplet[DoubleMatrix, DoubleMatrix])
      : Iterator[(VertexId, DoubleMatrix)] = {
        val doc = new DoubleMatrix(conf.topicNum)
        val word = new DoubleMatrix(conf.topicNum)
        for (i <- 0 until et.attr.length) {
          val t = et.attr.get(i).toInt
          doc.put(t, doc.get(t) + 1)
          word.put(t, word.get(t) + 1)
        }
        Iterator((et.srcId, doc), (et.dstId, word))
      }

      def gibbsSample(conf: Conf, topicWord: DoubleMatrix)
          (et: EdgeTriplet[DoubleMatrix, DoubleMatrix])
      : DoubleMatrix = {
        val (doc, word) = (et.srcAttr, et.dstAttr)
        val tokenTopic = new DoubleMatrix(et.attr.length)
        for ( i <- 0 until et.attr.length) {
          val t = et.attr.get(i).toInt
          doc.put(t, doc.get(t) - 1)
          word.put(t, word.get(t) - 1)
          topicWord.put(t, topicWord.get(t) - 1)
          val topicDist = new Array[Double](conf.topicNum)
          for (k <- 0 until conf.topicNum) {
            topicDist(k) = (doc.get(t) + conf.alpha) * (word.get(t) + conf.beta) / (topicWord.get(t) + conf.vocSize * conf.beta)
          }
          val newTopic = getRandFromMultinomial(topicDist)
          tokenTopic.put(i, newTopic)
          doc.put(t, doc.get(t) + 1)
          word.put(t, word.get(t) + 1)
          topicWord.put(t, topicWord.get(t) + 1)
        }
        tokenTopic
      }

      def computePerplexity(conf: Conf, topicWord: DoubleMatrix)
          (et: EdgeTriplet[DoubleMatrix, DoubleMatrix])
      : Iterator[(VertexId, (Double, Double))] = {
        val (doc, word) = (et.srcAttr, et.dstAttr)
        val docDis = new DoubleMatrix(conf.topicNum)
        val wordDis = new DoubleMatrix(conf.topicNum)
        val nm = doc.sum()
        var sum = 0.0
        for ( i <- 0 until conf.topicNum ) {
          wordDis.put(i, (word.get(i) + conf.beta) / (topicWord.get(i) + conf.beta * conf.vocSize))
          docDis.put(i, (doc.get(i) + conf.alpha) / (nm + conf.topicNum * conf.alpha))
          sum += wordDis.get(i) * docDis.get(i)
        }
        val res = math.log(sum)
        Iterator((et.srcId, (res, nm)))
      }

      def computeDis(conf: Conf, topicWord: DoubleMatrix)
                    (et: EdgeTriplet[DoubleMatrix, DoubleMatrix])
      : Iterator[(VertexId, DoubleMatrix)] = {
        val (doc, word) = (et.srcAttr, et.dstAttr)
        val docDis = new DoubleMatrix(conf.topicNum)
        val wordDis = new DoubleMatrix(conf.topicNum)
        val nm = doc.sum()
        for ( i <- 0 until conf.topicNum ) {
          wordDis.put(i, (word.get(i) + conf.beta) / (topicWord.get(i) + conf.beta * conf.vocSize))
          docDis.put(i, (doc.get(i) + conf.alpha) / (nm + conf.topicNum * conf.alpha))
        }
        Iterator((et.srcId, docDis), (et.dstId, wordDis))
      }


      var oldG: Graph[DoubleMatrix, DoubleMatrix] = null

      edges.cache()
      val g = Graph.fromEdges(edges, defaultF(conf.topicNum)).cache()

      //initialize topic for each token
      var newG: Graph[DoubleMatrix, DoubleMatrix] = g.mapTriplets(initMapFunc(conf)_)
      newG.cache()
      newG.vertices.count()
      newG.edges.count()
      newG.triplets.count()

      for ( i <- 0 until conf.maxIter) {
        oldG = newG
        //update vertex attr, N*T, M*T based on the given topic
        val t0 = newG.mapReduceTriplets(
          vertexUpdateFunc(conf),
          (g1: DoubleMatrix, g2: DoubleMatrix) => g1.addColumnVector(g2)
        )

        newG = newG.outerJoinVertices(t0) {
          (vid: VertexId, vd: DoubleMatrix,
           msg: Option[DoubleMatrix]) => vd.addColumnVector(msg.get)
        }
        newG.cache()
        newG.vertices.count()
        newG.edges.count()
        newG.triplets.count()
        oldG.unpersistVertices(blocking = false)
        oldG.edges.unpersist(blocking = false)
        oldG = newG

        //compute total word number for each topic
        val topicWord = newG.vertices.filter{ case (vid, vd) => vid % 2 == 1 }.map{ case (vid, vd) => vd
        }.reduce((a: DoubleMatrix, b: DoubleMatrix) => a.addColumnVector(b))

        //compute perplexity
        val perplexityG = newG.mapReduceTriplets(
          computePerplexity(conf, topicWord),
          (g1: (Double, Double), g2: (Double, Double)) => (g1._1 + g2._1, g1._2 + g2._2)
        )
        val perplexity_numerator = perplexityG.map{ case (vid, (res, nm)) =>
          if (vid %2 == 0) res else 0.0}.reduce(_ + _)
        val perplexity_denominator = perplexityG.map{ case (vid, (res, nm)) =>
          if (vid %2 == 0) nm else 0.0}.reduce(_ + _)
        val perplexity = math.exp((perplexity_numerator / perplexity_denominator) * (-1.0))
        println("perplexity is: " + perplexity)

        //give new topic for each token by using gibbsSampling
        newG = newG.mapTriplets(gibbsSample(conf, topicWord)_)
        newG.cache()
        newG.vertices.count()
        newG.edges.count()
        newG.triplets.count()
        oldG.unpersistVertices(blocking = false)
        oldG.edges.unpersist(blocking = false)
        oldG = newG

        //update N*T, M*T
        val t1 = newG.mapReduceTriplets(
          vertexUpdateFunc(conf),
          (g1: DoubleMatrix, g2: DoubleMatrix) => g1.addColumnVector(g2)
        )

        newG = newG.outerJoinVertices(t0) {
          (vid: VertexId, vd: DoubleMatrix,
           msg: Option[DoubleMatrix]) => vd.subColumnVector(msg.get)
        }

        newG = newG.outerJoinVertices(t1) {
          (vid: VertexId, vd: DoubleMatrix,
           msg: Option[DoubleMatrix]) => vd.addColumnVector(msg.get)
        }

        newG.cache()
        newG.vertices.count()
        newG.edges.count()
        newG.triplets.count()
        oldG.unpersistVertices(blocking = false)
        oldG.edges.unpersist(blocking = false)

      }

    newG
  }
}
