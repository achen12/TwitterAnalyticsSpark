/**
  * Created by ac on 6/30/16.
  */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.spark.streaming.dstream
import org.apache.spark.rdd.{PartitionerAwareUnionRDD, RDD, UnionRDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.Duration
import org.apache.spark.mllib.linalg.{DenseVector, Vectors, Vector}
import org.apache.spark.mllib.linalg._


import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

// Based off of WindowedDstream
// Provides a Dstream to generate RecurrentVector from a keyvector pair Dstream.
// This is to generate a sub time series of data for ML lib to take in.


class RecurrentVectorDStream[K : ClassTag ](
                                    parent: DStream[(K,org.apache.spark.mllib.linalg.Vector)],
                                    _windowDuration: Duration,
                                    _slideDuration: Duration)
  extends DStream[(K,org.apache.spark.mllib.linalg.Vector)](parent.ssc) {

  if (!_windowDuration.isMultipleOf(parent.slideDuration)) {
    throw new Exception("The window duration of windowed DStream (" + _windowDuration + ") " +
      "must be a multiple of the slide duration of parent DStream (" + parent.slideDuration + ")")
  }

  if (!_slideDuration.isMultipleOf(parent.slideDuration)) {
    throw new Exception("The slide duration of windowed DStream (" + _slideDuration + ") " +
      "must be a multiple of the slide duration of parent DStream (" + parent.slideDuration + ")")
  }

  // Persist parent level by default, as those RDDs are going to be obviously reused.
  parent.persist(StorageLevel.MEMORY_ONLY_SER)

  def windowDuration: Duration = _windowDuration

  override def dependencies: List[DStream[_]] = List(parent)

  override def slideDuration: Duration = _slideDuration

  override def parentRememberDuration: Duration = rememberDuration + windowDuration

  override def persist(level: StorageLevel): DStream[(K,org.apache.spark.mllib.linalg.Vector)] = {
    // Do not let this windowed DStream be persisted as windowed (union-ed) RDDs share underlying
    // RDDs and persisting the windowed RDDs would store numerous copies of the underlying data.
    // Instead control the persistence of the parent DStream.
    parent.persist(level)
    this
  }


  //Key Work:
  override def compute(validTime: Time): Some[RDD[(K, org.apache.spark.mllib.linalg.Vector)]] = {
    //TODO handle Empty Set problem.
    val currentWindow = new Interval(validTime - windowDuration + parent.slideDuration, validTime)
    val rddsInWindow : Array[RDD[(K,Vector)]] = parent.slice(currentWindow).toArray
    val m = rddsInWindow.length
    if(rddsInWindow.last.isEmpty()){ //Last window is empty Clutch Mechanism.
      return Some(ssc.sc.emptyRDD[(K, org.apache.spark.mllib.linalg.Vector)])
    }

    //Allow serailized opertions



    //FullOuter Joins
    val rdd : RDD[(K,Vector)] = rddsInWindow.map(_.mapValues(_.toArray)).reduceLeft[RDD[(K,Array[Double])]]({
      (rdd1 : RDD[(K,Array[Double])], rdd2) => {
        if (!rdd1.isEmpty  && !rdd2.isEmpty) {
          val l1 = rdd1.first()._2.length
          val l2 = rdd2.first()._2.length
          val rdd3 = rdd1.fullOuterJoin(rdd2).mapValues[Array[Double]]({
            case (Some(x), Some(y)) => x ++ y
            case (Some(x), None) => x ++ Array.fill[Double](l2)(0.0)
            case (None, Some(y)) => Array.fill[Double](l1)(0.0) ++ y
          })
          rdd3
        }
        else if(rdd2.isEmpty){
          rdd1
        }
        else rdd2
      }
    }).mapValues(Vectors.dense(_))



    /**val n = rddsInWindow.last.first()._2.size
    val windowKeys : RDD[K] = rddsInWindow.map(rdd => rdd.keys).reduce((rdd1,rdd2) => rdd1.union(rdd2)).distinct()
    val windowKeyPair : RDD[(K,Vector)]= windowKeys.map[(K,org.apache.spark.mllib.linalg.Vector)](k =>
      (k,
        rddsInWindow.map[Vector,Seq[Vector]]( (rdd : RDD[(K,Vector)]) => if(rdd.lookup(k).isEmpty) Vectors.zeros(n) else rdd.lookup(k).head)
        .foldRight[Vector](Vectors.zeros(0))((v1:Vector,v2:Vector) => Vectors.dense((v1.toArray ++ v2.toArray)))
        )
    )**/
    return Some(rdd)
  }
}




// Based off of UnionDStream
// Provides a Dstream of the vector formed by a collection of numeric Dstreams.
// Will assume default of zero for all sparse null results.


import org.apache.spark.streaming.{Duration, Time}
import org.apache.spark.rdd.RDD

class VectorDStream[K: ClassTag](parents: Array[DStream[(K,Double)]])
  extends DStream[(K,Vector)](parents.head.ssc) {

  require(parents.length > 0, "List of DStreams to union is empty")
  require(parents.map(_.ssc).distinct.size == 1, "Some of the DStreams have different contexts")
  require(parents.map(_.slideDuration).distinct.size == 1,
    "Some of the DStreams have different slide durations")

  override def dependencies: List[DStream[_]] = parents.toList

  override def slideDuration: Duration = parents.head.slideDuration

  override def compute(validTime: Time): Option[RDD[(K,Vector)]] = {
    val currSlide = parents.map(_.getOrCompute(validTime))
    val rdds : Array[RDD[(K,Array[Double])]]= currSlide.map({
      case Some(x) =>
        x.mapValues(Array[Double](_))
      case None =>
        ssc.sc.emptyRDD[(K,Array[Double])]
    }) //Everything is in Array now

    //FullOuter Joins
    val rdd : RDD[(K,Vector)] = rdds.reduceLeft[RDD[(K,Array[Double])]]({
      (rdd1, rdd2) => {
        if (!rdd1.isEmpty && !rdd2.isEmpty) {
          val l1 = rdd1.first()._2.length
          val l2 = rdd2.first()._2.length
          val rdd3 = rdd1.fullOuterJoin(rdd2).mapValues[Array[Double]]({
            case (Some(x), Some(y)) => x ++ y
            case (Some(x), None) => x ++ Array.fill[Double](l2)(0.0)
            case (None, Some(y)) => Array.fill[Double](l1)(0.0) ++ y
          })
          rdd3
        }
        else if (rdd2.isEmpty)
          rdd1
        else
          rdd2
      }
    }).mapValues(Vectors.dense(_))

    /**
      * val resultrdd  = new ArrayBuffer[RDD[(K,Vector)]]()
      * val currSlideBroadcast = ssc.sc.broadcast(currSlide)

      * val rdds = keys.map( k=>{
      * val cslide = currSlideBroadcast.value
      * (k, Vectors.dense(
      * cslide.map({
      * case Some(x) => x.lookup(k).head
      * case None => 0.0 //Default value of zero.
      * })))
      * }
      * )
      **/

    return Some(rdd)
  }
}
