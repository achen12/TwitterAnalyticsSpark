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

// scalastyle:off println
package streamingtests.twitter

import org.apache.spark.streaming._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.Logging
import org.apache.spark.rdd._

/**
 * Calculates popular hashtags (topics) over sliding 10 and 60 second windows from a Twitter
 * stream. The stream is instantiated with credentials and optionally filters supplied by the
 * command line arguments.
 *
 * Run this on your local machine as
 *
 */
object StreamingExamples extends Logging{
    def setStreamingLogLevels(){
        val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
        if(!log4jInitialized){
            logInfo("Setting log level to [WARN] for streaming example.")
            Logger.getRootLogger.setLevel(Level.OFF)
        }
    }
}

object TwitterPopularTags {
  def velocityTTransformFunc(givenRdd:RDD[(String,Long)] , givenT:Time) : RDD[(String,Long)] = {
    givenRdd.map[(String,Long)]{case(x,y) => (x,givenT.milliseconds*y)}
    return givenRdd
  }
  def main(args: Array[String]) {

    var batchWindow = Seconds(2);
    var analyticWindow = batchWindow*16;
    var referenceWindow = analyticWindow*16;
    val consumerKey = TwitterCred.consumerKey
    val consumerSecret = TwitterCred.consumerSecret
    val accessToken = TwitterCred.accessToken
    val accessTokenSecret = TwitterCred.accessTokenSecret

    StreamingExamples.setStreamingLogLevels()

    val filters = Array()

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf()
        .setAppName("TwitterPopularTags")
        .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, batchWindow)
    ssc.checkpoint("./checkpoint")
    val stream = TwitterUtils.createStream(ssc, None)



    // Starts with 30 seconds windows

    //Volume
    
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#"))).map((_,1))
    
    val sumVolume  = hashTags.reduceByKeyAndWindow(_ + _, analyticWindow)
    val volume = sumVolume.map{case (topic, count) => (count, topic)}
        .transform(_.sortByKey(false))

    //Velocity
    val halfSumVolume = hashTags.reduceByKeyAndWindow(_ + _, new Duration(analyticWindow.milliseconds/2))
    val diffVelocity = halfSumVolume.join[Int](other = sumVolume).mapValues[Int]{case(a:Int,b:Int) => (2*a - b)}
    val velocity = diffVelocity.map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))
    /**
    val minVelocity = hashTags.reduceByKeyAndWindow(_-_, windowDuration= batchWindow*2, slideDuration = batchWindow)
    val windowedVelocity = minVelocity
      .mapValues(1.0*_).cache()
        .reduceByKeyAndWindow(reduceFunc = {case(x,y) => x + 0.1*y}, windowDuration = analyticWindow, slideDuration = batchWindow)
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))
      **/



  //    val hashTagsTime = stream.flatMap( status => status.getText.split(" ").filter(_.) )
                        


//Unique User

    val hashTagsUser = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")).map{x => ( x, status.getUser.getScreenName)})
    
    val usersPerHashTags = hashTagsUser.mapValues[List[String]]{ user =>  List(user)}
                                        .reduceByKeyAndWindow({case (user1,user2) => List.concat(user1,user2)}, analyticWindow)
                                        .mapValues{ userList => userList.distinct.length}
                                        .map{case (hash,count) => (count,hash)}
                                        .transform(_.sortByKey(false))
//                                        .map{case(hash,user) =>  (hash,user.split(" ").toList.distinct().count() )}
    

//MultiTag Activeness
    
    val multiHashTags = stream.map{status => status.getText.split(" ").filter(_.startsWith("#"))}

    val windowActiveness = multiHashTags.flatMap{ hashs => hashs.map(x => (x,hashs))}
      .reduceByKeyAndWindow(reduceFunc = {(x,y) => Array.concat(x,y)},analyticWindow)
      .mapValues{ hashesList => hashesList.distinct.length}
                                        .map{case (topic, count) => (count, topic)}
                                         .transform(_.sortByKey(false))


//1stDeg Count


    val refSumVolume  = hashTags.reduceByKeyAndWindow(_ + _, analyticWindow)
    val mapRDegCount = multiHashTags.flatMap{ hashs => hashs.map(x => (x,hashs))}
      .reduceByKeyAndWindow(reduceFunc = {(x,y) => Array.concat(x,y)},analyticWindow)
      .flatMapValues{ hashesList => hashesList.distinct}
      .join[Int](refSumVolume)

      .transform(rdda => {
          val hashVolMap = rdda.mapValues[Int]{case(y,x)=>x}.collectAsMap()
          rdda.mapValues[Int]{case(hash , counts) =>
              hashVolMap.apply(hash)} })
      .reduceByKeyAndWindow(_+_,analyticWindow)
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))


    /**
    val firstDegCount = multiHashTags.flatMap(x => x.map( (_,List(x))))
                                        .reduceByKey(_+_) //Compute all other hash in window
                                        .map{case (hash,otherhashs) => (hash,otherhashs.distinct())} //Unique Hashs
                                        .map{case (hash,otherhashs) => (hash, otherhashs)    }
      **/

    /**multiHashTags.transform[(String,Int)]{ (rdd:RDD[List[String]],t:Time) => rdd.map{ x => x.flatMap{ hash =>
                                                    (hash, x.flatMap{otherhash => meanVolume.compute(t).get.map{case(count,hash)=>(hash,count)}.lookup(hash)}.reduce(_+_))
                                                } } } **/




    //Percent Share

    val totalVolume = sumVolume.transform[Int]( (x:RDD[(String,Int)]) => x.map[Int](_._2)).reduce(_+_)
    val percentShareByVolume = sumVolume.transformWith[Int,(String,Float)](totalVolume, (x:RDD[(String,Int)],y:RDD[Int], t:Time)=> {
      var sum = 0
      if (!y.isEmpty()){
        sum = y.first()
      }
      x.mapValues[Float]( xi => xi.toFloat / sum)
    })
    val percentShare = percentShareByVolume.map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    //Delta Percent Share
    val percentShareRef = percentShareByVolume.reduceByKeyAndWindow((x,y) => (x+y) / 2,referenceWindow)
    val deltaPercentShare = percentShareByVolume.join[Float](percentShareRef).mapValues[Float](x => x._1 - x._2)
       .map{case (topic, count) => (count, topic)}
       .transform(_.sortByKey(false))

    //Test Environment
    deltaPercentShare.foreachRDD (rdd => {
        val topList = rdd.take(10)
        println("\n============Test test TweetShare ===============")
        topList.foreach{println(_)}
 //       topList.foreach{ case(key,value)=> println("%s %s".format(key,value)) }
    })



/**
    volume.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nTop Volume Hashtags in past 32 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    velocity.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nTop Velocity Hashtags in past 32 seconds :")
      topList.foreach{case (count, tag) => println("%s (%s Delta-Tweets)".format(tag, count))}
    })


    usersPerHashTags.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nTop Unique User Count Hashtags in past 32 seconds:")
      topList.foreach{case (count, tag) => println("%s (%s Unique Users)".format(tag, count))}
    })

    windowActiveness.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nTop Active (Chain) Hashtags in past 32 seconds:")
      topList.foreach{case (count, tag) => println("%s (%s hashes)".format(tag, count))}
    })

    mapRDegCount.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nTop 1st Deg Count Tweets in past 32 seconds:")
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })
**/
    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
