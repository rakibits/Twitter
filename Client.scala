package twitter.client

import akka.actor._
import akka.io.IO
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Await
import scala.concurrent.Future
import spray.can.Http
import spray.util._
import spray.http._
import spray.json._
import HttpMethods._
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global

case class tweetActivity()
case class timelineActivity()
case class profileActivity()
case class majorEvent()

case class Tweet(userId: Int,
                 mentions: List[Int],
                 content: String,
				 timestamp: Long)
				 
case class Timeline(userId:Int,
                    timelineType: Int,
                    tweetsList: List[Tweet])

case class UserConfig(category: Int,
					  count: Int,
					  followers: Array[Int],
					  following: Array[Int],
					  tweetInterval: Int,
					  timelineInterval: Int,
					  profileInterval: Int)

case class TwitterConfig(serverIP: String,
                         serverPort: Int,
                         nOfUsers: Int,
                         scale: Double,
                         majorEvent: Int,
                         statsInterval: Int,
                         users: Array[UserConfig])

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val userTweetFormat = jsonFormat4(Tweet)
  implicit val timelineFormat = jsonFormat3(Timeline)
  implicit val userConfigFormat = jsonFormat7(UserConfig)
  implicit val twitterConfigFormat = jsonFormat7(TwitterConfig)
}

import MyJsonProtocol._

class User(id: Int,
    server: String,
    myConfig: UserConfig,
    nOfUsers: Int,
    eventTime: Int)(implicit system: ActorSystem) extends Actor {
  var scheduler: Cancellable = _
  var tweetCount: Int = _
  implicit val timeout = Timeout(60 seconds)

  waitForTweetActivity(Random.nextInt(myConfig.tweetInterval))
  waitForTimelineActivity(Random.nextInt(myConfig.timelineInterval))
  waitForProfileActivity(Random.nextInt(myConfig.profileInterval))
  if (eventTime > 0) {
    waitForMajorEvent(eventTime)
  }

  def waitForTweetActivity(waitTime: Int) {
    scheduler = context.system.scheduler.scheduleOnce(
          (new FiniteDuration(waitTime, MILLISECONDS)),
          self,
          tweetActivity())
  }

  def waitForTimelineActivity(waitTime: Int) {
    scheduler = context.system.scheduler.scheduleOnce(
          (new FiniteDuration(waitTime, MILLISECONDS)),
          self,
          timelineActivity())
  }

  def waitForProfileActivity(waitTime: Int) {
    scheduler = context.system.scheduler.scheduleOnce(
          (new FiniteDuration(waitTime, MILLISECONDS)),
          self,
          profileActivity())
  }
    
  def waitForMajorEvent(waitTime: Int) {
    scheduler = context.system.scheduler.scheduleOnce(
          (new FiniteDuration(waitTime, MILLISECONDS)),
          self,
          majorEvent())
  }

  def postTweet(tweet: Tweet) {
    tweetCount += 1
    val timestamp = System.currentTimeMillis()
    val isMention = Random.nextBoolean()
    var mentions = List[Int]()
    var content: String = null
    if (tweet == null) {
      if (isMention) {
        val mention = Random.nextInt(nOfUsers)
        mentions = mentions.::(mention)
        content = "User#" + id + " mentioned user#" + mention + " and his tweet count is " + tweetCount
      } else {
        content = "User#" + id + " posted a tweet and his tweet count is " + tweetCount
      }
    } else {
      // Retweet
      content = tweet.content
    }
    val jsonTweet = new Tweet(id, mentions, content, timestamp).toJson
    val future = IO(Http).ask(HttpRequest(POST, Uri(s"http://$server/tweet")).withEntity(HttpEntity(jsonTweet.toString))).mapTo[HttpResponse]
    val response = Await.result(future, timeout.duration).asInstanceOf[HttpResponse]
  }

  def receive = {
    case majorEvent() =>
      postTweet(null)
      waitForMajorEvent(eventTime)

    case tweetActivity() =>
      postTweet(null)
      waitForTweetActivity(myConfig.tweetInterval)
      
    case profileActivity() =>
      val future = IO(Http).ask(HttpRequest(GET,
          Uri(s"http://$server/user_timeline")).withEntity(HttpEntity(id.toString))).mapTo[HttpResponse]
      val response = Await.result(future, timeout.duration).asInstanceOf[HttpResponse]
      waitForProfileActivity(myConfig.profileInterval)
        
    case timelineActivity() =>
      var pick = Random.nextInt(10)
      // Timeline type: 0 - Home timeline
      //                1 - Mentions timeline
      var timelineType = 0
      var requestUri = s"http://$server/home_timeline"
      if(pick%3 == 0) {
      	timelineType = 1
        requestUri = s"http://$server/mentions_timeline"
      }
      
      val future = IO(Http).ask(HttpRequest(GET,
          Uri(requestUri)).withEntity(HttpEntity(id.toString))).mapTo[HttpResponse]
      val response = Await.result(future, timeout.duration).asInstanceOf[HttpResponse]
      val jsonTimeline = response.entity.asString.parseJson
      val timeline = jsonTimeline.convertTo[Timeline]
      if (Random.nextBoolean && (timeline.tweetsList.length > 0)) {
	    val tweetNum = Random.nextInt(timeline.tweetsList.length)
	    val retweet = timeline.tweetsList(tweetNum)
	    postTweet(retweet)
      }
      waitForTimelineActivity(myConfig.timelineInterval)
  }
}

object Client extends App {
  val source = scala.io.Source.fromFile("config.txt")
  val config_str = source.getLines.mkString
  source.close()
  val config_json = config_str.parseJson
  val config = config_json.convertTo[TwitterConfig]

  println("No. of users: " + config.nOfUsers)
  println("Scale: " + config.scale)
  println("No. of users scaled down to: " + (config.nOfUsers.toDouble * config.scale).toInt)

  implicit val system = ActorSystem("TwitterClientSimulator")

  val server = config.serverIP + ":" + config.serverPort.toString()
    
  var user_end = new Array[Int](8)
  user_end(0) = 0
  for (category <- 1 to 7) {
    user_end(category) = user_end(category-1) +
                         (config.users(category-1).count * config.scale).toInt
      
    for(nodeCount <- user_end(category-1) to user_end(category)-1) {
      var eventTime= 0
      if (Random.nextBoolean)
        eventTime = config.majorEvent
      system.actorOf(
        Props(new User(nodeCount, server, config.users(category-1),
            (config.nOfUsers.toDouble * config.scale).toInt, eventTime)),
        name = "user" + nodeCount.toString)
    }
  }
            
  println("All users created.")
}