package twitter.server

import akka.io.IO
import scala.concurrent.duration._
import akka.pattern.ask
import scala.concurrent.Await
import scala.concurrent.Future
import akka.util.Timeout
import akka.actor._
import spray.can.Http
import spray.can.server.Stats
import spray.util._
import spray.http._
import spray.json._
import HttpMethods._
import MediaTypes._
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global
import akka.routing.RoundRobinRouter
import com.typesafe.config.ConfigFactory

case class start()
case class getStats()
case class printStats()
case class PostTweet(query: String)
case class GetTimeline(timelineType: Int, query: String)
case class TweetRequest(userRef: ActorRef, query: String)
case class TimelineRequest(userRef: ActorRef, timelineType: Int, query: String)

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

case class Tweet(userId: Int,
                 mentions: List[Int],
                 content: String,
				 timestamp: Long)

case class Timeline(userId:Int,
                    timelineType: Int,
                    tweetsList: List[Tweet])

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val tweetFormat = jsonFormat4(Tweet)
  implicit val timelineFormat = jsonFormat3(Timeline)
  implicit val userConfigFormat = jsonFormat7(UserConfig)
  implicit val twitterConfigFormat = jsonFormat7(TwitterConfig)
}

import MyJsonProtocol._

class Following {
  var count: Int = _
  var start: Int = _
}

class Followers {
  var count: Int = _
  var start: Int = _
}

class UserInfo {
  var name: String = _
  var id: Int = _
  var myFollowing = new Array[Following](7)
  var myFollowers = new Array[Followers](7)
  var home_timeline = new ListBuffer[Tweet]()
  var user_timeline = new ListBuffer[Tweet]()
  var mentions_timeline = new ListBuffer[Tweet]()
}

class TwitterStats (var Requests: Int, var Tweets: Int, var Timeline: Int) {
  var nOfRequests: Int = Requests
  var nOfTweetRequests: Int = Tweets
  var nOfTimelineRequests: Int = Timeline
  def copy = {
    new TwitterStats(nOfRequests, nOfTweetRequests, nOfTimelineRequests)
  }
}

class Tracker(StatsInterval: Int) extends Actor {
  var startTime: Long = _
  var endTime: Long = _
  var scheduler: Cancellable = _
  var duration = new FiniteDuration(StatsInterval, MILLISECONDS)
  var future: Future[TwitterStats] = _
  var prevStats: TwitterStats = new TwitterStats(0,0,0)
  var timeCount: Int = _
 
  startTime = System.currentTimeMillis
  runTracker()
  def runTracker() {
    scheduler = context.system.scheduler.scheduleOnce(
               duration,
               self,
               printStats())
  }

  def receive = {
    case printStats() =>
      if (timeCount == 0) {
        println("A: Time interval (5 seconds)")
        println("B: Total requests in this interval")
        println("C: Total tweets in this interval")
        println("D: Total timeline accesses in this interval")
        println("A	B	C	D")
      }
      timeCount += 1
      val currStats = Server.stats.copy
      println(timeCount +
        "	" + (currStats.nOfRequests - prevStats.nOfRequests) +
        "	" + (currStats.nOfTweetRequests - prevStats.nOfTweetRequests) +
        "	" + (currStats.nOfTimelineRequests - prevStats.nOfTimelineRequests))

      prevStats.nOfRequests  = currStats.nOfRequests
      prevStats.nOfTweetRequests = currStats.nOfTweetRequests
      prevStats.nOfTimelineRequests = currStats.nOfTimelineRequests
      if((System.currentTimeMillis - startTime).millis.toMinutes >= (5 minutes).toMinutes) {
        println("Shutting down the system.")
        context.system.shutdown()
      }
      runTracker()
  }
}

class TweetService(userDatabase: Array[UserInfo], userRef: ActorRef) extends Actor {
  def receive = {
    case PostTweet(query) =>
      val tweet = query.parseJson.convertTo[Tweet]
      userDatabase(tweet.userId).user_timeline.+=:(tweet)
      if(!tweet.mentions.isEmpty) {
        // Add the tweet in mentions_timeline of the mentioned users
        var itr = tweet.mentions.iterator
        while(itr.hasNext) {
          userDatabase(itr.next).mentions_timeline.+=:(tweet)
        }
      }
      for (cat <- 1 to 7) {
        val count = userDatabase(tweet.userId).myFollowers(cat-1).count
        val start = userDatabase(tweet.userId).myFollowers(cat-1).start
        if (count > 0) {
          for (i <- start to start+count) {
            userDatabase(i).home_timeline.+=:(tweet)
          }
        }
      }
      userRef ! HttpResponse(status = 200, entity = "OK")
      Server.stats.nOfRequests += 1
      Server.stats.nOfTweetRequests += 1
      context.stop(self)
  }
}

class TweetServer(userDatabase: Array[UserInfo]) extends Actor {
  def receive = {
    case TweetRequest(userRef, query) =>
      //println("Creating service for user#" + id + ". Ref: " + client)
      val service = context.system.actorOf(
                      Props(new TweetService(userDatabase, userRef)))
      service ! PostTweet(query)
  }
}

class TweetEngine(userDatabase: Array[UserInfo]) extends Actor {
  var tweetServerRef: ActorRef = _

  initialize()
  
  def initialize() {
    val serverCount = Math.max(1, (userDatabase.length/1000000).toInt)
    tweetServerRef = context.system.actorOf(
      Props(new TweetServer(userDatabase)).withRouter(RoundRobinRouter(serverCount)),
      name = "TweetServer")
  }
  
  def receive = {
    case TweetRequest(userRef, query) =>
      //println("Creating service for user#" + id + ". Ref: " + client)
      val service = context.system.actorOf(
                      Props(new TweetService(userDatabase, userRef)))
      tweetServerRef ! TweetRequest(userRef, query)
  }
}

class TimelineService(userDatabase: Array[UserInfo], userRef: ActorRef) extends Actor {
  def receive = {
    case GetTimeline(timelineType, query) =>
      var tweetsList: List[Tweet] = List[Tweet]()
      val userId = query.parseJson.convertTo[Int]
      // Timeline type: 0 - Home timeline
      //                1 - Mentions timeline
      //                2 - User timeline
      
      if (timelineType == 0) {
        tweetsList = userDatabase(userId).home_timeline.take(20).toList
      } else if (timelineType == 1) {
        tweetsList = userDatabase(userId).mentions_timeline.take(20).toList
      } else {
        tweetsList = userDatabase(userId).user_timeline.take(20).toList
      }
      userRef ! HttpResponse(status = 200,
          entity = Timeline(userId, timelineType, tweetsList).toJson.toString)
      Server.stats.nOfRequests += 1
      Server.stats.nOfTimelineRequests += 1
      context.stop(self)
  }
}

class TimelineServer(userDatabase: Array[UserInfo]) extends Actor {
  def receive = {
    case TimelineRequest(userRef, timelineType, query) =>
      //println("Creating service for user#" + id + ". Ref: " + client)
      val service = context.system.actorOf(
                      Props(new TimelineService(userDatabase, userRef)))
      service ! GetTimeline(timelineType, query)
  }
}

class TimelineEngine(userDatabase: Array[UserInfo]) extends Actor {
  var timelineServerRef: ActorRef = _

  initialize()
  
  def initialize() {
    val serverCount = Math.max(1, (userDatabase.length/1000000).toInt)
    timelineServerRef = context.system.actorOf(
      Props(new TimelineServer(userDatabase)).withRouter(RoundRobinRouter(serverCount)),
      name = "TimelineServer")
  }

  def receive = {
    case TimelineRequest(userRef, timelineType, query) =>
      //println("Creating service for user#" + id + ". Ref: " + client)
      timelineServerRef ! TimelineRequest(userRef, timelineType, query)
  }
}

class TwitterServer(userDatabase: Array[UserInfo], config: TwitterConfig) extends Actor {
  var tweetEngine: ActorRef = _
  var timelineEngine: ActorRef = _
  
  InitializeServer()
  
  def InitializeServer() {
    tweetEngine = context.system.actorOf(
          Props(new TweetEngine(userDatabase)),
          name = "TweetEngine")
    timelineEngine = context.system.actorOf(
          Props(new TimelineEngine(userDatabase)),
          name = "TimelineEngine")
  }
  
  def receive = {
    // when a new connection comes in we register ourselves as the connection handler
    case _: Http.Connected => sender ! Http.Register(self)

    case HttpRequest(POST, Uri.Path("/tweet"), _, entity: HttpEntity.NonEmpty, _) =>
      //println("Received tweet request")
      tweetEngine ! TweetRequest(sender, entity.asString)

    case HttpRequest(GET, Uri.Path("/home_timeline"), _, entity: HttpEntity.NonEmpty, _) =>
      //println("Received home timeline request")
      timelineEngine ! TimelineRequest(sender, 0, entity.asString)

    case HttpRequest(GET, Uri.Path("/mentions_timeline"), _, entity: HttpEntity.NonEmpty, _) =>
      //println("Received mentions timeline request")
      timelineEngine ! TimelineRequest(sender, 1, entity.asString)

    case HttpRequest(GET, Uri.Path("/user_timeline"), _, entity: HttpEntity.NonEmpty, _) =>
      //println("Received user timeline request")
      timelineEngine ! TimelineRequest(sender, 2, entity.asString)
 
    case unknown: HttpRequest =>
      sender ! HttpResponse(status = 404,
        entity = s"$unknown: Sorry, this request cannot be serviced.")
  }
}

object Server extends App {
  var stats: TwitterStats = new TwitterStats(0,0,0)

  val source = scala.io.Source.fromFile("config.txt")
  val config_str = source.getLines.mkString
  source.close()
  val config_json = config_str.parseJson
  val config = config_json.convertTo[TwitterConfig]
  println("No. of users: " + config.nOfUsers)
  println("Scale: " + config.scale)

  var userDatabase: Array[UserInfo] =
    new Array[UserInfo]((config.nOfUsers.toDouble * config.scale).toInt)

  println("Database size: " + userDatabase.length + " users")

  for (nodeCount <- 0 until (config.nOfUsers * config.scale).toInt) {
    userDatabase(nodeCount) = new UserInfo() 
  }

  var user_end = new Array[Int](8)

  user_end(0) = 0

  // Create followers for each user
  for (category <- 1 to 7) {
    user_end(category) = user_end(category-1) +
                         (config.users(category-1).count * config.scale).toInt
    for(nodeCount <- user_end(category-1) to user_end(category)-1) {
      for (cat <- 1 to 7) {
        userDatabase(nodeCount).myFollowers(cat-1) = new Followers()
        userDatabase(nodeCount).myFollowers(cat-1).count =
          Math.min((config.users(cat-1).count * config.scale).toInt,
              config.users(category-1).followers(cat-1))
        if (userDatabase(nodeCount).myFollowers(cat-1).count > 0) {
          val range = (config.users(cat-1).count * config.scale).toInt -
            userDatabase(nodeCount).myFollowers(cat-1).count
          if (range == 0)
            userDatabase(nodeCount).myFollowers(cat-1).start = user_end(cat-1)
          else
            userDatabase(nodeCount).myFollowers(cat-1).start =
              user_end(cat-1) + Random.nextInt(range)
        }
      }
    }
  }
  
  val serverConfig = ConfigFactory.parseString(
      """spray.can {
           server{
             pipelining-limit = 128
    	   }
      }""")
  implicit val system = ActorSystem("TwitterServerSystem", ConfigFactory.load(serverConfig))
  //println(system.logConfiguration)
  val handler = system.actorOf(Props(new TwitterServer(userDatabase, config)), name = "handler")
  IO(Http) ! Http.Bind(handler, interface = config.serverIP, port = config.serverPort)
  println("Server started!")
  system.actorOf(
          Props(new Tracker(config.statsInterval)),
          name = "tracker")
}