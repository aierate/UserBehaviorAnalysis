import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.collection.mutable.ListBuffer


//定义输入的样例类
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)
//定义输出的样例类
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object NetworkFlow {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val dataStream = env.readTextFile("E:\\IDEAWorkSpace\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
      .map(data =>{
        val dataArray = data.split(" ")
        //定义时间转换
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        //将时间转换为时间戳格式
        val timestamp = simpleDateFormat.parse(dataArray(3).trim).getTime
        //转化成结果样例类
        ApacheLogEvent(dataArray(0).trim,dataArray(1).trim,timestamp,dataArray(5).trim,dataArray(6).trim)
      })
      //定义事件类型
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = t.eventTime
      })
      .keyBy(_.url)
      //定义滑动窗口
      .timeWindow(Time.minutes(10),Time.seconds(5))
      //允许时间延迟
      .allowedLateness(Time.seconds(60))
      //窗口聚合函数
      .aggregate(new CountAgg(), new WindowResult())

    val processStream = dataStream
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))

    //聚合，测试用，可以不用写。
    dataStream.print("agg")

    //处理
    processStream.print("process")

    env.execute("network flow")

  }
}
//定义累加和聚合
class CountAgg() extends AggregateFunction[ApacheLogEvent,Long,Long]{
  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

  override def createAccumulator(): Long = 0L

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}
//窗口输出的结果
class WindowResult() extends WindowFunction[Long,UrlViewCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key,window.getEnd,input.iterator.next()))
  }
}


class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String]{
  //加载状态值
  lazy val urlState: MapState[String,Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String,Long]("url-state",classOf[String],classOf[Long]))

  override def processElement(i: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
    urlState.put(i.url,i.count)
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    val allUrlViews:ListBuffer[(String,Long)] = new ListBuffer[(String, Long)]
    val iter = urlState.entries().iterator()
    while(iter.hasNext){
      val entry = iter.next()
      allUrlViews += ((entry.getKey, entry.getValue))
    }

    val sortedUrlViews = allUrlViews.sortWith(_._2>_._2).take(topSize)

    //格式化输出结果
    val result:StringBuilder = new StringBuilder()
    result.append("时间：").append(new Timestamp( timestamp - 1)).append("\n")
    for(i <- sortedUrlViews.indices){
      val currentUrlView = sortedUrlViews(i)
      result.append("NO").append(i + 1).append(":")
        .append(" URL=").append(currentUrlView._1)
        .append(" 访问量=").append(currentUrlView._2).append("\n")
    }
    result.append("==================================")
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}