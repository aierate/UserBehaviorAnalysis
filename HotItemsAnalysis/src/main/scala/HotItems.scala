import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction

import scala.collection.mutable.ListBuffer
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

//用户id，商品id，类别，用户行为，时间戳
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String,timestamp: Long)
//商品id，商品结束窗口，商品统计
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    //读数据,利用kafka源读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "172.16.135.101:9092")
    properties.setProperty("group.id","consumer-group")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset","latest")

    //val stream = env.readTextFile("H:\\UserBehavior.csv")
    val stream = env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(),properties))
      .map(line => {
        val linearray = line.split(",")
        //包装成用户行为类型
        UserBehavior(linearray(0).trim.toLong,linearray(1).trim.toLong,linearray(2).trim.toInt,linearray(3),linearray(4).trim.toLong)
      })

    //指定时间戳和水印
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior == "pv")
        .keyBy(_.itemId)
        .timeWindow(Time.minutes(60),Time.minutes(5))
      //复合型聚合函数
        .aggregate(new CountAgg(), new WindowResultFunction())
    //stream.print()
    //按照窗口分组
    val topItem = stream.keyBy(_.windowEnd).process(new TopNHotItems3(3))
    topItem.print()
    env.execute("Hot Items Job")
  }
}

// COUNT 统计的聚合函数实现，每出现一条记录就加一
class CountAgg extends AggregateFunction[UserBehavior,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
}

// 用于输出窗口的结果 输入，输出，key，结束时间窗口
class WindowResultFunction extends WindowFunction[Long,ItemViewCount,Long,TimeWindow]{
   override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
     out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
   }
}

class TopNHotItems3(topSize:Int) extends KeyedProcessFunction[Long,ItemViewCount,String]{
  private var itemState : ListState[ItemViewCount] = _

  //此处什么意思？？？  一种序列化的作用
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val itemsStateDesc = new ListStateDescriptor[ItemViewCount]("itemState-state",
      classOf[ItemViewCount])

    itemState = getRuntimeContext.getListState(itemsStateDesc)
  }

  override def processElement(input: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    itemState.add(input)
    context.timerService.registerEventTimeTimer(input.windowEnd + 1)
  }

  //窗口被触发时执行的操作
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //获取收到所有商品的点击量
    val allItems : ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for(item <- itemState.get()){
      allItems += item
    }
    itemState.clear()

    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    val result : StringBuffer = new StringBuffer()
    result.append("================================\n")
    result.append("时间").append(new Timestamp(timestamp-1)).append("\n")

    for(i <- sortedItems.indices){
      val currentItem : ItemViewCount = sortedItems(i)
      result.append("NO").append(i+1).append(":")
        .append(" 商品ID=").append(currentItem.itemId)
        .append(" 商品浏览量=").append(currentItem.count).append("\n")
    }
    result.append("===============================\n\n")

    Thread.sleep(1000)
    out.collect(result.toString)
  }
}