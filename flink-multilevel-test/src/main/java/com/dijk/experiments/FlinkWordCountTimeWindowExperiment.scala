package com.dijk.experiments

import java.lang.reflect.Field

import com.dijk.expriments.WordCountGlobalWindowExperiment
import com.dijk.ha.MultilevelHighAvailabilityServicesFactory
import com.dijk.multilevel.PatternBasedMultilevelStateBackend
import com.dijk.sources.{WordCount, WordCountSource, WordCountWithTimeStamp, WordCountWithTimeStampSource}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.graph.StreamConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object FlinkWordCountTimeWindowExperiment {
  val outputTag = new OutputTag[String]("Wrong Count Alert");

  def main(args: Array[String]): Unit = {

    // create stream environment
    //val env = StreamExecutionEnvironment.getExecutionEnvironment

    // environment with Web UI
    val config = new Configuration()
    config.setString("rest.bind-port", "8088")
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

    //    env.disableOperatorChaining()

    //checkpoint interval of 10 second
    env.enableCheckpointing(10000);
    //env.getCheckpointConfig.setCheckpointTimeout(10000);
    //multilevel backend
    //    val patternBasedMultilevelBackend = new PatternBasedMultilevelStateBackend(
    //      new MemoryStateBackend(false),
    //      new FsStateBackend("ftp://worker@hadoop101:21/opt/software/flink-1.11.2/data/flink/checkpoints"),
    //      new FsStateBackend("file:///data/flink/checkpoints/fakeRDB"),
    //      Array[Int](0, 1, 2)
    //    )

    val patternBasedMultilevelBackend = new PatternBasedMultilevelStateBackend(
      new FsStateBackend("file:///home/ec2-user/yahoo-streaming-benchmark/flink-1.11.2/data/checkpoints/fs", true),
      new FsStateBackend("file:///home/ec2-user/yahoo-streaming-benchmark/flink-1.11.2/data/checkpoints/fs2", true),
      new FsStateBackend("hdfs://hadoop1:9000/flink/checkpoints", true),
      Array[Int](0, 1, 2)
    )
    //    val patternBasedMultilevelBackend = new PatternBasedMultilevelStateBackend(
    //      new MemoryStateBackend(false),
    //      new FsStateBackend("file:///C:/Wenzhong/GithubRepo/FlinkFsStateBackend/checkpoints"),
    //      new FsStateBackend("file:///C:/Wenzhong/GithubRepo/FlinkFsStateBackend/fakeRDB"),
    //      Array[Int](0, 1, 2)
    //    )
    //env.setStateBackend(new MemoryStateBackend(true))
    //env.setStateBackend(new FsStateBackend("hdfs://115.146.92.102:9000/flink/checkpoints"))
    //env.setStateBackend(new FsStateBackend("file:///home/ec2-user/yahoo-streaming-benchmark/flink-1.11.2/data/checkpoints/fs"))
    env.setStateBackend(patternBasedMultilevelBackend)

    // set source
    val dataStream = env.addSource(new WordCountWithTimeStampSource)
      .assignAscendingTimestamps(_.timestamp)

    // wordcount
    val wordCountDataStream = dataStream
      .keyBy(_.word)
      .timeWindow(Time.seconds(10), Time.seconds(1))
      .process(new SumCounts)

    //dataStream.print("source")
    wordCountDataStream.print("data")
    wordCountDataStream.getSideOutput(outputTag).print("ALERT")

    env.execute("WordCount Global Window Experiment")
  }
}


/**
 * key: word
 * in: (true count, current count)
 * out: (word, true count, current count)
 */
class SumCounts extends ProcessWindowFunction[WordCountWithTimeStamp, (String, Long, Long), String, TimeWindow] {

  override def process(key: String, context: Context, elements: Iterable[WordCountWithTimeStamp], out: Collector[(String, Long, Long)]): Unit = {
    var currCt = 0L
    var maxCt = 0L
    for (e <- elements) {
      currCt += 1
      maxCt = Math.max(e.count, maxCt)
    }

    val res = (key, maxCt, currCt)
    out.collect(res)
    if (maxCt != currCt) {
      context.output(WordCountGlobalWindowExperiment.outputTag, "Wrong Count Alert: " + res.toString())
    }


  }
}
