import org.apache.flink.streaming.api.scala._

object Flink_Scala_WordCount {
  def main(args: Array[String]): Unit = {
    //1.获取流的执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    env
      .readTextFile("input/word.txt")
      .flatMap(_.split(" "))
      .map((_,1))
      .keyBy(_._1)
      .sum(1)
      .print()

    env.execute()
  }

}
