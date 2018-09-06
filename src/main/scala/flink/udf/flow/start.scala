package flink.udf.flow

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row
import org.apache.log4j.Logger

object start {
  private val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]):Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val udfClassFullname = "flink.udf.flow.testclass"
    val udfName = "abc"
    lazy val obj = UdfRegister.getUdfObject(udfClassFullname,udfName)
    tableEnv.registerFunction(udfName, obj.asInstanceOf[ScalarFunction])

    //val text = env.socketTextStream("10.143.133.113", 9999)
    val text = env.readTextFile("D:\\hello.txt")
    val ds = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      //.timeWindow(Time.seconds(2))
      .sum(1)
    //ds.print()

    val table = tableEnv.fromDataStream(ds)
    val schema = table.getSchema

    val sql = s"""SELECT _1,$udfName(_2) FROM $table"""
    val newTable = tableEnv.sqlQuery(sql)
    val newText = tableEnv.toAppendStream[Row](newTable)
    newText.print()

    env.execute("Window Stream WordCount")
  }
}
