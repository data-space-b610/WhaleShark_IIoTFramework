package ksb.csle.examples.energy

//import java.util._
import java.sql._

import scala.sys.process._
import scala.util.control.Breaks._
import scala.collection.JavaConversions._

import scala.collection.JavaConversions._
import ksb.csle.common.proto.StreamPipeControlProto._
import ksb.csle.common.proto.DatasourceProto._
import ksb.csle.common.proto.WorkflowProto._
import ksb.csle.common.proto.RunnerProto._
import ksb.csle.common.utils.config.ConfigUtils

import org.apache.logging.log4j.scala.Logging
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.commons.lang.time.DateUtils
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object ExecutePython extends App with Logging {
//  Logger.getLogger("org").setLevel(Level.OFF)
//  Logger.getLogger("akka").setLevel(Level.OFF)
////
//  private[this] val bootStrapServers: String = "localhost:9092"
//  private[this] val zooKeeperConnect: String = "localhost:2181"
//  private[this] val inputDir = System.getProperty("user.dir") + "/../examples"
//  private[this] val jsonSamplePath = s"file:///$inputDir/input/jsonSample2.csv"
//  
//  val spark = SparkSession.builder()
//    .appName("Temp")
//    .master("local[*]")
//    .config("spark.sql.warehouse.dir", "target/spark-warehouse")
//    .config("spark.driver.allowMultipleContexts" , "true")
//    .config("spark.scheduler.mode", "FAIR")
//    .config("spark.sql.shuffle.partitions", 6)
//    .config("spark.executor.memory", "2g")
//    .config("spark.driver.extraJavaOptions", "-Dmaster=local[2]")
//    .config("spark.executor.extraJavaOptions", "-Dmaster=local[2]")
//    .getOrCreate()
//  
//  import spark.implicits._
  val tmp = Seq.range(54, 63) ++ Seq(1,24,52,64,67,70,73,76,79,82,85,88,91,93)
  println(tmp)
  
//  val formatter = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//  val startTime = formatter.parse("2017-06-01 00:00:00")
//  val endTime = formatter.parse("2017-06-08 00:00:00")
//  val formatter2 = new java.text.SimpleDateFormat("yyyy-MM-dd")
//  var fileList = List[String]()
//  var start = startTime
//  var end = DateUtils.addDays(start, 1)
//  fileList = fileList :+ "hdfs://csle1:9000/energy/data/raw/"
//    .concat((formatter2.format(start) + "_" + formatter2.format(end) + ".csv"))
//  while(formatter2.format(end) < formatter2.format(endTime)) {
//    start = end
//    end = DateUtils.addDays(start, 1)
//    fileList = fileList :+ 
//      (formatter2.format(start) + "_" + formatter2.format(end) + ".csv")
//  }
//  fileList.map(x => println(x))
//  println(fileList.mkString(","))
    
//  val schema = spark.read.json(jsonSamplePath).schema
//  def getKafkaReader(topic: String) = {
//    spark
//      .readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", bootStrapServers)
//      .option("zookeeper.connect", zooKeeperConnect)
//      .option("subscribe", topic)
//      .option("failOnDataLoss", false)
//      .load()
//      .select($"value" cast "string" as "json")
//      .select(from_json($"json", schema) as "data")
//      .select("data.*")
//  }
//  val kafkaReaders = Seq.range(0 , 3).map(x => getKafkaReader("topic_" + x))
//    
//  var result: DataFrame = null
//  kafkaReaders.map(x => 
//    if(result == null) result = x
//    else result = result.join(x.select("TIMESTAMP", "VALUE_STRING"), "TIMESTAMP")
//  )
//  
//  result.writeStream
//    .outputMode("append")
//    .format("console")
//    .start()
//  
//      
//  var result = df.load()
//      .select($"value" cast "string" as "json")
//      .select(from_json($"json", schema) as "data")
//      .select("data.*")
//      
//  val llist = Seq(("bob", "2015-01-13", 4), ("alice", "2015-04-23",10))
//  val left = llist.toDF("name","date","duration")
//  val right = Seq(("2015-01-13", 100),("2015-04-22", 23)).toDF("date","upload")
//  left.show()
//  right.show
//  val df = left.join(right, "date")
//  df.show
//  val right2 = Seq(("2015-01-13", 100),("2015-04-22", 23)).toDF("date","upload")
//  df.join(right2, "date").show
  
//  private[this] val inputDir = System.getProperty("user.dir") + "/../examples"
//  val columnNamePath = s"file:///$inputDir/input/topicHbase.csv"
//  val schema = spark.read.format("csv")
//          .option("sep", ",")
//          .option("header", true)
//          .load(columnNamePath)
//          .schema
//  val df = spark
//    .readStream
//    .option("sep", ",")
//    .schema(schema)
//    .csv(columnNamePath)
//    
//  val query = df.writeStream
//    .format("console")
//    .outputMode("append")
//    .start()
      
//  val table = df.collect.map(row =>
//        (row.getAs[String](0).toInt-1 -> row.getAs[String](1)))
//        
//  table.map(x => println(x))
//  
//  var result = List[Row]()
//  for(i <- 0 until 2) 
//    result = result.+:(Row.fromSeq(Seq.range(0, 1830)))
//    
//  var newSchema = Seq[StructField]()
//  for(i <- 0 until 1830) 
//    newSchema = newSchema :+ new StructField(i.toString(), IntegerType)
//  val rdd = spark.sparkContext.parallelize(result)
//  spark.createDataFrame(rdd, StructType(newSchema)).show()

//  val output = 
//    Seq("/usr/local/bin/Rscript", "/Users/injesus/Documents/KYM/2017Y/Project/Energy/RLearning/ML7,8.r").!!
//  val output = Process("Rscript /Users/injesus/Documents/KYM/2017Y/Project/Energy/RLearning/ML7,8.r").!!
//  val output = "python a.py".!!
//  val output = Process("python a.py").!!
//  println(output)
//  
//  val conf = new Configuration()
//  val fs = FileSystem.get(new java.net.URI("hdfs://localhost:9000/energy/data"), conf)
//  val status = fs.listStatus(new Path("hdfs://localhost:9000/energy/data"))
//  status.foreach(x=> println(x.getPath))

//  val fileName = "hdfs://localhost:9000/energy/data/20121204122304.csv"
//  val lastStr = fileName.split("/").last
//  println("LAST: " + lastStr)
//  val timeStr = lastStr.split("\\.").head
//  println("lastStr + " + timeStr)
//  val formatter = new java.text.SimpleDateFormat("yyyyMMddHHmmss");
//  val time = formatter.parse(timeStr)
//  println(time)
//  
//    val formatter = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val startTime = "2017-06-01 00:00:00"
//    val finishTime = "2017-06-01 00:00:59"
//    import java.time.temporal.ChronoUnit;
//    val diff = formatter.parse(finishTime).getTime -
//      formatter.parse(startTime).getTime;
//    println(diff)
    
//    println(Seq.range(1, 12))
//  val formatter = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    var startTime = "2017-06-01 00:00:00"
//    val trainPeriod = 1440 * 7 // 60m * 24h * 7d = 10days
//    val HOURS = 1000 * 60 * 60 // 1000ms * 60s * 60m = 1hour
//    val wakeupPeriod = 1 // 6hours
//    var endTime = formatter.format(DateUtils.addMinutes(
//        formatter.parse(startTime), trainPeriod)).toString()
//    println("TIME (START): " + startTime + "TIME (END) " + endTime)
//    println((formatter.parse(startTime).getYear+1900).toString)
//    println((formatter.parse(startTime).getMonth+1).toString)
//    println(formatter.parse(startTime).getDate.toString)
//    println(formatter.parse(startTime).getHours.toString)
//    println(formatter.parse(startTime).getMinutes.toString)
//    println(formatter.parse(startTime).getSeconds.toString)
//    println(formatter.parse(s"2017-6-1 0:0:0"))
      
//  val phoenixJdbcUrl = "jdbc:phoenix:localhost:2181/hbase"
//  val phoenixZkUrl = "localhost:2181"
//  
//  Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
//  val conn = DriverManager.getConnection(phoenixJdbcUrl)
//  val stmt = conn.createStatement()
//  val formatter = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.KOREA)
//    println(Calendar.getInstance().getTime())
//    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
//    println(Calendar.getInstance().getTime())
//    println(formatter.format(Calendar.getInstance().getTime()))
//    TimeZone.setDefault(TimeZone.getTimeZone("Asia/Seoul"));
//    println(Calendar.getInstance().getTime())
//
//  implicit def function2TimerTask(f: () => Unit): TimerTask = {
//    return new TimerTask {
//      def run() = f()
//    }
//  }
//  
//  def timerTask() = println("Inside timer task")
//
//  val timer = new Timer()
//  timer.schedule(function2TimerTask(timerTask), 1000L, 100L)
//
//  Thread.sleep(5000)
//
//  timer.cancel()

//  val conf = new SparkConf()
//      .setAppName("first example")
//      .setMaster("local")
//  val sc = new SparkContext(conf)
//  val sqlContext = new SQLContext(sc)
//  val jsonStr = """{"Floor2_Smartplug9_Status" : 1.0, "Floor2_Occupancy" : "0", "Floor2_EHP1_Temp_Set_Status" : "24", "Floor2_Co2_2" : 422.0, "Floor2_LED_G1_Status" : false, "Floor2_EHP2_Temp" : "28.00", "Floor2_EHP2_ON_OFF_Command" : false, "Floor2_Smartplug3_Energy" : "197.0", "Floor2_Smartplug4_Energy" : "60.7", "Floor2_Humidity_1" : 37.5, "Floor2_EHP_power" : 0.0, "Floor2_Smartplug5_Power" : "632.0", "Floor2_EHP1_Mode_Status" : 1, "G1_0" : false, "G1_70" : false, "G3_30" : false, "Floor2_Smartplug3_Status" : "1.0", "Floor2_Smartplug10_Power" : 41.0, "Floor2_EHP1_Temp_Set_Command" : "24", "Floor2_Smartplug8_Energy" : 0.0, "G1_45" : false, "Floor2_LED_G3_Status" : false, "Floor2_Smartplug10_Energy" : 297.1, "Floor2_EHP2_Swing_Command" : false, "Floor2_Smartplug7_Energy" : 45.5, "Floor2_Lux2" : 247.43, "Floor2_Smartplug3_Power" : "1.0", "Floor2_Smartplug5_Status" : "1.0", "Floor2_EHP1_Temp" : "27.50", "Floor2_Smartplug2_Power" : "18.0", "Floor2_Smartplug9_Energy" : 111.5, "Floor2_EHP1_Mode_Command" : 1, "Floor2_LED_power" : 0.0, "Floor2_Temperature_1" : 26.0, "Floor2_Smartplug2_Energy" : "109.8", "Floor2_EHP1_ON_OFF_Status" : false, "Floor2_Smartplug8_Status" : 0.0, "G2_30" : false, "G3_15" : false, "Floor2_EHP1_FanSpeed_Status" : 1, "Floor2_EHP_energy" : 628.89, "Floor2_EHP1_FanSpeed_Command" : 1, "Floor2_Smartplug2_Status" : "1.0", "G2_0" : false, "Floor2_Lux1" : 128.22, "Floor2_EHP2_Mode_Command" : 2, "G1_15" : false, "G3_70" : false, "Floor2_Smartplug6_Power" : 4.0, "G2_70" : false, "Floor2_Smartplug7_Power" : 48.0, "Floor2_EHP2_Mode_Status" : 2, "G2_85" : false, "Floor2_Smartplug8_Power" : 0.0, "Outdoor_Temperature" : 20.2, "Floor2_Smartplug5_Energy" : "503.2", "Floor2_EHP2_Temp_Set_Command" : "24.00", "Floor2_Smartplug1_Energy" : "220.00", "Floor2_Smartplug6_Energy" : 38.3, "Floor2_Temperature_2" : 26.0, "Floor2_LED_energy" : 1594.98, "Floor2_EHP2_FanSpeed_Status" : 1, "Floor2_EHP2_FanSpeed_Command" : 1, "Floor2_Smartplug10_Status" : 1.0, "G2_45" : false, "Outdoor_Irradiation" : 332.0, "Floor2_Smartplug1_Power" : "10.00", "G1_30" : false, "Floor2_LED_G2_Status" : false, "Floor2_EHP2_ON_OFF_Status" : false, "G1_85" : false, "Floor2_Smartplug9_Power" : 9.0, "Floor2_Smartplug7_Status" : 1.0, "Floor2_Humidity_2" : 37.3, "Floor2_EHP1_Swing_Command" : false, "G2_15" : false, "G1_100" : true, "Floor2_Smartplug1_Status" : "1.00", "G3_100" : true, "G3_85" : false, "Floor2_EHP2_Temp_Set_Status" : "24.00", "Floor2_EHP1_Swing_Status" : false, "TIMESTAMP" : 2017-06-03 16:33:19.0, "Floor2_Smartplug6_Status" : 1.0, "Outdoor_Humidity" : 49.0, "Floor2_Smartplug4_Power" : "12.0", "G3_45" : false, "Floor2_EHP2_Swing_Status" : false, "Floor2_Co2_1" : 468.0, "Floor2_Smartplug4_Status" : "1.0", "G3_0" : false, "Floor2_EHP1_ON_OFF_Command" : false, "G2_100" : true}"""
////  val jsonStr = """{"Floor2_Smartplug9_Status" : 1.0, "Floor2_Occupancy" : "0", "Floor2_EHP1_Temp_Set_Status" : "24", "Floor2_Co2_2" : 422.0, "Floor2_LED_G1_Status" : false, "Floor2_EHP2_Temp" : "28.00", "Floor2_EHP2_ON_OFF_Command" : false, "Floor2_Smartplug3_Energy" : "197.0", "Floor2_Smartplug4_Energy" : "60.7", "Floor2_Humidity_1" : 37.5, "Floor2_EHP_power" : 0.0, "Floor2_Smartplug5_Power" : "632.0", "Floor2_EHP1_Mode_Status" : 1, "G1_0" : false, "G1_70" : false, "G3_30" : false, "Floor2_Smartplug3_Status" : "1.0", "Floor2_Smartplug10_Power" : 41.0, "Floor2_EHP1_Temp_Set_Command" : "24", "Floor2_Smartplug8_Energy" : 0.0, "G1_45" : false, "Floor2_LED_G3_Status" : false, "Floor2_Smartplug10_Energy" : 297.1, "Floor2_EHP2_Swing_Command" : false, "Floor2_Smartplug7_Energy" : 45.5, "Floor2_Lux2" : 247.43, "Floor2_Smartplug3_Power" : "1.0", "Floor2_Smartplug5_Status" : "1.0", "Floor2_EHP1_Temp" : "27.50", "Floor2_Smartplug2_Power" : "18.0", "Floor2_Smartplug9_Energy" : 111.5, "Floor2_EHP1_Mode_Command" : 1, "Floor2_LED_power" : 0.0, "Floor2_Temperature_1" : 26.0, "Floor2_Smartplug2_Energy" : "109.8", "Floor2_EHP1_ON_OFF_Status" : false, "Floor2_Smartplug8_Status" : 0.0, "G2_30" : false, "G3_15" : false, "Floor2_EHP1_FanSpeed_Status" : 1, "Floor2_EHP_energy" : 628.89, "Floor2_EHP1_FanSpeed_Command" : 1, "Floor2_Smartplug2_Status" : "1.0", "G2_0" : false, "Floor2_Lux1" : 128.22, "Floor2_EHP2_Mode_Command" : 2, "G1_15" : false, "G3_70" : false, "Floor2_Smartplug6_Power" : 4.0, "G2_70" : false, "Floor2_Smartplug7_Power" : 48.0, "Floor2_EHP2_Mode_Status" : 2, "G2_85" : false, "Floor2_Smartplug8_Power" : 0.0, "Outdoor_Temperature" : 20.2, "Floor2_Smartplug5_Energy" : "503.2", "Floor2_EHP2_Temp_Set_Command" : "24.00", "Floor2_Smartplug1_Energy" : "220.00", "Floor2_Smartplug6_Energy" : 38.3, "Floor2_Temperature_2" : 26.0, "Floor2_LED_energy" : 1594.98, "Floor2_EHP2_FanSpeed_Status" : 1, "Floor2_EHP2_FanSpeed_Command" : 1, "Floor2_Smartplug10_Status" : 1.0, "G2_45" : false, "Outdoor_Irradiation" : 332.0, "Floor2_Smartplug1_Power" : "10.00", "G1_30" : false, "Floor2_LED_G2_Status" : false, "Floor2_EHP2_ON_OFF_Status" : false, "G1_85" : false, "Floor2_Smartplug9_Power" : 9.0, "Floor2_Smartplug7_Status" : 1.0, "Floor2_Humidity_2" : 37.3, "Floor2_EHP1_Swing_Command" : false, "G2_15" : false, "G1_100" : true, "Floor2_Smartplug1_Status" : "1.00", "G3_100" : true, "G3_85" : false, "Floor2_EHP2_Temp_Set_Status" : "24.00", "Floor2_EHP1_Swing_Status" : false, "TIMESTAMP" : 2017-06-03 16:33:19.0, "Floor2_Smartplug6_Status" : 1.0, "Outdoor_Humidity" : 49.0, "Floor2_Smartplug4_Power" : "12.0", "G3_45" : false, "Floor2_EHP2_Swing_Status" : false, "Floor2_Co2_1" : 468.0, "Floor2_Smartplug4_Status" : "1.0", "G3_0" : false, "Floor2_EHP1_ON_OFF_Command" : false, "G2_100" : true}"""
////  val jsonStr = """{"key": 84896, "value": 54 }"""
//  
//  val rdd = sc.parallelize(Seq(jsonStr))
//  val result = sqlContext.read.json(rdd)
//  result.show(false)
    
//  val rdd = src.spark.sparkContext.parallelize(Seq(jsonStr))
//    val result = src.sqlContext.read.json(rdd)
//    result.show(false)
//    
//  import spark.implicits._ // spark is your SparkSession object
//  val df = spark.read.json(Seq(jsonStr).toDS)
//  def tmp(str: String) = println(str)
//  val jsonStr = """{"Floor2_Smartplug9_Status" : 1.0, "Floor2_Occupancy" : "0", "Floor2_EHP1_Temp_Set_Status" : "24", "Floor2_Co2_2" : 422.0, "Floor2_LED_G1_Status" : false, "Floor2_EHP2_Temp" : "28.00", "Floor2_EHP2_ON_OFF_Command" : false, "Floor2_Smartplug3_Energy" : "197.0", "Floor2_Smartplug4_Energy" : "60.7", "Floor2_Humidity_1" : 37.5, "Floor2_EHP_power" : 0.0, "Floor2_Smartplug5_Power" : "632.0", "Floor2_EHP1_Mode_Status" : 1, "G1_0" : false, "G1_70" : false, "G3_30" : false, "Floor2_Smartplug3_Status" : "1.0", "Floor2_Smartplug10_Power" : 41.0, "Floor2_EHP1_Temp_Set_Command" : "24", "Floor2_Smartplug8_Energy" : 0.0, "G1_45" : false, "Floor2_LED_G3_Status" : false, "Floor2_Smartplug10_Energy" : 297.1, "Floor2_EHP2_Swing_Command" : false, "Floor2_Smartplug7_Energy" : 45.5, "Floor2_Lux2" : 247.43, "Floor2_Smartplug3_Power" : "1.0", "Floor2_Smartplug5_Status" : "1.0", "Floor2_EHP1_Temp" : "27.50", "Floor2_Smartplug2_Power" : "18.0", "Floor2_Smartplug9_Energy" : 111.5, "Floor2_EHP1_Mode_Command" : 1, "Floor2_LED_power" : 0.0, "Floor2_Temperature_1" : 26.0, "Floor2_Smartplug2_Energy" : "109.8", "Floor2_EHP1_ON_OFF_Status" : false, "Floor2_Smartplug8_Status" : 0.0, "G2_30" : false, "G3_15" : false, "Floor2_EHP1_FanSpeed_Status" : 1, "Floor2_EHP_energy" : 628.89, "Floor2_EHP1_FanSpeed_Command" : 1, "Floor2_Smartplug2_Status" : "1.0", "G2_0" : false, "Floor2_Lux1" : 128.22, "Floor2_EHP2_Mode_Command" : 2, "G1_15" : false, "G3_70" : false, "Floor2_Smartplug6_Power" : 4.0, "G2_70" : false, "Floor2_Smartplug7_Power" : 48.0, "Floor2_EHP2_Mode_Status" : 2, "G2_85" : false, "Floor2_Smartplug8_Power" : 0.0, "Outdoor_Temperature" : 20.2, "Floor2_Smartplug5_Energy" : "503.2", "Floor2_EHP2_Temp_Set_Command" : "24.00", "Floor2_Smartplug1_Energy" : "220.00", "Floor2_Smartplug6_Energy" : 38.3, "Floor2_Temperature_2" : 26.0, "Floor2_LED_energy" : 1594.98, "Floor2_EHP2_FanSpeed_Status" : 1, "Floor2_EHP2_FanSpeed_Command" : 1, "Floor2_Smartplug10_Status" : 1.0, "G2_45" : false, "Outdoor_Irradiation" : 332.0, "Floor2_Smartplug1_Power" : "10.00", "G1_30" : false, "Floor2_LED_G2_Status" : false, "Floor2_EHP2_ON_OFF_Status" : false, "G1_85" : false, "Floor2_Smartplug9_Power" : 9.0, "Floor2_Smartplug7_Status" : 1.0, "Floor2_Humidity_2" : 37.3, "Floor2_EHP1_Swing_Command" : false, "G2_15" : false, "G1_100" : true, "Floor2_Smartplug1_Status" : "1.00", "G3_100" : true, "G3_85" : false, "Floor2_EHP2_Temp_Set_Status" : "24.00", "Floor2_EHP1_Swing_Status" : false, "TIMESTAMP" : 2017-06-03 16:33:19.0, "Floor2_Smartplug6_Status" : 1.0, "Outdoor_Humidity" : 49.0, "Floor2_Smartplug4_Power" : "12.0", "G3_45" : false, "Floor2_EHP2_Swing_Status" : false, "Floor2_Co2_1" : 468.0, "Floor2_Smartplug4_Status" : "1.0", "G3_0" : false, "Floor2_EHP1_ON_OFF_Command" : false, "G2_100" : true}"""
//  tmp(jsonStr)
}
