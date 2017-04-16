/**
  * Created by Evgeniy on 15.04.2017.
  */
import java.io.PrintWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TestBase extends FunSuite with BeforeAndAfterAll {
  private var sparkConf: SparkConf = _
  private var sc: SparkContext = _

  override protected def beforeAll(): Unit = {
    sparkConf = new SparkConf()
      .setAppName("spark-test-app")
      .setMaster("local")
    sc = new SparkContext(sparkConf)
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
  }

  test("spark-test") {
    val dHJ = "D:\\_AT\\sources\\dim_host_jarus.csv"
    val ips = "D:\\_AT\\sources\\ips.tsv"
    val region_bs = "D:\\_AT\\sources\\region_bs.csv"
    val region_dim = "D:\\_AT\\sources\\phones.tsv"
    val jarusHTTP = "D:\\_AT\\sources\\jarus_http.csv"
    val jarusSSL = "D:\\_AT\\sources\\jarus_ssl.csv"
    val cdr = "D:\\_AT\\sources\\cdr.csv"

    val jHTTP = Parse.getJarusHTTP(sc.textFile(jarusHTTP))
    val jSSL = Parse.getJarusSSL(sc.textFile(jarusSSL))
    val dhj = Parse.getDimHostJarus(sc.textFile(dHJ))
    val ip = Parse.getIPs(sc.textFile(ips))
    val rBS = Parse.getRegion(sc.textFile(region_bs))
    val rDIM = Parse.getPhones(sc.textFile(region_dim))
    val cdrRDD = Parse.getCDR(sc.textFile(cdr))

    //val jAll = Stage1.getJarus(jHTTP, jSSL, dhj, ip, rBS, rDIM)
    //val cdrAll = Stage1.getCDR(cdrRDD, rDIM, rBS)

    val ats = Stage1.getATS(jHTTP, jSSL, cdrRDD, dhj, ip, rBS, rDIM)

    new PrintWriter("D:\\ats.txt") {
      write(ats.collect().mkString("\n"))
      close
    }

  }

  override def afterAll(): Unit = {
    sc.stop()
  }

}
