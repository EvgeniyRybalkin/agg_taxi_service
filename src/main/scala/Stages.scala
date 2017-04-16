import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, Minutes}
import org.joda.time.format.DateTimeFormat


/**
  * Created by Evgeniy on 15.04.2017.
  */
object Parse {
  //==========================
  //==== Parse Directory =====
  //==========================
  def getDateTime(time: Long): Long = {
    var date = new DateTime(time * 1000L)
    val patternCDR = DateTimeFormat.forPattern("yyyyMMddHHmmss")
    date.toDateTime.toString(patternCDR).toLong
  }

  def getDimHostJarus(jarusRDD: RDD[String]): RDD[DimHostJarus] = {
    val jarusHost = jarusRDD.map(e =>
      DimHostJarus(e.split(";")(0),
        e.split(";")(1)))
    jarusHost
  }

  def getIPs(ipsRDD: RDD[String]): RDD[IPS] = {
    val ips = ipsRDD.map(e =>
      IPS(e.split("\t")(0),
        e.split("\t")(1)))
    ips
  }

  def getPhones(phonesRDD: RDD[String]): RDD[Phones] = {
    val phones = phonesRDD.map(e =>
      Phones(e.split("\t")(0),
        e.split("\t")(1),
        e.split("\t")(2)))
    phones
  }

  def getRegion(regionRDD: RDD[String]): RDD[RegionBS] = {
    val region_bs = regionRDD.map(e =>
      RegionBS(e.split(",")(0) + ":" + e.split(",")(1),
        e.split(",")(2)))
    region_bs
  }

  //===========================
  //==== Parse Input Files ====
  //===========================
  def getJarusHTTP(jarusHTTP: RDD[String]): RDD[JarusHTTP] = {
    val jarusHTTP_RDD = jarusHTTP.map(e =>
      JarusHTTP(getDateTime(e.split("\t")(0).toLong),
        e.split("\t")(2),
        e.split("\t")(6) + ":" + e.split("\t")(7),
        e.split("\t")(9),
        e.split("\t")(10)))
    jarusHTTP_RDD
  }

  def getJarusSSL(jarusSSL: RDD[String]): RDD[JarusSSL] = {
    val jarusSSL_RDD = jarusSSL.map(e =>
      JarusSSL(getDateTime(e.split("\t")(0).toLong),
        e.split("\t")(2),
        e.split("\t")(6) + ":" + e.split("\t")(7),
        e.split("\t")(9),
        e.split("\t")(10)))
    jarusSSL_RDD
  }

  def getCDR(cdrRDD: RDD[String]): RDD[CDR] = {
    val cdr = cdrRDD.map(e =>
      CDR(e.split("\\|")(2).toLong,
        e.split("\\|")(6).toInt,
        e.split("\\|")(8).toDouble,
        e.split("\\|")(9),
        e.split("\\|")(10),
        e.split("\\|")(32),
        e.split("\\|")(26) + ":" + e.split("\\|")(23)))
    cdr
  }

  //============================
}

object Stage1 {
  //============================
  //===== Agg Taxi Service =====
  //============================
  // разделитель lacCell ":"
  private def getDateTime(time: Long): Long = {
    var date = new DateTime(time * 1000L)
    val patternCDR = DateTimeFormat.forPattern("yyyyMMddHHmmss")
    date.toDateTime.toString(patternCDR).toLong
  }

  private def getCountTrip(dates: Iterable[Long]): Int = {
    val d = dates.toArray.sortWith(_ < _)
    var countTrips = 0

    for (i <- 1 to d.length - 1) {
      if (Minutes.minutesBetween(new DateTime(d(i)), new DateTime(d(i - 1))).getMinutes <= 60) {
        countTrips += 1
      }
    }
    countTrips
  }

  private def getJarus(jHTTP: RDD[JarusHTTP], jSSL: RDD[JarusSSL], dHJ: RDD[DimHostJarus], ips: RDD[IPS], rBS: RDD[RegionBS], phones: RDD[Phones]): RDD[TS] = {
    // фильтрация по времени
    val jHTTP1 = jHTTP.filter(e => (e.dateTime > 20170101235959L && e.dateTime < 20170109000000L))
      .map(e =>
        TS(e.dateTime,
          e.urlAddress,
          1,
          e.phoneNumber,
          e.lacCell,
          0))

    val jSSL1 = jSSL.filter(e => (e.dateTime > 20170101235959L && e.dateTime < 20170109000000L))
      .map(e =>
        TS(e.dateTime,
          e.ipAddress,
          1,
          e.phoneNumber,
          e.lacCell,
          0))

    // присоединение service_name
    val snHTTP = dHJ.map(e => (e.urlJarus, e.serviceNJ))
    val jHTTP2 = jHTTP1.map(e => (e.taxiServiceName, e))
      .leftOuterJoin(snHTTP)
      .map(e =>
        TS(e._2._1.dateTaxi,
          e._2._2.getOrElse("-99"),
          e._2._1.serviceType,
          e._2._1.regionDim,
          e._2._1.regionBS,
          e._2._1.countTaxiTrips))

    val snSSL = ips.map(e => (e.ipAdrress, e.serviceNameIP))
    val jSSL2 = jSSL1.map(e => (e.taxiServiceName, e))
      .leftOuterJoin(snSSL)
      .map(e =>
        TS(e._2._1.dateTaxi,
          e._2._2.getOrElse("-99"),
          e._2._1.serviceType,
          e._2._1.regionDim,
          e._2._1.regionBS,
          e._2._1.countTaxiTrips))

    val jarus1 = jHTTP2.union(jSSL2).map(e => (e.regionBS, e))

    // присоединение region_BS
    val regBS = rBS.map(e => (e.lacCell, e.regionBS))
    val jarus2 = jarus1.leftOuterJoin(regBS)
      .map(e =>
        TS(e._2._1.dateTaxi,
          e._2._1.taxiServiceName,
          e._2._1.serviceType,
          e._2._1.regionDim,
          e._2._2.getOrElse("-99"),
          e._2._1.countTaxiTrips))

    // присоединение region_DIM
    val regDIM = phones.map(e => (e.phoneNumber, e.regionPhone))
    val jarus3 = jarus2.map(e => (e.regionDim, e))
      .leftOuterJoin(regDIM)
      .map(e =>
        TS(e._2._1.dateTaxi,
          e._2._1.taxiServiceName,
          e._2._1.serviceType,
          e._2._2.getOrElse("-99"),
          e._2._1.regionBS,
          e._2._1.countTaxiTrips))

    val jarus = jarus3.map(e =>
      (e.taxiServiceName + "\t" +
        e.serviceType.toString + "\t" +
        e.regionDim + "\t" +
        e.regionBS, e.dateTaxi))
      .groupByKey()
      .map(e =>
        TS(e._2.min,
          e._1.split("\t")(0),
          e._1.split("\t")(1).toInt,
          e._1.split("\t")(2),
          e._1.split("\t")(3),
          getCountTrip(e._2)))
    jarus
  }

  private def getCDR(cdrRDD: RDD[CDR], phones: RDD[Phones], rBS: RDD[RegionBS]): RDD[TS] = {
    val regionDIM = phones.map(e => (e.phoneNumber, e.regionPhone))
    val regionBS = rBS.map(e => (e.lacCell, e.regionBS))
    val serviceName = phones.map(e => (e.phoneNumber, e.serviceNamePhone))

    val cdr = cdrRDD.filter(e => (e.callDuration > 5.0 && (e.dateTimeTranz > 20170101235959L && e.dateTimeTranz < 20170109000000L)))
      .filter(e => e.basicServiceType == "V")
      .map(e =>
        TS(e.dateTimeTranz, {
          if (e.inOrOut == 1) e.inCall else e.outCall
        },
          0, {
            if (e.inOrOut == 1) e.inCall else e.outCall
          },
          e.lacCell,
          0))
      .map(e => (e.regionBS, e))

    // присоединение regionBS
    val cdr1 = cdr.leftOuterJoin(regionBS)
      .map(e =>
        TS(e._2._1.dateTaxi,
          e._2._1.taxiServiceName,
          e._2._1.serviceType,
          e._2._1.regionDim,
          e._2._2.getOrElse("-99"),
          e._2._1.countTaxiTrips))
      .map(e => (e.regionDim, e))

    // Присоединение regionDIM
    val cdr2 = cdr1.leftOuterJoin(regionDIM)
      .map(e => TS(e._2._1.dateTaxi,
        e._2._1.taxiServiceName,
        e._2._1.serviceType,
        e._2._2.getOrElse("-99"),
        e._2._1.regionBS,
        e._2._1.countTaxiTrips))
      .map(e => (e.taxiServiceName, e))

    // присоединение serviceName
    val cdr3 = cdr2.leftOuterJoin(serviceName)
      .map(e => TS(e._2._1.dateTaxi,
        e._2._2.getOrElse("-99"),
        e._2._1.serviceType,
        e._2._1.regionDim,
        e._2._1.regionBS,
        e._2._1.countTaxiTrips))

    // группировка для подсчета поездок
    val cdr4 = cdr3.map(e =>
      (e.taxiServiceName + "\t" +
        e.serviceType.toString + "\t" +
        e.regionDim + "\t" +
        e.regionBS, e.dateTaxi)).groupByKey()

    // подсчет поездок
    val cdr5 = cdr4.map(e => TS(e._2.min,
      e._1.split("\t")(0),
      e._1.split("\t")(1).toInt,
      e._1.split("\t")(2),
      e._1.split("\t")(3),
      getCountTrip(e._2)))
    cdr5


  }

  def getATS(jHTTP: RDD[JarusHTTP], jSSL: RDD[JarusSSL], cdrRDD: RDD[CDR], dHJ: RDD[DimHostJarus], ips: RDD[IPS], rBS: RDD[RegionBS], phones: RDD[Phones]) : RDD[String]={
    val jAll = Stage1.getJarus(jHTTP, jSSL, dHJ, ips, rBS, phones)
    val cdrAll = Stage1.getCDR(cdrRDD, phones, rBS)
    val ats = jAll.union(cdrAll).sortBy(e => e.dateTaxi).map(e => e.toString)
    ats
  }

}
