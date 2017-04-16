/**
  * Created by Evgeniy on 15.04.2017.
  */
case class DimHostJarus(
                         serviceNJ: String,
                         urlJarus: String
                       ) {}

case class IPS(
                serviceNameIP: String,
                ipAdrress: String
              ) {}

case class Phones(
                   regionPhone: String,
                   serviceNamePhone: String,
                   phoneNumber: String
                 ) {}

case class RegionBS(
                     lacCell: String,
                     regionBS: String
                   ) {}

case class JarusHTTP(
                      dateTime: Long,
                      phoneNumber: String,
                      lacCell: String,
                      urlAddress: String,
                      urluri: String
                    ) {}

case class JarusSSL(
                     dateTime: Long,
                     phoneNumber: String,
                     lacCell: String,
                     ipAddress: String,
                     urluri: String
                   ) {}

case class TS( // TaxiServise
                           dateTaxi: Long, //дата начала расчета(т.к расчет недельный – то дата понедельника)
                           taxiServiceName: String, // название сервиса из справочника
                           serviceType: Int, // тип сервиса 1 – онлайн/ 0 – оффлайн
                           regionDim: String, // город / регион из справочника сервисов
                           regionBS: String, //регион из справочника базовых станций
                           countTaxiTrips: Int // количество поездок по сервису

                         ) {
  override def toString: String =
    dateTaxi.toString +
      "\t" + taxiServiceName +
      "\t" + serviceType.toString +
      "\t" + regionDim.toString +
      "\t" + regionBS +
      "\t" + countTaxiTrips.toString
}

case class UserActivity(
                         dateTaxi: Long, // – дата начала расчета(т.к расчет недельный – то дата понедельника)
                         phoneNumber: String, // – номер абонента с 7ки
                         serviceTaxi: String, // – название сервиса из справочника
                         regionDim: String, // город / регион из справочника сервисов
                         regionBS: String, //– регион из справочника базовых станций
                         serviceType: Int, //– тип сервиса 1 – онлайн / 0 – оффлайн
                         taxiDriverFCT: Int, //– Является ли пользователь водителем (1 или 0)
                         countTaxiTrips: Int //– количество поездок данного пользователя в данном сервисе
                       ) {
  override def toString: String =
    dateTaxi.toString +
      "\t" + phoneNumber +
      "\t" + serviceTaxi +
      "\t" + regionDim +
      "\t" + regionBS +
      "\t" + serviceType.toString +
      "\t" + taxiDriverFCT.toString +
      "\t" + countTaxiTrips.toString
}

case class CDR(
                dateTimeTranz: Long, //2
                inOrOut: Int, // 6
                callDuration: Double, // 8
                outCall: String, // 9 кому звонили (абонент)
                inCall: String, // 10 кто звонил (сервис такси)
                basicServiceType: String, // 32
                lacCell: String // 26 + 23
              ) {}

