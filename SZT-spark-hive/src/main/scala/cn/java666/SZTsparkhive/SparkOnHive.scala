package cn.java666.SZTsparkhive

import org.apache.spark.sql.SparkSession

case class SparkOnHive() {
    val spark = SparkSession.builder()
        .master("local[*]")
        .appName("SparkOnHive")
        .enableHiveSupport()
        .config("spark.driver.host", "lx")
        .getOrCreate()

    import spark.sql
    import spark.implicits._

    def testSqls(): Unit = {
        val sqls =
            """
                |show databases;
                |show tables in szt;
                |use szt;
                |select * from ads_card_deal_day_top;
                |select * from ads_in_out_station_day_top;
                |select * from ads_in_station_day_top;
                |select * from ads_line_send_passengers_day_top;
                |select * from ads_out_station_day_top;
                |""".stripMargin
        val sqlArr = sqls.split(";")

        sqlArr.foreach { x =>
            if (x.trim.nonEmpty) {
                sql(x).show(10, 50)
            }
        }

        spark.stop()
    }

    def testLocal(): Unit = {
        val spark = SparkSession.builder()
            .master("local[*]")
            .appName("AppLocal2")
            .config("spark.driver.host", "lx")
            .enableHiveSupport()
            .getOrCreate()

        import spark.sql

        sql("select * from szt.ods_szt_data limit 100")
            .show(200, false)

        spark.stop()
    }
}
