import java.util.Scanner
import java.io.File
import java.io.FileWriter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._

///JUST CHECKING AGAIN
object Trends{
    def main(args:Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("Big_Data_P3")
            .master("local")
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        import spark.implicits._   
        import spark.sqlContext.implicits._ 

       

        //FILTER DATA
        val dataNoNull = removeNull(spark).coalesce(1)
        // dataNoNull.write.option("header", true).mode("overwrite").csv("outputs/filtered/")

        //STEP 1: ANALYZE COMPETITORS 
        //TOTAL SALE ON EACH WEBSITE
        //===========================================================================
        //val totalOrdersPerWebsite = totalOrdersPerWeb(dataNoNull, spark).coalesce(1)
        //totalOrdersPerWebsite.show
      // totalOrdersPerWebsite.write.option("header", true).mode("overwrite").csv("outputs/totalOrdersPerWebsite/")


        //STEP 2: ANALYZE COMPETITORS 
        //TOTAL SALE ON EACH WEBSITE
        //===========================================================================
        //val totalSalesPerWebsite = totalSalesPerWeb(dataNoNull, spark).coalesce(1)
        //totalSalesPerWebsite.show
       // totalSalesPerWebsite.write.option("header", true).mode("overwrite").csv("outputs/totalSalesPerWebsite/")

        //STEP 3: ANALYZE MARKET 
        //TOTAL SALE PER COUNTRY
        //===========================================================================
        //Total sales per country
        // val totalSalesPerCountry = totSalesPerCount(dataNoNull, spark).coalesce(1)
        // totalSalesPerCountry.show
        // totalSalesPerCountry.write.option("header", true).mode("overwrite").csv("outputs/totalSalesPerCountry/")


        //STEP 4: ANALYZE MARKET 
        //TOTAL ORDERS PER COUNTRY
        //===========================================================================
        //Total orders per country
        // val totalOrdersPerCountry = totOrderPerCount(dataNoNull, spark).coalesce(1)
        // totalOrdersPerCountry.show
        // totalOrdersPerCountry.write.option("header", true).mode("overwrite").csv("outputs/totalOrdersPerCountry/")


        //STEP 5: ANALYZE FLOW OF EACH WEBSITE
        //HOW MUCH MONEY EACH COUNTRY SPEND ON EACH WEBSITE
        //===========================================================================
        //How popularity of websites is different per country?
        // val countryTrafficPerWeb  = countryTrafPerWeb(dataNoNull, spark).coalesce(1)
        // countryTrafficPerWeb.show
        // countryTrafficPerWeb.write.option("header", true).mode("overwrite").csv("outputs/countryTrafficPerWeb/")


        //STEP 6: ANALYZE COUNTRY
        //WHAT WEBSITE EACH COUNTRY LIKES
        //===========================================================================
        //What top 5 websites are popular in each country?
        //val websiteTrafficPerCountry = trafWebPerCount(dataNoNull, spark).coalesce(1)
        //websiteTrafficPerCountry.show
        //websiteTrafficPerCountry.write.option("header", true).mode("overwrite").csv("outputs/websiteTrafficPerCountry/")


        //STEP 7:ANALYZE CATEGORIES FLOW PER WEBSITE
        //POPULAR CATEGORIES IN EACH COUNTRY
        //===========================================================================
        //What categories are popular per website?
        // val popularCategoriesPerWebsite = popCatPerWeb(dataNoNull, spark).coalesce(1)
        // popularCategoriesPerWebsite.show
        // popularCategoriesPerWebsite.write.option("header", true).mode("overwrite").csv("outputs/popularCategoriesPerWebsite/")
        

 
        //STEP 8:ANALYZE CATEGORIES FLOW PER COUNTRY
        //POPULAR CATEGORIES IN EACH COUNTRY
        //============================================================================
        //What is the top selling category of items? Per Country?
        // val popSellingCategoryPerCountry = popCatPerCount(dataNoNull, spark).coalesce(1)
        // popSellingCategoryPerCountry.show
        // popSellingCategoryPerCountry.write.option("header", true).mode("overwrite").csv("outputs/popularSellingCategoryPerCountry/")



               
////==========================================================================================
  ///=============================================================================



        //STEP 9: ANALYZE CATEGORY PER WEBSITE
        //POPULARITY OF CATEGORY PER WEBSITE PER EACH COUNTRY
        //=============================================================================
        //How much money each country spend on a specific category on each website?
        val totalSalePerCategoryPerCountryPerWebsite = salePerCatPerCountPerWeb(dataNoNull, spark).coalesce(1)
        totalSalePerCategoryPerCountryPerWebsite.show
        totalSalePerCategoryPerCountryPerWebsite.write.option("header", true).mode("overwrite").partitionBy("product_category").csv("outputs/totalSalePerCategoryPerCountryPerWebsite/")


//========================================================================================================
  //===========================================================================================

        //STEP 10: ANALYZE FLOW PER YEAR
        //TRAFFIC OF AVERAGE SALE PER MONTH ON EACH COUNTRY
        //===============================================================================
        //What is the top selling category of items? Per month? per quarter?
        //val avgSellingPerMonthPerCountry = avgSelPerMonPerCount(dataNoNull, spark).coalesce(1)
        // avgSellingPerMonthPerCountry.show
        //avgSellingPerMonthPerCountry.write.option("header", true).mode("overwrite").csv("outputs/avgSellingPerMonthPerCountry/europe")

        //STEP 11: ANALYZE DAY
        //TRAFFIC OF AVERAGE SALE PER HOUR PER EACH COUNTRY
        //====================================================================================
        //What times have the highest traffic of sales? Per country?
        // val avgSellingPerHourPerCountry = avgSelPerHourPerCount(dataNoNull, spark).coalesce(1)
        // avgSellingPerHourPerCountry.show
        // avgSellingPerHourPerCountry.write.option("header", true).mode("overwrite").csv("outputs/avgSellingPerHourPerCountry/NorthAmerica")


        //STEP 12: ANALYZE FAILURES
        //COUNT FAILURES PER COUNTRY
        //====================================================================================
        //How many failures? Per country?
        // val failurePerCountry = failurePerCount(dataNoNull, spark).coalesce(1)
        // failurePerCountry.show
        // failurePerCountry.write.option("header", true).mode("overwrite").csv("outputs/failurePerCountry/")

      }


      //=============================================================================
      //=============================================================================
      //FUNCTIONS 
      //=============================================================================
      //=============================================================================

       //STEP 1: ANALYZE OUR COMPETITORS 
      //TOTAL ORDERS PER WEBSITE
      def totalOrdersPerWeb(dataNoNull:DataFrame, spark:SparkSession):DataFrame ={
          val df = dataNoNull.select("*")
          df.createOrReplaceTempView("df")
          val question_3 = spark.sql("SELECT ecommerce_website_name, count(*) as Total_Orders " +
            "FROM df " +
            "GROUP by ecommerce_website_name " +
            "ORDER BY Total_Orders DESC")
        return question_3
      }

      //STEP 2: ANALYZE OUR COMPETITORS 
      //TOTAL SALES PER WEBSITE
      def totalSalesPerWeb(dataNoNull:DataFrame, spark:SparkSession):DataFrame ={
          val df = dataNoNull.select("*")
          df.createOrReplaceTempView("df")
          val question_3 = spark.sql("SELECT ecommerce_website_name, round(sum(qty*price), 2) as Total_Sales " +
            "FROM df " +
            "GROUP by ecommerce_website_name " +
            "ORDER BY Total_Sales DESC")
        return question_3
      }


      //STEP 3: ANALYZE MARKET 
      //SEE HOW MUCH MONEY EACH COUNTRY SPENDS ON ONLINE SHOPPING
      //Which locations see the highest traffic of sales? per country
      def totSalesPerCount(dataNoNull:DataFrame, spark:SparkSession):DataFrame = {
            val df = dataNoNull.select("*")
            df.createOrReplaceTempView("df")
            val question_3 = spark.sql("SELECT country, round(sum(qty*price),2) as Total_Sales " +
              "FROM df " +
              "GROUP by country " +
              "ORDER BY Total_Sales DESC")
          
          return question_3
      }

      //STEP 4: ANALYZE MARKET 
      //SEE HOW MUCH MONEY EACH COUNTRY SPENDS ON ONLINE SHOPPING
      //Which locations see the highest traffic of sales? per country
      def totOrderPerCount(dataNoNull:DataFrame, spark:SparkSession):DataFrame = {
            val df = dataNoNull.select("*")
            df.createOrReplaceTempView("df")
            val question_3 = spark.sql("SELECT country, count(*) as Total_Orders " +
              "FROM df " +
              "GROUP by country " +
              "ORDER BY Total_Orders DESC")
          
          return question_3
      }


      //STEP 5: ANALYZE FLOW OF EACH WEBSITE
      //HOW MUCH MONEY EACH COUNTRY SPEND IN EACH WEBSITE
      //===========================================================================
      //how popularity of websites is different per country?
      def countryTrafPerWeb(dataNoNull:DataFrame, spark:SparkSession):DataFrame ={
          val df = dataNoNull.select("*")
          df.createOrReplaceTempView("df")
          val sumPerCountry = spark.sql("SELECT ecommerce_website_name, country, count(*) as sum " +
            "FROM df " +
            "GROUP by ecommerce_website_name, country " +
            "ORDER BY ecommerce_website_name, sum desc")

          sumPerCountry.createOrReplaceTempView("sumPerCountry")
        
          val top5CountPerWeb = spark.sql(
            "SELECT *, SUM(sum)" +
            " OVER (PARTITION BY ecommerce_website_name) AS rank " +
            " FROM sumPerCountry ")

        top5CountPerWeb.createOrReplaceTempView("top5CountPerWeb")

        val percent = spark.sql("select ecommerce_website_name, country, round((sum/rank)*100, 1) as percent" +
            " from top5CountPerWeb ")
     
          
        return percent
      }



      //STEP 6: ANALYZE COUNTRY
      //WHAT WEBSITE EACH COUNTRY LIKES
      //===========================================================================
      //What top 5 websites are popular in each country?
      def trafWebPerCount(dataNoNull:DataFrame, spark:SparkSession):DataFrame ={
          val df = dataNoNull.select("*")
          df.createOrReplaceTempView("df")
          val sumPerCountry = spark.sql("SELECT country, ecommerce_website_name, count(*) as orders " +
            "FROM df " +
            "GROUP by country, ecommerce_website_name " +
            "ORDER BY country, orders desc")

          sumPerCountry.createOrReplaceTempView("sumPerCountry")

          val top5CountPerWeb = spark.sql(
            "SELECT *, SUM(orders)" +
            " OVER (PARTITION BY country) AS total " +
            " FROM sumPerCountry ")
          
          top5CountPerWeb.createOrReplaceTempView("top5CountPerWeb")
          
          val percent = spark.sql("select country, ecommerce_website_name, round((orders/total)*100, 1) as percent" +
            " from top5CountPerWeb ")



          // sumPerCountry.createOrReplaceTempView("sumPerCountry")

          // val top5WebPerCountry = spark.sql("SELECT t.country, t.ecommerce_website_name, t.sum " +
          //   " FROM (" +
          //   "SELECT *, ROW_NUMBER()" +
          //   " OVER (PARTITION BY country order by sum desc) AS rank " +
          //   " FROM sumPerCountry ) t" +
          //   " WHERE rank <= 3")
          
          return percent
      }


      //STEP 7:ANALYZE CATEGORIES FLOW
      //POPULAR ATEGORIES IN EACH WEBSITE
      //===========================================================================
      //What categories are popular per website?
      def popCatPerWeb(dataNoNull:DataFrame, spark:SparkSession):DataFrame ={
        val df = dataNoNull.select("*")
        df.createOrReplaceTempView("df")
        val sumPerCountry = spark.sql("SELECT ecommerce_website_name, product_category, count(*) as orders " +
          "FROM df " +
          "GROUP BY ecommerce_website_name, product_category " +
          "ORDER BY ecommerce_website_name, orders desc")

        sumPerCountry.createOrReplaceTempView("sumPerCountry")

        val top5CountPerWeb = spark.sql(
            "SELECT *, SUM(orders)" +
            " OVER (PARTITION BY ecommerce_website_name) AS total " +
            " FROM sumPerCountry ")
          
        top5CountPerWeb.createOrReplaceTempView("top5CountPerWeb")
          
        val percent = spark.sql("select  ecommerce_website_name, product_category, round((orders/total)*100, 1) as percent" +
            " from top5CountPerWeb ")

          return percent
      }


      //STEP 8:ANALYZE CATEGORIES FLOW PER COUNTRY
      //POPULAR CATEGORIES IN EACH COUNTRY
      //============================================================================
      //What is the top selling category of items? Per Country?
      def popCatPerCount(dataNoNull:DataFrame, spark:SparkSession):DataFrame ={
      val df = dataNoNull.select("*")
      df.createOrReplaceTempView("df")
      val sumPerCountry = spark.sql("SELECT country, product_category, count(*) as orders " +
        "FROM df " +
        "GROUP BY country, product_category " +
        "ORDER BY country, orders desc")

        
        sumPerCountry.createOrReplaceTempView("sumPerCountry")

        val top5CountPerWeb = spark.sql(
            "SELECT *, SUM(orders)" +
            " OVER (PARTITION BY country) AS total " +
            " FROM sumPerCountry ")
          
        top5CountPerWeb.createOrReplaceTempView("top5CountPerWeb")
          
        val percent = spark.sql("select  country, product_category, round((orders/total)*100, 1) as percent" +
           " from top5CountPerWeb ")

        
        return percent
      }

      

      

        ///==================================================================================
        ///=====INCLOMPLETED ====FIXING





      //STEP 9: ANALYZE CATEGORY PER WEBSITE
      //POPULARITY OF CATEGORY PER WEBSITE PER EACH COUNTRY
      //=============================================================================
      //How much money each country spend on a specific category on each website? 
      def salePerCatPerCountPerWeb(dataNoNull:DataFrame, spark:SparkSession):DataFrame ={
        val df = dataNoNull.select("*")
        df.createOrReplaceTempView("df") 
        val sumPerCountryPerWeb = spark.sql("SELECT  product_category, ecommerce_website_name, country, round(sum(qty*price), 2) as sum " +
          "FROM df " +
          "GROUP BY product_category, ecommerce_website_name, country " +
          "ORDER BY product_category, ecommerce_website_name, sum desc")

        sumPerCountryPerWeb.createOrReplaceTempView("sumPerCountryPerWeb")

        val topSales = spark.sql(
            "SELECT *, SUM(sum)" +
            " OVER (PARTITION BY product_category, ecommerce_website_name) AS total " +
            " FROM sumPerCountryPerWeb ")
          
        topSales.createOrReplaceTempView("topSales")
          
        val percent = spark.sql("select product_category, ecommerce_website_name, country, round((sum/total)*100, 1) as percent" +
           " from topSales")

        //sumPerCountryPerWeb.createOrReplaceTempView("sumPerCountryPerWeb")
        // val top5WebPerCountryPerWeb = spark.sql("SELECT t.product_category, t.ecommerce_website_name, t.country, t.sum, rank " +
        //     " FROM (" +
        //     "SELECT *, ROW_NUMBER()" +
        //     " OVER (PARTITION BY product_category, ecommerce_website_name order by product_category, ecommerce_website_name, sum desc) AS rank " +
        //     " FROM sumPerCountryPerWeb ) t" +
        //     " WHERE rank <= 3")

        return percent
    }



    //==============================================================================================

      //STEP 10: ANALYZE FLOW PER YEAR
      //TRAFFIC OF AVERAGE SALE PER MONTH IN EACH COUNTRY
      //===============================================================================
      //What is the top selling category of items? Per month? per quarter?
      //val avgSellingPerMonthPerCountry = avgSelPerMonPerCount(dataNoNull, spark).coalesce(1)
      def avgSelPerMonPerCount(dataNoNull:DataFrame, spark:SparkSession):DataFrame ={
        val df = dataNoNull.select("*")
        df.createOrReplaceTempView("df") 

        val amerN = Seq("Mexico", "Canada")
        val asian = Seq("China", "South Korea", "Bangladesh", "Japan", "Pakistan")
        val southAm = Seq("Brazil", "Colombia", "Ecuador", "Panama", "Jamaica", "Chile")
        val europ = Seq("France", "Greece", "Germany", "Albania", "United Kingdom", "Italy" )
        val ocean = Seq("Indonesia", "Philippines", "Australia", "Bahamas")
        val afr = Seq("Egypt", "Mali", "Somalia", "Cameroon", "Togo")

        val data = spark.sql("select country, MONTH(date_format(to_timestamp(`datetime`, 'yyyy-MM-dd HH:mm'), 'yyyy-MM-dd')) as month, ROUND(AVG(qty*price),2) as avg from df  group by country, month order by country, month")
        val americanC = data.select("*").filter(col("country").isin(amerN:_*))
       // val asianC = data.select("*").filter(col("country").isin(asian:_*))
        //val southAmC = data.select("*").filter(col("country").isin(southAm:_*))
        //val europC = data.select("*").filter(col("country").isin(europ:_*))
       // val oceanC = data.select("*").filter(col("country").isin(ocean:_*))
       // val afrC = data.select("*").filter(col("country").isin(afr:_*))
        
        return  americanC
    }

    //STEP 11: ANALYZE DAY PER HOUR
    //TRAFFIC OF AVERAGE SALE PER HOUR PER EACH COUNTRY
    //====================================================================================
    //What times have the highest traffic of sales? Per country?
    def avgSelPerHourPerCount(dataNoNull:DataFrame, spark:SparkSession):DataFrame = {
        val data = dataNoNull.select("*")
        data.createOrReplaceTempView("data")
        val dataH = spark.sql("select country, HOUR(date_format(to_timestamp(`datetime`, 'yyyy-MM-dd HH:mm'), 'yyyy-MM-dd HH:mm')) as hour, ROUND(AVG(qty*price),2) as avg from data group by country, hour order by country, hour")
      
        val amerN = Seq("Mexico", "Canada")
        val asian = Seq("China", "South Korea", "Bangladesh", "Japan", "Pakistan")
        val southAm = Seq("Brazil", "Colombia", "Ecuador", "Panama", "Jamaica", "Chile")
        val europ = Seq("France", "Greece", "Germany", "Albania", "United Kingdom", "Italy" )
        val ocean = Seq("Indonesia", "Philippines", "Australia", "Bahamas")
        val afr = Seq("Egypt", "Mali", "Somalia", "Cameroon", "Togo")

        val americanC = dataH.select("*").filter(col("country").isin(amerN:_*))
        // val asianC = dataH.select("*").filter(col("country").isin(asian:_*))
         //val southAmC = dataH.select("*").filter(col("country").isin(southAm:_*))
       /// val europC = dataH.select("*").filter(col("country").isin(europ:_*))
       //val oceanC = dataH.select("*").filter(col("country").isin(ocean:_*))
       //val afrC = dataH.select("*").filter(col("country").isin(afr:_*))
        
    return americanC
    }

    
    //STEP 12: ANALYZE FAILURES
    //COUNT FAILURES PER COUNTRY
    //====================================================================================
    //How many failures? Per country?
  
    def failurePerCount(data:DataFrame, spark:SparkSession): DataFrame = {

        data.createOrReplaceTempView("data")
       
        val total_failure = spark.sql("SELECT country, count(*) total_failure " +
          "FROM data " +
          "WHERE payment_txn_success = 'N'" +
          "GROUP BY country " +
          "ORDER BY total_failure DESC")
        total_failure.createOrReplaceTempView("failure")

        val total_sales = spark.sql("SELECT country, count(*) as total_sales " +
          "FROM data " +
          "GROUP BY country")

        total_sales.createOrReplaceTempView("t_sales")
        val res = spark.sql("SELECT t1.country, ROUND(total_sales/total_failure, 2)AS failure_Rate " +
          "FROM t_sales AS t1 JOIN failure AS t2 " +
          "ON t1.country = t2.country " +
          "ORDER BY failure_Rate DESC")
       
        
        return res
     }



    //============================================================================================
  def removeNull(spark:SparkSession):DataFrame = {
        val csv_data = spark.read
            .format("csv")
            .option("inferSchema",true)
            .option("header", true)
            .load("P3Data.csv")
        spark.sparkContext.setLogLevel("OFF")
        // val nonull = csv_data.na.drop()
        csv_data.createOrReplaceTempView("csv_data")
        //REMOVE NULL THAT IS STRING
        val noStringNull = csv_data.select("*").filter(col("customer_name").cast("string") =!= "null"
        && col("product_id").cast("string") =!= "null"
        && col("product_name").cast("string") =!= "null"
        && col("product_category").cast("string") =!= "null"
        && col("payment_type").cast("string") =!= "null"
        && col("qty").cast("string") =!= "null"
        && col("price").cast("string") =!= "null"
        && col("datetime").cast("string") =!= "null"
        && col("country").cast("string") =!= "null"
        && col("city").cast("string") =!= "null"
        && col("ecommerce_website_name").cast("string") =!= "null"
        && col("payment_txn_id").cast("string") =!= "null"
        && col("payment_txn_success").cast("string") =!= "null")
        //REMOVE NULL
        val noNull = noStringNull.select("*").filter(col("customer_name").isNotNull
        && col("product_id").isNotNull
        && col("product_name").isNotNull
        && col("product_category").isNotNull
        && col("payment_type").isNotNull
        && col("qty").isNotNull
        && col("price").isNotNull
        && col("datetime").isNotNull
        && col("country").isNotNull
        && col("city").isNotNull
        && col("ecommerce_website_name").isNotNull
        && col("payment_txn_id").isNotNull
        && col("payment_txn_success").isNotNull)
        val orderId = noNull.select("city")
        //FILTERED FAILURE TO LEAVE VALUE INCUDED "-"
        val cleanPayId = noNull.select("*").filter(col("payment_txn_id").rlike("-"))
        //REMOVE NAME OF STORES FROM FAILURE REASON
        //created table with unique websites
        val websites = spark.sql("SELECT `ecommerce_website_name` FROM csv_data GROUP BY `ecommerce_website_name` ORDER BY `ecommerce_website_name` ")
        websites.createOrReplaceTempView("websites")
        //converted table of websites to list
        val rddWeb = websites.select("ecommerce_website_name").rdd.map(row => row(0))
          .collect().toList
        //filtered failuer reason by websites
        val cleanFailureReason = cleanPayId.select("*").filter(
        !col("failure_reason").isin(rddWeb:_*)
        || col("failure_reason").isNull)
        // noStringNull.createOrReplaceTempView("noStringNull")
        // val countNN = spark.sql("SELECT count(*) from noStringNull ")

        //Canceled transactions should a reason if not then drop record
        val unsuccessful_txn = cleanFailureReason.filter("payment_txn_success =  'N' and failure_reason IS NOT NULL")
        //Successful transaction don't need a reason
        val successful_txn = cleanFailureReason.filter("payment_txn_success = 'Y' and failure_reason IS NULL")
        //union both tables
        val transaction_clean = unsuccessful_txn.union(successful_txn)

        //Regular expression to match digits only ##.#### or ####
        transaction_clean.createOrReplaceTempView("data")
        val final_filter = spark.sql("SELECT * " +
          "FROM data " +
          "WHERE price REGEXP '^[0-9]+[.][0-9]*|^[0-9]+$' " +
          "ORDER BY price")
        return final_filter
    }
}