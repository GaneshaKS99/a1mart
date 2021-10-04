
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.io.StdIn.readLine

object A1mart {
    def main(args: Array[String]):Unit= {

        val spark = SparkSession
          .builder
          .master("local[*]")
          .appName("mart")
          .getOrCreate()

        val sc = spark.sparkContext
        sc.setLogLevel("ERROR")


        //read in CSV format
        val customerDF = spark.read.options(Map("inferSchema" -> "true")).csv("C:/Users/ganks/OneDrive - Publicis Groupe/Documents/itv001181/dataset/customers/*").withColumnRenamed("_c0", "id").withColumnRenamed("_c1", "firstname").withColumnRenamed("_c2", "lastname").withColumnRenamed("_c7", "state")
        val orderDF = spark.read.options(Map("inferSchema" -> "true")).csv("C:/Users/ganks/OneDrive - Publicis Groupe/Documents/itv001181/dataset/orders/*").withColumnRenamed("_c3", "status").withColumnRenamed("_c1", "date").withColumnRenamed("_c0", "oid").withColumnRenamed("_c2", "ocid")

        //uncomment below 2 lines to save csv as parquet

        //orderDF.write.format("parquet").mode("overwrite").save("C:/Users/ganks/dataset/orders")
        //customerDF.write.format("parquet").mode("overwrite").save("C:/Users/ganks/dataset/customers")

        //uncomment below 2 lines to Read Parquet Data
        //val orderDF = spark.read.options(Map("inferSchema" -> "true")).parquet("C:/Users/ganks/dataset/orders")
        //val customerDF = spark.read.options(Map("inferSchema" -> "true")).parquet("C:/Users/ganks/dataset/customers")


        //reading csv/parquet
        customerDF.createOrReplaceTempView("customers")
        orderDF.createOrReplaceTempView("orders")

        //customerDF.show()
        //orderDF.show()


        //val resultdf = customerDF.count()


        //----------------------------------------------------------------------------------------------------------------------------------------------------------------
        //2.a Retrieve all records for a particular customer (user input)
        //<editor-fold desc="Solution">
        {
            print("Enter your first name: ")
            val firstName = readLine()
            print("Enter your last name: ")
            val lastName = readLine()

            val result = spark.sql(s"select * from customers where firstname=='$firstName' and lastname=='$lastName' ")
            result.show()


            //using dataframe functions
            val resultDf = customerDF.select("*").where(customerDF("firstname") === s"$firstName" && customerDF("lastname") === s"$lastName")
            resultDf.show()
        }
        //</editor-fold>
        //----------------------------------------------------------------------------------------------------------------------------------------------------------------

        // 2.b List Count of orders based on status and month

        //<editor-fold desc="Solution">
        // val a=spark.sql(s"select distinct status from orders ")

        val result2 = spark.sql(s"select count(oid) as numberOfOrders,month(date),status from orders group by month(date),status order by month(date)")
        result2.show()


        //using dataframe functions

        val result2Df = orderDF.select(col("oid"), month(col("date")).as("month"), col("status")).groupBy("month").pivot("status").agg(count("oid").as("numberOfOrders")).orderBy("month")
        result2Df.show()
        //</editor-fold>


        //----------------------------------------------------------------------------------------------------------------------------------------------------------------

        // 2.c List Count of orders based on status and month for a particular customer

        //<editor-fold desc="Solution">
        print("enter the customer ID")
        val cid = readLine()
        val result3 = spark.sql(s"select count(oid) as numberOfOrders,month(date),status from orders where ocid='$cid' group by month(date),status order by month(date)")
        result3.show()

            //using dataframe functions

        val result3Df = orderDF.select(col("oid"), month(col("date")).as("month"), col("status")).where(orderDF("ocid") === s"$cid").groupBy("month").pivot("status").agg(count("oid").as("numberOfOrders")).orderBy("month")
        result3Df.show()
        //</editor-fold>


        //----------------------------------------------------------------------------------------------------------------------------------------------------------------
        //2.d List Count of orders based on status and customer


        //<editor-fold desc="Solution">

        val result4 = spark.sql(s"select count(oid) as numberOfOrders,ocid,status from orders group by ocid,status order by ocid")
        result4.show()

        //using dataframe functions

        val result4Df = orderDF.select(col("oid"), col("ocid"), col("status")).groupBy("ocid").pivot("status").agg(count("oid").as("numberOfOrders")).orderBy("ocid")
        result4Df.show()
        //</editor-fold>


        //----------------------------------------------------------------------------------------------------------------------------------------------------------------
        //2.e Find the customers who have placed orders


        //<editor-fold desc="Solution ">

        val result5 = spark.sql(s"select firstname,lastname,id from customers as c where exists (select * from orders as o where o.ocid=c.id) order by id")
        result5.show()


        //    val result5 = spark.sql(s"SELECT distinct firstname,lastname,id FROM customers as c LEFT JOIN orders as o ON o.ocid = c.id WHERE o.ocid IS not NULL order by id")
        //    result5.show()


        //using dataframe functions

        val result5Df = customerDF.join(orderDF, customerDF("id") === orderDF("ocid"), "left")
        result5Df.select(col("firstname"), col("lastname"), col("id")).where("ocid IS not NULL").orderBy("id").dropDuplicates().show()
        //</editor-fold>


        //----------------------------------------------------------------------------------------------------------------------------------------------------------------
        ////2.f Find the customers who have not placed orders

        //<editor-fold desc="Solution ">

        val result6 = spark.sql(s"select firstname,lastname,id from customers as c where not exists ( select * from orders as o where c.id=o.ocid) order by id")
        result6.show()

        //  val result6 = spark.sql(s"SELECT distinct firstname,lastname,id FROM customers LEFT OUTER JOIN orders ON orders.ocid = customers.id WHERE ocid IS NULL order by id")
        //  result6.show()


        //using dataframe functions

        val result6Df = customerDF.join(orderDF, customerDF("id") === orderDF("ocid"), "left")
        result6Df.select(col("firstname"), col("lastname"), col("id")).where("ocid IS NULL").orderBy("id").dropDuplicates().show()
        //</editor-fold>


        //----------------------------------------------------------------------------------------------------------------------------------------------------------------
        //2.g.1 Find the top 5 customers who have placed highest number of orders

        //<editor-fold desc="Solution">

        val result7 = spark.sql(s"select count(oid) as numberOfOrders,ocid from orders group by ocid order by numberOfOrders DESC limit 5 ")
        result7.show()

        //using dataframe functions

        val result7Df = orderDF.select(col("oid"), col("ocid")).groupBy("ocid").agg(count("oid").as("numberOfOrders")).orderBy(col("numberOfOrders").desc).limit(5)
        result7Df.show()
        //</editor-fold>

        //extra // Find the top 5 customers who have placed lowest number of orders

        //<editor-fold desc="Solution">

        val result7a = spark.sql(s"select count(oid) as numberOfOrders,ocid from orders group by ocid order by numberOfOrders ASC limit 5 ")
        result7a.show()

        //using dataframe functions

        val result7aDf = orderDF.select(col("oid"), col("ocid")).groupBy("ocid").agg(count("oid").as("numberOfOrders")).orderBy(col("numberOfOrders").asc).limit(5)
        result7aDf.show()
        //</editor-fold>

        //----------------------------------------------------------------------------------------------------------------------------------------------------------------

        //2.g.2 Find the top 5 customers with highest sum of orders //added order_id

        //<editor-fold desc="Solution">

        val result8 = spark.sql(s"select sum(oid) as sumOfOrders,ocid from orders group by ocid order by sumOfOrders DESC limit 5 ")
        result8.show()

        //using dataframe functions

        val result8Df = orderDF.select(col("oid"), col("ocid")).groupBy("ocid").agg(sum("oid").as("sumOfOrders")).orderBy(col("sumOfOrders").desc).limit(5)
        result8Df.show()
        //</editor-fold>


        //extra // Find the top 5 customers with lowest sum of orders
        //<editor-fold desc="Solution">

        val result8a = spark.sql(s"select sum(oid) as sumOfOrders,ocid from orders group by ocid order by sumOfOrders ASC limit 5 ")
        result8a.show()

        //using dataframe functions

        val result8aDf = orderDF.select(col("oid"), col("ocid")).groupBy("ocid").agg(sum("oid").as("sumOfOrders")).orderBy(col("sumOfOrders").asc).limit(5)
        result8aDf.show()
        //</editor-fold>

        //----------------------------------------------------------------------------------------------------------------------------------------------------------------
        //2.h Find Customer who did not order in last one month or long time

        //<editor-fold desc="Solution">

        val result9 = spark.sql(s"select ocid from orders where DATEDIFF(current_date(),date ) >=30 ")
        result9.show()


        val result9Df = orderDF.select(col("ocid")).where(s"${datediff(current_date(), col("date"))} >= 30")
        result9Df.show()

        //</editor-fold>


        //----------------------------------------------------------------------------------------------------------------------------------------------------------------
        //2.i Find the last order date for all customers
        //<editor-fold desc="Solution ">

        val result10 = spark.sql(s"select max(date) as lastOrder,ocid from orders group by ocid order by ocid ")
        result10.show()

        //using dataframe functions

        val result10Df = orderDF.select(col("date"), col("ocid")).groupBy("ocid").agg(max("date").as("lastOrder")).orderBy(col("ocid"))
        result10Df.show()
        //</editor-fold>

        //----------------------------------------------------------------------------------------------------------------------------------------------------------------
        //extra //Find the first order date for all customers

        //<editor-fold desc="Solution">

        val result10a = spark.sql(s"select min(date) as lastOrder,ocid from orders group by ocid order by ocid ")
        result10a.show()

        //using dataframe functions

        val result10aDf = orderDF.select(col("date"), col("ocid")).groupBy("ocid").agg(min("date").as("lastOrder")).orderBy(col("ocid"))
        result10aDf.show()
        //</editor-fold>

        //----------------------------------------------------------------------------------------------------------------------------------------------------------------
        //2.j Find the open and close number of order for a customer
        //<editor-fold desc="Solution">

        print("enter the customer ID")
        val customer_id = readLine()
        val result11 = spark.sql(s"(select count(oid),status from orders where status='CLOSED' and ocid='$customer_id' group by status)")
        val result11a = spark.sql(s"(select count(oid),status from orders where status<>'CLOSED' and ocid='$customer_id' group by status)")
        result11.show()
        result11a.show()

        //using dataframe functions
        val result11Df = orderDF.select(col("oid"), col("status")).where(s"status == 'CLOSED' and ocid='$customer_id'").agg(count(col("oid")).as("number of Closed orders")).orderBy(col("number of Closed orders").desc).limit(1)
        val result11aDf = orderDF.select(col("oid"), col("status")).where(s"status <> 'CLOSED' and ocid='$customer_id'").agg(count(col("oid")).as("number of Open orders")).orderBy(col("number of Open orders").desc).limit(1)

        result11Df.show()
        result11aDf.show()
        //</editor-fold>

        //----------------------------------------------------------------------------------------------------------------------------------------------------------------
        //2.k Find number of customers in every state

        //<editor-fold desc="Solution">

        val result12 = spark.sql(s"select count(id) as numberOfCustomers,state from customers group by state")
        result12.show()


        //using dataframe functions

        val result12Df = customerDF.select(col("id"), col("state")).groupBy("state").agg(count("id").as("numberOfCustomers"))
        result12Df.show()
        //----------------------------------------------------------------------------------------------------------------------------------------------------------------
        //</editor-fold>


    }

}

