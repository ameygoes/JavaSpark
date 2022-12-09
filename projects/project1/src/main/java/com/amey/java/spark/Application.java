package com.amey.java.spark;

import com.amey.java.spark.dbUtils.DBUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.amey.java.spark.constants.Constants.TABLE_NAME;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class Application {

	public static void main(String args[]) throws InterruptedException {

//		GET THE SPARK SESSION TO CONNECT WITH MASTER NODE
		SparkSession spark = new SparkSession.Builder()
				.appName("CSV to DataBase")
				.master("local")
				.getOrCreate();


		//		GET DATA FROM SRC/MAIN/RESOURCES FILE
		//		IN PRODUCTION ENVIRONMENT THIS LOAD IS GOING TO BE HDFS/S3 ETC
		Dataset<Row> df = spark.read()
				.format("csv")
				.option("header", true)
				.load("src/main/resources/name_and_comments.txt");


		//		df  IS LIKE A TABLE, WE CAN PERFORM ALL THE OPERATIONS SUCH AS, GROUP BY SUM ETC
		//		SHOW TAKES A DEFAULT ARGUMENTS TO SHOW HOW MANY ROWS YOU WANT TO SHOW
		df.show(5);

		// 		IT WILL JUST MAP THE DF, BECAUSE WE HAVEN'T DONE ANY ACTIONS
//		not a Transformation
		df.withColumn("FullName", concat(df.col("last_name"), lit(" "), df.col("first_name")));

		//		SO THIS df.show() WILL NOT BE MODIFIED, BECAUSE DATASET IS A IMMUTABLE STRUCTURE
//		df.show();


		//		IF WE ASSIGN IT BACK TO DF IT WILL WORK, IT WILL CREATE A NEW DF AND ASSIGN IT TO THAT AND NAME IT DF AGAIN
		//		withColumn Function takes in two arguments, one is name of the coloumn we want to create and the value of it
		//		concat function returns a that value
		//		Transformation
		df = df.withColumn("FullName", concat(df.col("last_name"), lit(" "), df.col("first_name")));
//		df.show();

		//		TRANSFORMATION 2
		//		EXTRACT ONLY ONE COLOUMN FORM THE DATASET
		df = df.filter(df.col("comment")
				.rlike("d"))
				.orderBy(df.col("last_name")
				.asc()) ;
//		df.show();

		DBUtils dbUtils = new DBUtils();
		dbUtils.writeToDB(TABLE_NAME, df);



	}
}