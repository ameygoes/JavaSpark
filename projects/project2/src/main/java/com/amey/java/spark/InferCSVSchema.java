package com.amey.java.spark;

import com.amey.java.spark.sparkItutils.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class InferCSVSchema {
	
	public void printSchema() {
			SparkUtils sparkUtils = new SparkUtils();
		 	SparkSession spark = sparkUtils.getSparkSession("Complex CSV to Dataframe");

		    Dataset<Row> df = sparkUtils.readComplexCSV(spark);
		 
		    System.out.println("Except of the dataframe content:");

			// truncate after 90 chars
		    df.show(7, 90);

		    System.out.println("Dataframe's schema:");
		    df.printSchema();
	}
	
}
