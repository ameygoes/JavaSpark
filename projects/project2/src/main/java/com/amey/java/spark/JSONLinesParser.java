package com.amey.java.spark;

import com.amey.java.spark.sparkItutils.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JSONLinesParser {
	
	 
	 
	  public void parseJsonLines() {
		  SparkUtils sparkUtils = new SparkUtils();
		  SparkSession spark = sparkUtils.getSparkSession("Complex JSON to Dataframe");
	    
		Dataset<Row> df1 = sparkUtils.readSimpleJSON(spark);
		Dataset<Row> df2 = sparkUtils.readMultilineJSON(spark);

		  df1.show(5, 150);
		  df1.printSchema();

	    df2.show(5, 150);
	    df2.printSchema();
	  }
	
	
}



