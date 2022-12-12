package com.amey.java.spark;

import com.amey.java.spark.sparkItutils.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DefineCSVSchema {

	public void printDefinedSchema() {

		SparkUtils sparkUtils = new SparkUtils();

		SparkSession spark = sparkUtils.getSparkSession("Complex CSV to Dataframe with predefined schema");
        
        StructType schema = DataTypes.createStructType(new StructField[] { //
	            DataTypes.createStructField(
	                "id", //
	                DataTypes.IntegerType, //
	                false), //
	            DataTypes.createStructField(
	                "product_id",
	                DataTypes.IntegerType,
	                true),
	            DataTypes.createStructField(
	                "item_name",
	                DataTypes.StringType,
	                false),
	            DataTypes.createStructField(
	                "published_on",
	                DataTypes.DateType,
	                true),
	            DataTypes.createStructField(
	                "url",
	                DataTypes.StringType,
	                false) });
	     
	        Dataset<Row> df = sparkUtils.readComplexCSVWithSchema(spark, schema);

	        df.show(5, 15);
	        df.printSchema();
		
	}

}
