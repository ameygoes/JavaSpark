/*
 * Copyright (c) 12/12/2022 . Amey Bhilegaonkar. All rights reserved.
 * AUTHOR: Amey Bhilegoankar
 * PORTFOLIO: https://ameyportfolio.netlify.app/
 * FILE CREATION DATE: 12/12/2022
 */
package com.amey.java.spark.sparkItutils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static com.amey.java.spark.constants.Constants.*;

public class SparkUtils {

    // RETURNS A SPARK SESSION
    public SparkSession getSparkSession(String appName) {
        SparkSession spark = SparkSession.builder()
                .appName(appName)
                .master("local")
                .getOrCreate();

        return spark;
    }

    // FUNCTION TO READ CSV FILE CONTENTS, AND RETURN A DATAFRAME
    public Dataset<Row> readComplexCSV(SparkSession spark){
        return spark.read().format("csv") //
                .option("header", "true") //
                .option("multiline", true) //
                .option("sep", ";") //
                .option("quote", "^") //
                .option("dateFormat", "M/d/y") //
                .option("inferSchema", true) //
                .load(CSV_PATH);
    }


    // FUNCTION TO READ CSV FILE CONTENTS with PREDEFINED SCHEMA, AND RETURN A DATAFRAME
    public Dataset<Row> readComplexCSVWithSchema(SparkSession spark, StructType schema){
        return spark.read().format("csv")
                .option("header", "true")
                .option("multiline", true)
                .option("sep", ";")
                .option("dateFormat", "M/d/y")
                .option("quote", "^")
                .schema(schema) //
                .load(CSV_PATH);
    }

    // FUNCTION TO READ SIMPLE JSON FILE CONTENTS, AND RETURN A DATAFRAME
    public Dataset<Row> readSimpleJSON(SparkSession spark){
        return spark.read().format("json")
		        .load(SIMPLE_JSON_PATH);
    }

    // FUNCTION TO READ CSV FILE CONTENTS, AND RETURN A DATAFRAME
    public Dataset<Row> readMultilineJSON(SparkSession spark){
        return spark.read().format("json")
                .option("multiline", true)
                .load(MULTILINE_JSON_PATH);
    }

}
