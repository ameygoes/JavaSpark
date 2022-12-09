/*
 * Copyright (c) 12/8/2022 . Amey Bhilegaonkar. All rights reserved.
 * AUTHOR: Amey Bhilegoankar
 * PORTFOLIO: https://ameyportfolio.netlify.app/
 * FILE CREATION DATE: 12/8/2022
 */
package com.amey.java.spark.dbUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import java.util.Properties;
import static com.amey.java.spark.constants.Constants.*;
import static org.apache.hadoop.hdfs.server.namenode.ListPathsServlet.df;

public class DBUtils {

    private String dbURL = String.format("jdbc:postgresql://localhost/%s", DB_NAME);

    public static void main(String[] args) {

    }
    //	LETS SAVE THE DATAFRAME TO DB
    public Properties setDBProperties(){
        Properties prop = new Properties();
        prop.setProperty("driver", POSTGRES_DRIVER);
        prop.setProperty("user", POSTGRES_USER);
        prop.setProperty("password", PASSWORD);
        return prop;
    }

    public void writeToDB(String tableName, Dataset<Row> df){
        System.out.println(this.dbURL);
    // SAVE MODE WILL BE OVERWRITTEN TO WRITE THE DATA TO TABLE
		df.write()
            .mode(SaveMode.Overwrite)
            .jdbc(this.dbURL, tableName, this.setDBProperties());
    }

}
