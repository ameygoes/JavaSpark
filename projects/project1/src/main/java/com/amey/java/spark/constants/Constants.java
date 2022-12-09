/*
 * Copyright (c) 12/8/2022 . Amey Bhilegaonkar. All rights reserved.
 * AUTHOR: Amey Bhilegoankar
 * PORTFOLIO: https://ameyportfolio.netlify.app/
 * FILE CREATION DATE: 12/8/2022
 */
package com.amey.java.spark.constants;

public class Constants {
    public static final String PASSWORD = System.getenv("POSTGRES_PASS");
    public static final String POSTGRES_USER = "postgres";
    public static final String POSTGRES_DRIVER = "org.postgresql.Driver";

    public static final String TABLE_NAME = "project1";
    public static final String DB_NAME = "csvToDB";
}
