package com.amey.java.spark;

public class Application {

	public static void main(String[] args) {

		// INFER SCHEMA IS A USER DEFINED CLASS, WHICH TAKES IN
		InferCSVSchema parser = new InferCSVSchema();
		parser.printSchema();
		
		DefineCSVSchema parser2 = new DefineCSVSchema();
		parser2.printDefinedSchema();

		JSONLinesParser parser3 = new JSONLinesParser();
		parser3.parseJsonLines();

	}

}
