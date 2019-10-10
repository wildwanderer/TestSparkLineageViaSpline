package com.arun.learn.sparklineage.dummy;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;

/**
 * Hello world!
 *
 */
public class App {
	
	private static UDF1 priceConv = new UDF1<String, Double>() {
	    public Double call(final String str) throws Exception {
	    	
	    	try {
	    		return Double.parseDouble(str.replace("\t", "."));
	    	} catch(Exception e) {
	    		
	    	}
	    	return new Random().nextDouble();
	    	
	        
	    }
	};
	
	
	
	public static void main(String[] args) throws Exception {
		System.out.println("Hello World!");
		// runJoin();
		readCustomer();
		readProduct();
		readSales();

	}

	

	private static void readSales() {
		// TODO Auto-generated method stub

	}

	private static void readProduct() throws Exception {
		
		
		
		
		//ProductID;ProductName;Price;CategoryID;Class;ModifyDate;Resistant;IsAllergic;VitalityDays 
		
		SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
		
		spark.sqlContext().udf().register("priceConv", priceConv, DataTypes.DoubleType);

		List<StructField> fields = new ArrayList<>();
		
	

		StructField productId = DataTypes.createStructField("ProductID", DataTypes.IntegerType, true);
		StructField productName = DataTypes.createStructField("ProductName", DataTypes.StringType, true);
		StructField price = DataTypes.createStructField("Price", DataTypes.StringType, true);
		StructField catgId = DataTypes.createStructField("CategoryID", DataTypes.IntegerType, true);
		StructField classType = DataTypes.createStructField("Class", DataTypes.StringType, true);
		StructField modifyDate = DataTypes.createStructField("ModifyDate", DataTypes.StringType, true);
		StructField resistant = DataTypes.createStructField("Resistant", DataTypes.StringType, true);
		StructField isAllergic = DataTypes.createStructField("IsAllergic", DataTypes.StringType, true);
		StructField vitalityDays = DataTypes.createStructField("VitalityDays", DataTypes.StringType, true);

		fields.add(productId);
		fields.add(productName);
		fields.add(price);
		fields.add(catgId);
		fields.add(classType);
		fields.add(modifyDate);
		fields.add(resistant);
		fields.add(isAllergic);
		fields.add(vitalityDays);

		StructType schema = DataTypes.createStructType(fields);
		
		Dataset<Row> df = spark.read().format("csv")
		  .option("header", "true")
		  .option("dateFormat", "dd/mm/yyyy  hh:mm:ss")
		  .schema(schema)
		  .load("/Users/a0p00q1/Downloads/products_updated.csv");
						
		if(df.count() == 1) {
			throw new Exception("failed to read records proeporly.");
		}
		
		
		
		
		df.withColumn("priceUpdated", callUDF("priceConv",(col("price")))).drop("price").withColumnRenamed("priceUpdated","price").show(10);
		System.out.println("---> " + df.count());

	}

	

	private static void readCustomer() {
		SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();

		List<StructField> fields = new ArrayList<>();

		StructField custId = DataTypes.createStructField("CustomerID", DataTypes.IntegerType, true);
		StructField fName = DataTypes.createStructField("FirstName", DataTypes.StringType, true);
		StructField mInitial = DataTypes.createStructField("MiddleInitial", DataTypes.StringType, true);
		StructField lName = DataTypes.createStructField("LastName", DataTypes.StringType, true);
		StructField cityId = DataTypes.createStructField("CityID", DataTypes.IntegerType, true);
		StructField addresss = DataTypes.createStructField("Address", DataTypes.StringType, true);

		fields.add(custId);
		fields.add(fName);
		fields.add(mInitial);
		fields.add(lName);
		fields.add(cityId);
		fields.add(addresss);

		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> df = spark.read().option("delimiter", ";").csv("/Users/a0p00q1/Downloads/customers_updated.csv");
		
		df.show(10);
		
		System.out.println("---> " + df.count());

	}

	private static void runJoin() {

		String logFile = "/Users/a0p00q1/Downloads/CSV_Database_of_First_Names.csv";
		SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
		Dataset<String> logData = spark.read().textFile(logFile).cache();

		long numAs = logData.filter(new FilterFunction<String>() {

			@Override
			public boolean call(String value) throws Exception {
				return value.contains("a");
			}
		}).count();

		System.out.println("Lines with a: " + numAs);

		spark.stop();

	}
}
