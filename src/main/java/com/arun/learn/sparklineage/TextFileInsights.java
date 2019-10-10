package com.arun.learn.sparklineage;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.monotonically_increasing_id;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TextFileInsights {

	public static void main(String[] args) {
		
		String filePath = "/tmp/input.txt";
		
		String filePath2 = "/tmp/input2.txt";
		
		String outputPath = "/tmp/";

		Dataset<Row> r1 = readFileAsRdd1(filePath);
		Dataset<Row> r2 = readFileAsRdd2(filePath);
				
		r1.join(r2, r1.col("word").equalTo(r2.col("threePlusWords")), "outer").toDF()
			.write().mode(SaveMode.Overwrite).csv("/tmp/threePlusJoin");
		
		r1.except(r2).write().mode(SaveMode.Overwrite).csv("/tmp/r1-r2");
		r2.except(r1).write().mode(SaveMode.Overwrite).csv("/tmp/r2-r1");
		
		r1.filter(r1.col("word").startsWith("a")).write().mode(SaveMode.Overwrite).csv("/tmp/startWithA");
		
		r1.filter(r1.col("word").startsWith("a"))
		.except(r2)
		.write().mode(SaveMode.Overwrite).csv("/tmp/startWithA-r2");
;
		
	}

	private static Dataset<Row> readFileAsRdd1(String path) {
		SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
		
		
		// Creates a DataFrame having a single column named "line"
		JavaRDD<String> textFile = spark.sparkContext().textFile(path, 0).toJavaRDD();
		JavaRDD<Row> rowRDD = textFile.flatMap(x -> Arrays.asList(x.split(" ")).iterator()).map(RowFactory::create);
		
		
		List<StructField> fields = Arrays.asList(
		  DataTypes.createStructField("word", DataTypes.StringType, true)
		  );
		
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> df = spark.sqlContext().createDataFrame(rowRDD, schema)
				//.dropDuplicates()
				;	
		
		return df.withColumn("_id1", monotonically_increasing_id());
		
		
		
	}
	
	private static Dataset<Row> readFileAsRdd2(String path) {
		SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
		
		
		// Creates a DataFrame having a single column named "line"
		JavaRDD<String> textFile = spark.sparkContext().textFile(path, 0).toJavaRDD();
		JavaRDD<Row> rowRDD = textFile.flatMap(x -> Arrays.asList(x.split(" ")).iterator()).map(RowFactory::create);
		
	
		
		List<StructField> fields = Arrays.asList(
		  DataTypes.createStructField("word", DataTypes.StringType, true)
		  );
		
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> df = spark.sqlContext().createDataFrame(rowRDD, schema)
				//.dropDuplicates()
				.filter(length(col("word")).gt(3)).withColumnRenamed("word", "threePlusWords");	
		
		df.show();
		
		return df.withColumn("_id2", monotonically_increasing_id());
		
		
		
	}

}
