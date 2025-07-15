package com.phdata;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;

public class SparkChargePoints {

    static  String input  = "data/input/electric-chargepoints-2017.csv";
    static  String output = "data/output/chargepoints-2017-analysis";

    static final SparkSession spark = SparkSession.builder()
            .master("local[*]")
            .appName("SparkChargePoints")
	    .config("spark.ui.enabled", "false")
	    .config("spark.sql.shuffle.partitions", "2")
	    .config("spark.driver.memory", "512m")
	    .getOrCreate();

    public static void main(String[] args) {
        load(transform(extract()));
        spark.stop();
    }

    // Reads CSV raw and returns DataFrame 
    private static Dataset<Row> extract() {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "false")
		.option("sep", ",")
                .csv(input);
    }

    // Calculates duration in hours 
    private static Dataset<Row> transform(Dataset<Row> df) {

	Column startTs = expr("int(split(StartTime, ':')[0]) * 60 + int(split(StartTime, ':')[1])");

  	Column endTs = expr("int(split(EndTime, ':')[0]) * 60 + int(split(EndTime, ':')[1])");

  	Column days = datediff(col("EndDate"), col("StartDate"));

  	Dataset<Row> pe = df.select(
      		col("CPID").alias("chargepoint_id"),
      		days.multiply(1440)
          	  .plus(endTs.minus(startTs))
                  .divide(60.0)
                  .alias("duration_hr")
	);

 	 Dataset<Row> out = pe.groupBy("chargepoint_id").agg(
          round(avg("duration_hr"), 2).alias("avg_duration"),
          round(max("duration_hr"), 2).alias("max_duration"))
      	 .orderBy("chargepoint_id");
 
    	return out;
    }

    // Saves in parquet
    private static void load(Dataset<Row> df) {
        df.coalesce(1)               
          .write()
          .mode(SaveMode.Overwrite)
          .parquet(output);
    }
}
