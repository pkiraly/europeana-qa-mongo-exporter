package de.gwdg.metadataqa.mongo;

import org.apache.commons.cli.ParseException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

public final class ReadFromMongoDB {

  public static void main(final String[] args) throws ParseException {

    Parameters parameters = new Parameters(args);

    SparkSession spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config(
        "spark.mongodb.input.uri",
        String.format("mongodb://%s:%s/%s.record",
                parameters.getMongoHost(), parameters.getMongoPort(), parameters.getMongoDatabase())
      )
      .getOrCreate();

    // Create a JavaSparkContext using the SparkSession's SparkContext object
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    /*Start Example: Read data from MongoDB************************/
    JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
    /*End Example**************************************************/

    // Analyze data from MongoDB
    System.out.println(rdd.count());
    System.out.println(rdd.first().toJson());

    jsc.close();
  }
}
