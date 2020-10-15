package de.gwdg.metadataqa.mongo;

import com.jayway.jsonpath.InvalidJsonException;
import com.mongodb.MongoClient;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.mongodb.util.JSON;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.logging.Logger;

public class MongoToJson {
  static final Logger logger = Logger.getLogger(MongoToJson.class.getCanonicalName());

  public static void main(final String[] args)
    throws InterruptedException, ParseException, MalformedURLException {

    Parameters parameters = new Parameters(args);
    if (StringUtils.isBlank(parameters.getOutputFileName())) {
      System.err.println("Please provide a full path to the output file");
      System.exit(0);
    }

    if (StringUtils.isBlank(parameters.getRecordAPIUrl())) {
      System.err.println("Please provide a URL of the record API");
      System.exit(0);
    }
    System.err.println(parameters.toString());

    SparkSession spark = createSparkSession(
      parameters.getMongoHost(), parameters.getMongoPort(),
      parameters.getMongoUser(), parameters.getMongoPassword(),
      parameters.getMongoDatabase(), "record",
      parameters.getPartitionerType()
    );

    final DocumentTransformer transformer = new DocumentTransformer();

    // Create a JavaSparkContext using the SparkSession's SparkContext object
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
    final CodecRegistry defaultRegistry = MongoClient.getDefaultCodecRegistry();
    final CodecRegistry codecRegistry = CodecRegistries.fromRegistries(defaultRegistry);
    final DocumentCodec codec = new DocumentCodec(codecRegistry, new BsonTypeClassMap());

    // JsonWriterSettings writerSettings = new JsonWriterSettings(JsonMode.STRICT, "", "");

    final EuropeanaRecordReaderAPIClient client = new EuropeanaRecordReaderAPIClient(
      parameters.getRecordAPIUrl()
    );

    final boolean useApi = true;

    JavaRDD<String> baseCountsRDD = rdd.map(record -> {
      String json = "";
      if (parameters.idsOnly()) {
        String jsonFragment = JSON.serialize(record);
        json = record.get("about", String.class);
      } else {
        if (useApi) {
          String jsonFragment = JSON.serialize(record);
          String id = record.get("about", String.class);
          int wait = 2;
          boolean success = false;
          int trial = 1;
          do {
            if (trial > 1) {
              logger.info("trial #" + trial);
            }
            try {
              json = client.resolveFragmentWithPost(jsonFragment, id);
              success = true;
            } catch (IOException e) {
              wait = 5;
              if (e.getLocalizedMessage().equals("Cannot assign requested address"))
                logger.severe(String.format("Id: %s. Error: %s", id, e.getLocalizedMessage()));
              else
                logger.severe(
                  String.format(
                    "Resolving error. Id: %s, fragment: %s. Error message: %s (%s).",
                    id, jsonFragment, e.getLocalizedMessage(), e.getCause()
                  )
                );
              e.printStackTrace();
            } catch (InvalidJsonException e) {
              logger.severe(String.format("Invalid JSON: %s. Error message: %s.",
                jsonFragment, e.getLocalizedMessage()));
            }
            trial++;
            Thread.sleep(wait);
          } while (!success);
        } else {
          MongoClient myClient = new MongoClient(parameters.getMongoHost(), 27017);
          transformer.transform(
            myClient.getDatabase(parameters.getMongoDatabase()),
            record,
            true
          );

          json = record.toJson(
            new DocumentCodec(
              CodecRegistries.fromRegistries(MongoClient.getDefaultCodecRegistry()),
              new BsonTypeClassMap()
            )
          );
          myClient.close();
          Thread.sleep(10);
        }
      }

      return json;
    });

    baseCountsRDD
      .filter(record -> !record.equals(""))
      .saveAsTextFile(parameters.getOutputFileName());

    jsc.close();
  }

  /**
   * Create a new Spark session
   *
   * @param mongoHost Host of MongoDB
   * @param mongoPort Port of MongoDB
   * @param database  MongoDB database name
   * @param collection MongoDB base collection name to iterate over
   *
   * @return A Spark session
   */
  private static SparkSession createSparkSession(String mongoHost, Integer mongoPort,
                                                 String mongoUser, String mongoPassword,
                                                 String database, String collection,
                                                 PartitionerType type) {

    String credentialsPrefix = "";
    String credentialsPostfix = "";
    if (mongoUser != null && mongoPassword != null) {
      credentialsPrefix = String.format("%s:%s@", mongoUser, mongoPassword);
      credentialsPostfix = "?authSource=admin&gssapiServiceName=mongodb";
    }

    String uri = (mongoPort == null)
      ? String.format("mongodb://%s%s/", credentialsPrefix, mongoHost)
      : String.format("mongodb://%s%s:%d/", credentialsPrefix, mongoHost, mongoPort);
    uri += String.format("%s.%s", database, collection);
    uri += credentialsPostfix;

    logger.info("URL: " + uri);

    SparkSession.Builder builder = SparkSession
      .builder()
      // .master("local[*]")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", uri)
      .config("spark.mongodb.input.database", database)
      .config("spark.mongodb.input.collection", collection)
      .config("spark.mongodb.input.readPreference.name", "primaryPreferred");
    if (type == null || type.equals(PartitionerType.NONE)) {
      // builder.config("spark.mongodb.input.partitioner", "MongoDefaultPartitioner");
    } else if (type.equals(PartitionerType.DEFAULT)) {
      builder = builder.config("spark.mongodb.input.partitioner", "MongoDefaultPartitioner");
    } else if (type.equals(PartitionerType.SAMPLE)) {
      builder = builder.config("spark.mongodb.input.partitioner", "MongoSamplePartitioner");
    } else if (type.equals(PartitionerType.SPLITVECTOR)) {
      builder = builder.config("spark.mongodb.input.partitioner", "MongoSplitVectorPartitioner");
    }

    builder = builder
      .config("spark.mongodb.input.partitionerOptions.partitionKey", "_id")
      .config("spark.mongodb.input.partitionerOptions.partitionSizeMB", "64");

    return builder.getOrCreate();
  }

  private static boolean isProcessable(int partitionId) {
    return (partitionId == 2 || partitionId == 413 || partitionId > 1238);
  }

}
