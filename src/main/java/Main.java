import org.apache.spark.sql.*;

public class Main {
    public static void main(String[] args) {
        System.out.println("Initiating connection to Elasticsearch...");

        // Establish a Spark session with Elasticsearch configurations
        SparkSession session = SparkSession.builder()
                .appName("ElasticsearchIntegration")
                .master("local[*]")
                .config("es.nodes", "172.23.0.1")
                .config("es.port", "9200")
                .getOrCreate();

        System.out.println("Configuring data stream...");

        // Reading CSV data into a DataFrame with schema inference
        Dataset<Row> vahobiddin = session.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("/home/welltron/Downloads/data/part-00003-533b6aa9-2b29-4da8-8554-715668de302b-c000.csv");

        // Extracting date from datetime column
        vahobiddin = vahobiddin.withColumn("extracted_date", functions.date_format(vahobiddin.col("date_time"), "yyyy-MM-dd"));

        // Elasticsearch index for storing data
        String indexName = "vahobiddin/data";

        // Indexing DataFrame in Elasticsearch
        vahobiddin.write()
                .format("org.elasticsearch.spark.sql")
                .option("es.resource", indexName)
                .mode(SaveMode.Append)
                .save();

        // Terminating Spark session
        session.stop();

        System.out.println("Process completed. Check Elasticsearch at http://localhost:9200/vahobiddin and http://localhost:9200/vahobiddin/_search?pretty for details.");
    }
}
