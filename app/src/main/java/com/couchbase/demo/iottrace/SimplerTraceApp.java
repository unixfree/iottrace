package com.couchbase.demo.iottrace;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.java.ClusterOptions;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import java.time.Duration;

public class SimplerTraceApp {

  // Update these connection details
  private static final String endpoint = "couchbase://127.0.0.1";
  private static final String username = "ss";
  private static final String password = "ss";
  private static final String bucketName = "fdc";

  public static void main(String[] args) throws Exception {
    String equipmentId = args[0];
    int numDocuments = 10000; // Number of documents to generate

    // Connect to Couchbase
    Cluster cluster = connectToCouchbase();
    Bucket bucket = cluster.bucket(bucketName);
    Collection collection = bucket.scope("poc").collection("trace");

    // Generate timestamps
    LocalDateTime startTime = LocalDateTime.now();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("YYYY-MM-DD HH:mm:ss[.SSS]");

    // Create a thread pool for document generation
    ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    try {
      for (int i = 0; i < numDocuments; i++) {
        String docKey = generateDocumentKey(equipmentId, startTime, formatter);
        executor.submit(() -> generateAndStoreDocument(collection, docKey));
      }

      // Wait for all tasks to finish
      executor.shutdown();
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    } finally {
      cluster.disconnect();
    }

    LocalDateTime endTime = LocalDateTime.now();
    System.out.println("===================== Start Time : " + startTime.format(formatter));
    System.out.println("===================== End Time   : " + endTime.format(formatter));
    System.out.println("===================== END ==========================");
  }

  private static Cluster connectToCouchbase() {
    ClusterEnvironment env = ClusterEnvironment.builder()
        .timeoutConfig(TimeoutConfig.kvTimeout(Duration.ofSeconds(10))) // Set timeout for KV operations
        .build();

    Cluster cluster = Cluster.connect("localhost",
        ClusterOptions.clusterOptions("username", "password").environment(env));
    return cluster;
}


  private static String generateDocumentKey(String equipmentId,
         LocalDateTime timestamp, DateTimeFormatter formatter) {
    return equipmentId + timestamp.format(formatter);
  }

  private static void generateAndStoreDocument(Collection collection, String docKey) {
    try {
        JsonObject doc = createTraceDocument(docKey);
        collection.upsert(docKey, doc.toString(), UpsertOptions.upsertOptions().transcoder(RawJsonTranscoder.INSTANCE));
    } catch (Exception e) {
        System.err.println("Error storing document: " + e.getMessage());
        // Handle the error appropriately, e.g., log details, retry, or notify user
    }
}


  private static JsonObject createTraceDocument(String equipmentId) {
    JsonObject doc = JsonObject.create();
    doc.put("equipmentId", equipmentId);
    // Add other fields to the document as needed
    return doc;
}
}
