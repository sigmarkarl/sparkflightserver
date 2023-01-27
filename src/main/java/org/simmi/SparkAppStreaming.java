package org.simmi;

import org.apache.spark.api.python.Py4JServer;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.time.Duration;
import java.util.concurrent.Callable;

public class SparkAppStreaming implements Callable<String>, AutoCloseable {
    private static final int DEFAULT_TIMEOUT = 3600*2;
    private final SparkSession sparkSession;
    private Py4JServer py4JServer;

    public SparkAppStreaming(SparkSession sparkSession, Py4JServer py4JServer) {
        this.sparkSession = sparkSession;
        this.py4JServer = py4JServer;
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public SparkAppStreaming() {
        this(SparkSession.builder().master("local[*]").appName("SparkAppStreaming").getOrCreate(), null);
    }

    public void setPy4JServer(Py4JServer py4JServer) {
        this.py4JServer = py4JServer;
    }

    @Override
    public String call() throws Exception {
        try(SparkBatchConsumer sparkBatchConsumer = new SparkBatchConsumer(sparkSession, py4JServer.secret(), py4JServer.getListeningPort())) {
            StreamingQuery query = sparkSession.readStream().format("org.simmi.SparkAppSource")
                                               //.option("stream.keys", streamKey)
                                               .schema(SparkAppSource.SCHEMA)
                                               .load().writeStream().outputMode("update")
                                               .foreachBatch(sparkBatchConsumer).start();
            //redisBatchConsumer.mont.setQuery(query, timeoutCount);
            query.awaitTermination(Duration.ofMinutes(DEFAULT_TIMEOUT).toMillis());
        }
        return "";
    }

    @Override
    public void close() throws Exception {

    }
}
