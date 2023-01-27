package org.simmi;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.spark.api.python.Py4JServer;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SparkFlightServer implements AutoCloseable {
    final static Location location = Location.forGrpcInsecure("0.0.0.0", 33333);
    private Py4JServer py4jServer;
    private SparkAppStreaming sparkAppStreaming;
    private ExecutorService executor;
    private Future<String>  streamRes;

    public SparkFlightServer() {
        executor = Executors.newSingleThreadExecutor();
    }

    private void init() {
        sparkAppStreaming = new SparkAppStreaming();
        initPy4JServer(sparkAppStreaming.getSparkSession());
        sparkAppStreaming.setPy4JServer(py4jServer);
        streamRes = executor.submit(sparkAppStreaming);
    }

    private void initPy4JServer(SparkSession spark) {
        py4jServer = new Py4JServer(spark.sparkContext().conf());
        py4jServer.start();
    }
    public static void main(String[] args) throws Exception {
        try (var sparkFlightServer = new SparkFlightServer(); BufferAllocator allocator = new RootAllocator()) {
            sparkFlightServer.init();
            try (FlightServer flightServer = FlightServer.builder(allocator, location,
                                                                  new SparkAppProducer(sparkFlightServer.sparkAppStreaming.getSparkSession(), SparkAppSource.SCHEMA, allocator, location))
                                                         .build()) {
                try {
                    flightServer.start();
                    System.out.println("S1: Server (Location): Listening on port " + flightServer.getPort());
                    flightServer.awaitTermination();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        sparkAppStreaming.close();
        py4jServer.shutdown();
        executor.shutdown();
    }
}