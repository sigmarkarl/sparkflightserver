package org.simmi;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.spark.api.python.Py4JServer;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class SparkFlightServer implements AutoCloseable {
    final static Location location = Location.forGrpcInsecure("0.0.0.0", 33333);
    private Py4JServer py4jServer;

    public SparkFlightServer() {}

    private void initPy4JServer(SparkSession spark) {
        py4jServer = new Py4JServer(spark.sparkContext().conf());
        py4jServer.start();
    }
    public static void main(String[] args) throws Exception {
        try (var sparkFlightServer = new SparkFlightServer(); BufferAllocator allocator = new RootAllocator()) {
            try (FlightServer flightServer = FlightServer.builder(allocator, location,
                                                                  new SparkAppProducer(allocator, location))
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
        py4jServer.shutdown();
    }
}