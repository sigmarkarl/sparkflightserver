package org.simmi;

import io.kubernetes.client.openapi.ApiException;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.SynchronousQueue;

public class SparkAppProducer extends NoOpFlightProducer implements AutoCloseable {
    private final BufferAllocator                          allocator;
    private final Location                                 location;
    private final ConcurrentMap<FlightDescriptor, Dataset>       datasets;
    private final Map<String,SynchronousQueue<ArrowRecordBatch>> queue = new ConcurrentHashMap<>();
    private final Schema schema = new Schema(Arrays.asList(
            new Field("type", FieldType.nullable(new ArrowType.Utf8()), null), new Field("query", FieldType.nullable(new ArrowType.Utf8()), null), new Field("config", FieldType.nullable(new ArrowType.Utf8()), null)));

    public SparkAppProducer(BufferAllocator allocator, Location location) {
        this.allocator = allocator;
        this.location = location;
        this.datasets = new ConcurrentHashMap<>();
    }
    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        var batches = new ArrayList<>();
        var path = flightStream.getDescriptor().getPath().get(0);
        var queue = this.queue.computeIfAbsent(path, s -> new SynchronousQueue<>());
        return () -> {
            long rows = 0;
            VectorUnloader unloader;
            while (flightStream.next()) {
                var vectorSchemaRoot = flightStream.getRoot();
                unloader = new VectorUnloader(vectorSchemaRoot);
                final ArrowRecordBatch arb = unloader.getRecordBatch();

                try {
                    if (!this.queue.containsKey(path)) {
                        var config = vectorSchemaRoot.getVector("config");
                        var sparkAppLauncher = new SparkAppLauncher();
                        for (int i = 0; i < config.getBufferSize(); i++) {
                            var yaml = config.getObject(i);
                            sparkAppLauncher.launchSparkApplicationCRD(yaml.toString());
                        }
                    }
                    System.err.println("current thread " + Thread.currentThread().getName());
                    queue.put(arb);
                } catch (InterruptedException | IOException | ApiException e) {
                    throw new RuntimeException(e);
                }
                batches.add(arb);
                rows += flightStream.getRoot().getRowCount();
            }

            //System.err.println("done accept");
            //Dataset dataset = new Dataset(batches, flightStream.getSchema(), rows);
            //datasets.put(flightStream.getDescriptor(), dataset);

            ackStream.onCompleted();
        };
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        //FlightDescriptor flightDescriptor = FlightDescriptor.path(
        //        new String(ticket.getBytes(), StandardCharsets.UTF_8));
        /*Dataset dataset = this.datasets.get(flightDescriptor);
        if (dataset == null) {
            throw CallStatus.NOT_FOUND.withDescription("Unknown descriptor").toRuntimeException();
        }*/
        var path = new String(ticket.getBytes(), StandardCharsets.UTF_8);
        var queue = this.queue.computeIfAbsent(path, s -> new SynchronousQueue<>());
        try (VectorSchemaRoot root = VectorSchemaRoot.create(
                schema, allocator)) {
            VectorLoader loader = new VectorLoader(root);
            listener.start(root);
            while(true) {
                var pythonCode = queue.take();
                System.err.println("receive " + Thread.currentThread().getId());
                loader.load(pythonCode);
                listener.putNext();
            }
            /*for (ArrowRecordBatch arrowRecordBatch : this.datasets.get(flightDescriptor).getBatches()) {
                loader.load(arrowRecordBatch);
                listener.putNext();
            }*/
            //listener.completed();
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        FlightDescriptor flightDescriptor = FlightDescriptor.path(
                new String(action.getBody(), StandardCharsets.UTF_8));
        switch (action.getType()) {
            case "DELETE": {
                Dataset removed = datasets.remove(flightDescriptor);
                if (removed != null) {
                    try {
                        removed.close();
                    } catch (Exception e) {
                        listener.onError(CallStatus.INTERNAL
                                                 .withDescription(e.toString())
                                                 .toRuntimeException());
                        return;
                    }
                    Result result = new Result("Delete completed".getBytes(StandardCharsets.UTF_8));
                    listener.onNext(result);
                } else {
                    Result result = new Result("Delete not completed. Reason: Key did not exist."
                                                       .getBytes(StandardCharsets.UTF_8));
                    listener.onNext(result);
                }
                listener.onCompleted();
            }
        }
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        FlightEndpoint flightEndpoint = new FlightEndpoint(
                new Ticket(descriptor.getPath().get(0).getBytes(StandardCharsets.UTF_8)), location);
        return new FlightInfo(
                datasets.get(descriptor).getSchema(),
                descriptor,
                Collections.singletonList(flightEndpoint),
                /*bytes=*/-1,
                datasets.get(descriptor).getRows()
        );
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        datasets.forEach((k, v) -> { listener.onNext(getFlightInfo(null, k)); });
        listener.onCompleted();
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(datasets.values());
    }
}
