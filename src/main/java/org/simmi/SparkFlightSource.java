package org.simmi;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.streaming.LongOffset;
import org.apache.spark.sql.execution.streaming.Offset;
import org.apache.spark.sql.execution.streaming.Source;
import org.apache.spark.sql.types.StructType;
import scala.Option;

import java.util.concurrent.SynchronousQueue;

public class SparkFlightSource implements Source {
    public static final SynchronousQueue<Dataset<Row>> queue = new SynchronousQueue<>();

    StructType schema;
    public SparkFlightSource(StructType schema) {
        this.schema = schema;
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public Option<Offset> getOffset() {
        return Option.apply(LongOffset.apply(0));
    }

    @Override
    public Dataset<Row> getBatch(Option<Offset> start, Offset end) {
        try {
            return queue.take();
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void commit(Offset end) {
        Source.super.commit(end);
    }

    @Override
    public org.apache.spark.sql.connector.read.streaming.Offset initialOffset() {
        return Source.super.initialOffset();
    }

    @Override
    public org.apache.spark.sql.connector.read.streaming.Offset deserializeOffset(String json) {
        return Source.super.deserializeOffset(json);
    }

    @Override
    public void commit(org.apache.spark.sql.connector.read.streaming.Offset end) {
        Source.super.commit(end);
    }

    @Override
    public void stop() {

    }
}
