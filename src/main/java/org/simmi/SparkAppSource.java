package org.simmi;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.streaming.Source;
import org.apache.spark.sql.sources.StreamSourceProvider;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.Map;

public class SparkAppSource implements StreamSourceProvider {
    private static final StructField[]  fields = {StructField.apply("name", DataTypes.StringType, true, Metadata.empty())}; //{StructField.apply("_id", DataTypes.StringType, true, Metadata.empty()), StructField.apply("job", DataTypes.StringType, true, Metadata.empty()), StructField.apply("field", DataTypes.StringType, true, Metadata.empty()), StructField.apply("value", DataTypes.StringType, true, Metadata.empty()), StructField.apply("sec", DataTypes.StringType, true, Metadata.empty())};
    public static final StructType SCHEMA = new StructType(fields);

    @Override
    public Tuple2<String, StructType> sourceSchema(SQLContext sqlContext, Option<StructType> schema,
                                                   String providerName, Map<String, String> parameters) {
        return new Tuple2<>("sparkApp", SCHEMA);
    }

    @Override
    public Source createSource(SQLContext sqlContext, String metadataPath, Option<StructType> schema,
                               String providerName, Map<String, String> parameters) {
        return new SparkFlightSource(SCHEMA);
    }
}
