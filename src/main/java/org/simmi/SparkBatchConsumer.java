package org.simmi;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SparkBatchConsumer implements VoidFunction2<Dataset<Row>, Long>, AutoCloseable {
    private final SparkSession sparkSession;
    private final int port;
    private final String          secret;
    private       ExecutorService es = Executors.newFixedThreadPool(2);

    ByteArrayOutputStream baOutput = new ByteArrayOutputStream();
    ByteArrayOutputStream  baError  = new ByteArrayOutputStream();
    public SparkBatchConsumer(SparkSession sparkSession, String secret, int port) {
        this.sparkSession = sparkSession;
        this.secret = secret;
        this.port = port;
    }

    @Override
    public void call(Dataset<Row> rowDataset, Long aLong) throws Exception {
        List<Row> rows = rowDataset.collectAsList();
        for (Row row : rows) {
            var cmd = row.getString(0);
            Files.writeString(Path.of("cmd.py"), cmd);
            List<String> cmds = new ArrayList<>();
            var pysparkPython = System.getenv("PYSPARK_PYTHON");
            cmds.add(pysparkPython != null ? pysparkPython : "python");
            cmds.add("cmd.py");
            ProcessBuilder     pb  = new ProcessBuilder(cmds);
            Map<String,String> env = pb.environment();

            env.put("PYSPARK_GATEWAY_PORT",Integer.toString(port));
            env.put("PYSPARK_GATEWAY_SECRET",secret);
            env.put("PYSPARK_PIN_THREAD","true");

            var pythonProcess = pb.start();

            es.submit(() -> pythonProcess.getErrorStream().transferTo(System.out));
            es.submit(() -> pythonProcess.getInputStream().transferTo(System.err));

            pythonProcess.waitFor();
        }
    }

    @Override
    public void close() {
        sparkSession.close();
        es.shutdown();
    }
}
