package org.simmi;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.kubernetes.client.openapi.ApiException;

import java.io.IOException;
import java.util.Map;

public class SparkAppLauncher {
    SparkOperatorRunner sparkOperatorRunner;

    public SparkAppLauncher() throws IOException {
        this.sparkOperatorRunner = new SparkOperatorRunner();
    }

    public void launchSparkApplicationCRD(String yaml) throws JsonProcessingException, ApiException {
        sparkOperatorRunner.createSparkApplicationFromYaml(yaml);
    }
}
