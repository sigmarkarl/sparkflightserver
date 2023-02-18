package org.simmi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.gson.reflect.TypeToken;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.CustomObjectsApi;
import io.kubernetes.client.openapi.models.V1DeleteOptions;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Watch;
import okhttp3.Call;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class SparkOperatorRunner {
    private static final Logger log = LoggerFactory.getLogger(SparkOperatorRunner.class);

    public static final String SPARKAPPLICATION_COMPLETED_STATE = "COMPLETED";
    public static final String SPARKAPPLICATION_FAILED_STATE = "FAILED";
    public static final String SPARKAPPLICATION_RUNNING_STATE = "RUNNING";

    ApiClient client;
    CustomObjectsApi apiInstance;
    CoreV1Api core;
    ObjectMapper objectMapper;
    String jobName;
    String namespace;
    boolean hostMount = false;
    SparkSession sparkSession;
    String securityContext;

    public SparkOperatorRunner() throws IOException {
        client = Config.defaultClient();
        Configuration.setDefaultApiClient(client);
        apiInstance = new CustomObjectsApi();
        core = new CoreV1Api(client);
        objectMapper = new ObjectMapper(new YAMLFactory());
    }

    Map<String, Object> loadBody(String query, String project, String result_dir, Map<String, Object> parameters) throws IOException {
        Map<String, Object> body;
        body = objectMapper.readValue(query, Map.class);
        Map<String, Object> metadata = (Map<String, Object>) body.get("metadata");
        jobName = metadata.get("name").toString();
        namespace = metadata.containsKey("namespace") ? metadata.get("namespace").toString() : "gorkube";

        if (body.containsKey("spec")) {
            Map<String, Object> specMap = (Map) body.get("spec");
            if (specMap.containsKey("arguments")) {
                List<String> arguments = (List) specMap.get("arguments");
                int i = arguments.indexOf("#{result_dir}");
                if (i != -1) arguments.set(i, result_dir);
            }
            if (specMap.containsKey("executor")) {
                Map<String, Object> executor = (Map) specMap.get("executor");
                Object cores = executor.get("cores");
                if (cores instanceof String) executor.put("cores", Integer.parseInt(cores.toString()));
                Object instances = executor.get("instances");
                if (instances instanceof String) executor.put("instances", Integer.parseInt(instances.toString()));
            }
        }

        return body;
    }

    public String getSparkApplicationState(String name) throws ApiException {
        try {
            Object obj = apiInstance.getNamespacedCustomObject("sparkoperator.k8s.io", "v1beta2", namespace, "sparkapplications", name);
            Map map = (Map)obj;
            Map statusMap = (Map)map.get("status");
            if(statusMap!=null) {
                Map appMap = (Map) statusMap.get("applicationState");
                return (String) appMap.get("state");
            }
        } catch (ApiException e) {
            if(!e.getMessage().contains("Not Found")) throw e;
        }
        return "";
    }

    public void deleteSparkApplication(String name) throws ApiException {
        V1DeleteOptions body = new V1DeleteOptions();
        apiInstance.deleteNamespacedCustomObject("sparkoperator.k8s.io", "v1beta2", namespace, "sparkapplications", name, null, null, null, null, body);
    }

    public boolean waitForSparkApplicationToComplete(String name) throws ApiException, InterruptedException {
        String state = getSparkApplicationState(name);
        while (!state.equals(SPARKAPPLICATION_COMPLETED_STATE)) {
            if (state.equals(SPARKAPPLICATION_FAILED_STATE)) {
                throw new RuntimeException(state, null);
            }
            Thread.sleep(1000);
            state = getSparkApplicationState(name);
        }
        return true;
    }

    void waitSparkApplicationState(String name,String state) throws ApiException {
        Call call = core.listNamespacedPodCall(namespace, null,null, null, null, null, null, null, null, 120, null, null);
        try (Watch<V1Pod> watchSparkApplication = Watch.createWatch(client, call, new TypeToken<Watch.Response<V1Pod>>() {}.getType())) {
            for (Watch.Response<V1Pod> item : watchSparkApplication) {
                if (item.type != null && item.type.equals("MODIFIED") && item.object != null && item.object.getStatus() != null) {
                    String phase = item.object.getStatus().getPhase();
                    if (state.equals(phase)) {
                        break;
                    } else if ("Failed".equals(phase) || "Error".equals(phase)) {
                        throw new RuntimeException(item.object.toString(), null);
                    }
                }
            }
        } catch (Exception e) {
            // Ignore watch errors
            log.error(e.getMessage(), e);
        }
    }

    public void createSparkApplicationFromJson(String json) throws ApiException, JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        createSparkApplication(mapper, json);
    }

    public void createSparkApplicationFromYaml(String yaml) throws ApiException, JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        createSparkApplication(mapper, yaml);
    }

    public void createSparkApplication(ObjectMapper mapper, String contents) throws ApiException, JsonProcessingException {
        Object jsonObject = mapper.readValue(contents, Object.class);
        apiInstance.createNamespacedCustomObject("sparkoperator.k8s.io", "v1beta2", namespace, "sparkapplications", jsonObject, "true", null, null);
    }

    public static String getSparkOperatorYaml(String projectDir) throws IOException {
        String json = null;
        Path p = Paths.get(projectDir);
        if (Files.exists(p)) {
            Path so_json = p.resolve("config/sparkoperator.yaml");
            if (Files.exists(so_json)) json = new String(Files.readAllBytes(so_json));
        }
        return json;
    }

    public void runQueryHandler(String appName, String uristr, String requestId, Path projectDir, String[] commands, String[] fingerprints, String[] jobIds, String[] cacheFiles, String[] resources) throws IOException, ApiException, InterruptedException {
        String[] args = new String[] {uristr,requestId,projectDir.toString(),String.join(";;",commands),String.join(";",fingerprints),String.join(";",cacheFiles),String.join(";",jobIds)};
        runSparkOperator(appName, projectDir, args, resources);
    }

    public void runSparkOperator(String sparkApplicationName, Path projectDir, String[] args, String[] resources) throws IOException, ApiException, InterruptedException {
        var sparkOperatorSpecs = new SparkOperatorSpecs();
        sparkOperatorSpecs.addConfig("spec.arguments", Arrays.asList(args));
        sparkOperatorSpecs.addConfig("metadata.name", sparkApplicationName);

        for (String config : resources) {
            String[] confSplit = config.split("=");
            try {
                Integer ii = Integer.parseInt(confSplit[1]);
                sparkOperatorSpecs.addConfig(confSplit[0], ii);
            } catch (NumberFormatException ne) {
                sparkOperatorSpecs.addConfig(confSplit[0], confSplit[1]);
            }
        }

        String yaml = getSparkOperatorYaml(projectDir.toString());
        runJob(sparkSession, yaml, projectDir.toString(), sparkOperatorSpecs, sparkApplicationName, args);
    }

    public void runJob(SparkSession sparkSession, String yaml, String projectDir, SparkOperatorSpecs sparkOperatorSpecs, String sparkApplicationName, String[] args) throws InterruptedException, ApiException, IOException {
        runYaml(yaml, projectDir, sparkOperatorSpecs);
        waitForSparkApplicationToComplete(sparkApplicationName);
    }

    public void runYaml(String yaml, String projectroot, SparkOperatorSpecs specs) throws IOException, ApiException {
        if (projectroot == null || projectroot.length() == 0)
            projectroot = Paths.get(".").toAbsolutePath().normalize().toString();
        Map<String, Object> body = loadBody(yaml, projectroot, "", new HashMap<>());
        specs.apply(body);
        apiInstance.createNamespacedCustomObject("sparkoperator.k8s.io", "v1beta2", namespace, "sparkapplications", body, "true", null, null);
    }
}
