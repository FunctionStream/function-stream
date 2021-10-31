package org.functionstream.fsflow.service.impl;

import static java.util.Map.entry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.gson.Gson;
import io.functionmesh.compute.mesh.models.V1alpha1FunctionMesh;
import io.functionmesh.compute.mesh.models.V1alpha1FunctionMeshSpecPodResources;
import io.functionmesh.compute.mesh.models.V1alpha1FunctionMeshSpecPulsar;
import io.kubernetes.client.util.Yaml;
import java.io.IOException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.functionstream.fsflow.entity.DeployDefinitionEntity;
import org.functionstream.fsflow.service.DeployDefinitionService;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class DeployDefinitionServiceImpl implements DeployDefinitionService {
    private static final String API_VERSION = "compute.functionmesh.io/v1alpha1";
    private static final String KIND = "FunctionMesh";
    private static final String VERSION = "v0.1";

    private final Gson gson = new Gson();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final YAMLMapper yamlMapper = new YAMLMapper();

    @Override
    public synchronized V1alpha1FunctionMesh getFunctionMeshCRD(String name, DeployDefinitionEntity deployDefinition)
            throws IOException {

        deployDefinition.setApiVersion(API_VERSION);
        deployDefinition.setKind(KIND);

        if (deployDefinition.getMetadata() == null) {
            deployDefinition.setMetadata(new DeployDefinitionEntity.Metadata());
        }

        deployDefinition.getMetadata().setName(name);

        if (deployDefinition.getVersion() != null && !VERSION.equals(deployDefinition.getVersion())) {
            throw new IllegalArgumentException(
                    "The deploy definition version is not compatible with current implementation.");
        }

        // The version field must be removed before converting to the k8s crd.
        deployDefinition.setVersion(null);

        // TODO: function checker logic

        // TODO: warp the exception
        String deployDefinitionYaml = jsonToYaml(gson.toJson(deployDefinition));

        V1alpha1FunctionMesh functionMesh = (V1alpha1FunctionMesh) Yaml.load(deployDefinitionYaml);

        if (functionMesh.getSpec() != null && functionMesh.getSpec().getFunctions() != null) {
            functionMesh.getSpec().getFunctions().forEach((function) -> {
                if (function.getResources() == null) {
                    function.setResources(new V1alpha1FunctionMeshSpecPodResources());
                }
                function.getResources().setLimits(Map.ofEntries(
                        entry("cpu", "0.2"),
                        entry("memory", "200M")
                ));
                if (function.getPulsar() == null) {
                    function.setPulsar(new V1alpha1FunctionMeshSpecPulsar());
                }
                // Set config for the function to use the default config.
                function.getPulsar().setPulsarConfig("pulsar-function-config");
            });
        }

        return functionMesh;
    }

    private String jsonToYaml(String jsonString) throws JsonProcessingException {
        return yamlMapper.writeValueAsString(objectMapper.readTree(jsonString));
    }
}
