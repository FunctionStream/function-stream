package org.functionstream.fsflow.service;

import io.functionmesh.compute.mesh.models.V1alpha1FunctionMeshSpecFunctions;
import io.kubernetes.client.util.Yaml;
import org.junit.Assert;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.gson.Gson;
import io.functionmesh.compute.mesh.models.V1alpha1FunctionMesh;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.functionstream.fsflow.entity.DeployDefinitionEntity;
import org.functionstream.fsflow.service.impl.DeployDefinitionServiceImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.json.YamlJsonParser;

@Slf4j
public class DeployDefinitionServiceImplTest {

    @Test
    public void testDeployDefinitionJson() throws IOException {
        // This line must be added during unit tests.
        Yaml.addModelMap("compute.functionmesh.io/v1alpha1", "FunctionMesh", V1alpha1FunctionMesh.class);

        DeployDefinitionService service = new DeployDefinitionServiceImpl();

        File src = new File("src/test/resources/test-DD.json");
        String name = "test";
        Gson gson = new Gson();

        DeployDefinitionEntity deployDefinition = gson.fromJson(new FileReader(src),
                DeployDefinitionEntity.class);

        V1alpha1FunctionMesh crd = service.getFunctionMeshCRD(name, deployDefinition);

        assert crd.getMetadata() != null;
        Assertions.assertEquals(name, crd.getMetadata().getName());
        assert crd.getSpec() != null;
        assert crd.getSpec().getFunctions() != null;
        Assertions.assertEquals(2, crd.getSpec().getFunctions().size());
        assertFunction(crd.getSpec().getFunctions().get(0), "test-ex", "persistent://public/default/a1",
                "persistent://public/default/a2");
        assertFunction(crd.getSpec().getFunctions().get(1), "test-ex2", "persistent://public/default/a2",
                "persistent://public/default/a3");
    }

    private void assertFunction(V1alpha1FunctionMeshSpecFunctions function, String name, String inputTopic,
                                String outputTopic) {
        Assertions.assertEquals("streamnative/pulsar-functions-go-sample:2.8.1", function.getImage());
        Assertions.assertEquals(name, function.getName());
        Assertions.assertEquals(true, function.getAutoAck());
        Assertions.assertEquals("exclamation_function.ExclamationFunction", function.getClassName());
        Assertions.assertEquals(true, function.getForwardSourceMessageProperty());
        Assertions.assertEquals(1000, function.getMaxPendingAsyncRequests());
        Assertions.assertEquals(1, function.getReplicas());
        Assertions.assertEquals(5, function.getMaxReplicas());
        Assertions.assertEquals("persistent://public/default/logging-function-logs", function.getLogTopic());
        assert function.getInput() != null;
        assert function.getInput().getTopics() != null;
        Assertions.assertEquals(1, function.getInput().getTopics().size());
        Assertions.assertEquals(inputTopic, function.getInput().getTopics().get(0));
        Assertions.assertEquals("java.lang.String", function.getInput().getTypeClassName());
        assert function.getOutput() != null;
        Assertions.assertEquals(outputTopic, function.getOutput().getTopic());
        Assertions.assertEquals("java.lang.String", function.getOutput().getTypeClassName());
        assert function.getPulsar() != null;
        Assertions.assertEquals("mesh-test-pulsar", function.getPulsar().getPulsarConfig());
        assert function.getResources() != null;
        assert function.getResources().getRequests() != null;
        Assertions.assertEquals("0.1", function.getResources().getRequests().get("cpu"));
        Assertions.assertEquals("10M", function.getResources().getRequests().get("memory"));
        assert function.getResources().getLimits() != null;
        Assertions.assertEquals("0.2", function.getResources().getLimits().get("cpu"));
        Assertions.assertEquals("200M", function.getResources().getLimits().get("memory"));
        assert function.getGolang() != null;
        Assertions.assertEquals("/pulsar/examples/go-exclamation-func", function.getGolang().getGo());
    }
}
