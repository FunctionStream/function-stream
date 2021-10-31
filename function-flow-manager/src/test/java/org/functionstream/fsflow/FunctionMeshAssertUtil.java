package org.functionstream.fsflow;

import io.functionmesh.compute.mesh.models.V1alpha1FunctionMesh;
import io.functionmesh.compute.mesh.models.V1alpha1FunctionMeshSpecFunctions;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;

@Slf4j
public class FunctionMeshAssertUtil {
    public static void assertFunctionMesh(V1alpha1FunctionMesh functionMesh, String name) {
        assert functionMesh.getMetadata() != null;
        Assertions.assertEquals(name, functionMesh.getMetadata().getName());
        assert functionMesh.getSpec() != null;
        assert functionMesh.getSpec().getFunctions() != null;
        Assertions.assertEquals(2, functionMesh.getSpec().getFunctions().size());
        assertFunction(functionMesh.getSpec().getFunctions().get(0), "test-ex", "persistent://public/default/a1",
                "persistent://public/default/a2");
        assertFunction(functionMesh.getSpec().getFunctions().get(1), "test-ex2", "persistent://public/default/a2",
                "persistent://public/default/a3");
    }

    public static void assertFunction(V1alpha1FunctionMeshSpecFunctions function, String name, String inputTopic,
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
        Assertions.assertEquals("pulsar-function-config", function.getPulsar().getPulsarConfig());
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
