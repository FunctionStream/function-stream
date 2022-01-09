package org.functionstream.fsflow.controller;

import com.google.gson.Gson;
import io.functionmesh.compute.mesh.models.V1alpha1FunctionMesh;
import io.kubernetes.client.util.Yaml;
import java.io.File;
import java.io.FileReader;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import org.functionstream.fsflow.FunctionMeshAssertUtil;
import org.functionstream.fsflow.entity.DeployDefinitionEntity;
import org.functionstream.fsflow.service.KubernetesService;
import org.functionstream.fsflow.service.impl.DeployDefinitionServiceImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;

@Slf4j
public class FunctionFlowControllerTest {
    @Value("${flow.k8s.namespace}")
    private String k8sNamespace;
    private final Response.Builder successfulResponseBuilder =
            new Response.Builder()
                    .request(new Request.Builder().url("http://test").build())
                    .protocol(Protocol.HTTP_2)
                    .message("test message")
                    .code(200);

    private final KubernetesService mockKubernetesService = new KubernetesService() {
        @Override
        public Response createFunctionMesh(String namespace, V1alpha1FunctionMesh functionMesh) {
            log.info("Create function mesh: {}", namespace);
            Assertions.assertEquals(k8sNamespace, namespace);
            FunctionMeshAssertUtil.assertFunctionMesh(functionMesh, "test");
            return successfulResponseBuilder.build();
        }

        @Override
        public Response replaceFunctionMesh(String namespace, String name, V1alpha1FunctionMesh functionMesh) {
            log.info("Replace function mesh: {}, {}", namespace, name);
            Assertions.assertEquals(k8sNamespace, namespace);
            Assertions.assertEquals("test", name);
            FunctionMeshAssertUtil.assertFunctionMesh(functionMesh, "test");
            return successfulResponseBuilder.build();
        }

        @Override
        public Response deleteFunctionMesh(String namespace, String name) {
            log.info("Delete function mesh: {}, {}", namespace, name);
            Assertions.assertEquals(k8sNamespace, namespace);
            Assertions.assertEquals("test", name);
            return successfulResponseBuilder.build();
        }
    };

    @BeforeEach
    public void setup() {
        // This line must be added during unit tests.
        Yaml.addModelMap("compute.functionmesh.io/v1alpha1", "FunctionMesh", V1alpha1FunctionMesh.class);
    }

    private DeployDefinitionEntity getTestDeployDefinition() {
        File src = new File("src/test/resources/test-DD.json");
        String name = "test";
        Gson gson = new Gson();

        try {
            DeployDefinitionEntity deployDefinition = gson.fromJson(new FileReader(src),
                    DeployDefinitionEntity.class);

            return deployDefinition;
        } catch (Exception e) {
            Assertions.fail(e);
        }
        Assertions.fail("Unexpected null.");
        return null;
    }

    @Test
    public void testDeployFlow() {
        DeployDefinitionEntity deployDefinition = getTestDeployDefinition();

        FunctionFlowController functionFlowController =
                new FunctionFlowController(new DeployDefinitionServiceImpl(), mockKubernetesService);

        ResponseEntity<Map<String, Object>> responseEntity =
                functionFlowController.deployFlow("test", deployDefinition);
        Assertions.assertNotNull(responseEntity.getBody());
        Assertions.assertNotNull(responseEntity.getBody().get("message"));
        Assertions.assertNull(responseEntity.getBody().get("error"));
    }

    @Test
    public void testUpdateFlow() {
        DeployDefinitionEntity deployDefinition = getTestDeployDefinition();

        FunctionFlowController functionFlowController =
                new FunctionFlowController(new DeployDefinitionServiceImpl(), mockKubernetesService);

        ResponseEntity<Map<String, Object>> responseEntity =
                functionFlowController.updateFlow("test", deployDefinition);
        Assertions.assertNotNull(responseEntity.getBody());
        Assertions.assertNotNull(responseEntity.getBody().get("message"));
        Assertions.assertNull(responseEntity.getBody().get("error"));
    }

    @Test
    public void testDeleteFlow() {
        FunctionFlowController functionFlowController =
                new FunctionFlowController(new DeployDefinitionServiceImpl(), mockKubernetesService);

        ResponseEntity<Map<String, Object>> responseEntity =
                functionFlowController.deleteFlow("test");
        Assertions.assertNotNull(responseEntity.getBody());
        Assertions.assertNotNull(responseEntity.getBody().get("message"));
        Assertions.assertNull(responseEntity.getBody().get("error"));
    }
}
