package org.functionstream.fsflow.service;

import io.kubernetes.client.util.Yaml;
import com.google.gson.Gson;
import io.functionmesh.compute.mesh.models.V1alpha1FunctionMesh;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.functionstream.fsflow.FunctionMeshAssertUtil;
import org.functionstream.fsflow.entity.DeployDefinitionEntity;
import org.functionstream.fsflow.service.impl.DeployDefinitionServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
public class DeployDefinitionServiceImplTest {
    @BeforeEach
    public void setup() {
        // This line must be added during unit tests.
        Yaml.addModelMap("compute.functionmesh.io/v1alpha1", "FunctionMesh", V1alpha1FunctionMesh.class);
        log.info("Added model map.");
    }

    @Test
    public void testDeployDefinitionJson() throws IOException {
        DeployDefinitionService service = new DeployDefinitionServiceImpl();

        File src = new File("src/test/resources/test-DD.json");
        String name = "test";
        Gson gson = new Gson();

        DeployDefinitionEntity deployDefinition = gson.fromJson(new FileReader(src),
                DeployDefinitionEntity.class);

        V1alpha1FunctionMesh crd = service.getFunctionMeshCRD(name, deployDefinition);

        FunctionMeshAssertUtil.assertFunctionMesh(crd, name);
    }
}
