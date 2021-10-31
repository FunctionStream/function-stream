package org.functionstream.fsflow.controller;


import com.google.common.collect.Maps;
import io.functionmesh.compute.mesh.models.V1alpha1FunctionMesh;
import io.kubernetes.client.openapi.ApiException;
import io.swagger.annotations.Api;
import java.io.IOException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Response;
import org.functionstream.fsflow.entity.DeployDefinitionEntity;
import org.functionstream.fsflow.service.DeployDefinitionService;
import org.functionstream.fsflow.service.KubernetesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping(value = "/flow")
@Api(value = "Function flow management")
public class FunctionFlowController {
    @Value("${flow.k8s.namespace}")
    private String k8sNamespace;

    private final DeployDefinitionService deployDefinitionService;
    private final KubernetesService kubernetesService;

    @Autowired
    public FunctionFlowController(DeployDefinitionService deployDefinitionService,
                                  KubernetesService kubernetesService) {
        this.deployDefinitionService = deployDefinitionService;
        this.kubernetesService = kubernetesService;
    }

    private V1alpha1FunctionMesh generateCRD(String name, DeployDefinitionEntity deployDefinitionEntity,
                                             Map<String, Object> result) {
        V1alpha1FunctionMesh crd = null;
        try {
            crd = deployDefinitionService.getFunctionMeshCRD(name, deployDefinitionEntity);
            assert crd != null;
        } catch (IOException e) {
            log.error("Failed to generate function mesh crd: ", e);
            result.put("error", "Failed to parse definition, please check the definition.");
            ResponseEntity.ok(result);
        }
        return crd;
    }

    @RequestMapping(value = "/deploy/{name}", method = RequestMethod.PUT)
    public ResponseEntity<Map<String, Object>> deployFlow(
            @PathVariable String name,
            @RequestBody DeployDefinitionEntity deployDefinition) {
        Map<String, Object> result = Maps.newHashMap();
        V1alpha1FunctionMesh crd = generateCRD(name, deployDefinition, result);

        try {
            Response k8sResponse = kubernetesService.createFunctionMesh(k8sNamespace, crd);
            if (!k8sResponse.isSuccessful()) {
                result.put("error", "Failed to deploy function flow to k8s: " + k8sResponse.message());
            }
        } catch (IOException | ApiException e) {
            log.error("Failed to deploy function mesh: ", e);
            result.put("error", "Failed to deploy function flow: " + e);
            ResponseEntity.ok(result);
        }

        result.put("message", "Deploy function flow success.");
        return ResponseEntity.ok(result);
    }

    @RequestMapping(value = "/deploy/{name}", method = RequestMethod.PATCH)
    public ResponseEntity<Map<String, Object>> updateFlow(
            @PathVariable String name,
            @RequestBody DeployDefinitionEntity deployDefinition) {
        Map<String, Object> result = Maps.newHashMap();
        V1alpha1FunctionMesh crd = generateCRD(name, deployDefinition, result);

        try {
            Response k8sResponse = kubernetesService.replaceFunctionMesh(k8sNamespace, name, crd);
            if (!k8sResponse.isSuccessful()) {
                result.put("error", "Failed to update function flow to k8s: " + k8sResponse.message());
            }
        } catch (IOException | ApiException e) {
            log.error("Failed to update function mesh: ", e);
            result.put("error", "Failed to update function flow: " + e);
            ResponseEntity.ok(result);
        }

        result.put("message", "Update function flow success.");
        return ResponseEntity.ok(result);
    }

    @RequestMapping(value = "/deploy/{name}", method = RequestMethod.DELETE)
    public ResponseEntity<Map<String, Object>> deleteFlow(
            @PathVariable String name) {
        Map<String, Object> result = Maps.newHashMap();

        try {
            Response k8sResponse = kubernetesService.deleteFunctionMesh(k8sNamespace, name);
            if (!k8sResponse.isSuccessful()) {
                result.put("error", "Failed to delete function flow to k8s: " + k8sResponse.message());
            }
        } catch (IOException | ApiException e) {
            log.error("Failed to delete function mesh: ", e);
            result.put("error", "Failed to delete function flow: " + e);
            ResponseEntity.ok(result);
        }

        result.put("message", "Delete function flow success.");
        return ResponseEntity.ok(result);
    }
}
