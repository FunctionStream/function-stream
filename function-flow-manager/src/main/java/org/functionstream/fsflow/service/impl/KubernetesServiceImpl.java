package org.functionstream.fsflow.service.impl;

import io.functionmesh.compute.mesh.models.V1alpha1FunctionMesh;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CustomObjectsApi;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Yaml;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import okhttp3.Response;
import org.functionstream.fsflow.service.KubernetesService;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KubernetesServiceImpl implements KubernetesService {
    private final CustomObjectsApi api;
    private static final String PLURAL = "functionmeshes";
    private static final String GROUP = "compute.functionmesh.io";
    private static final String VERSION = "v1alpha1";

    public KubernetesServiceImpl() throws IOException {
        api = new CustomObjectsApi(Config.defaultClient());
        // This line must be added during debugging.
        Yaml.addModelMap("compute.functionmesh.io/v1alpha1", "FunctionMesh", V1alpha1FunctionMesh.class);
    }

    @Override
    public Response createFunctionMesh(String namespace, V1alpha1FunctionMesh functionMesh)
            throws ApiException, IOException {
        Call call = api.createNamespacedCustomObjectCall(
                GROUP,
                VERSION,
                namespace,
                PLURAL,
                functionMesh,
                null,
                null,
                null,
                null
        );
        return call.execute();
    }

    @Override
    public Response replaceFunctionMesh(String namespace, String name, V1alpha1FunctionMesh functionMesh)
            throws IOException, ApiException {
        Call call = api.replaceNamespacedCustomObjectCall(
                GROUP,
                VERSION,
                namespace,
                PLURAL,
                name,
                functionMesh,
                null,
                null,
                null
        );
        return call.execute();
    }

    @Override
    public Response deleteFunctionMesh(String namespace, String name) throws IOException, ApiException {
        Call call = api.deleteNamespacedCustomObjectCall(
                GROUP,
                VERSION,
                namespace,
                PLURAL,
                name,
                null,
                null,
                null,
                null,
                null,
                null
        );
        return call.execute();
    }
}
