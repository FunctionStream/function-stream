package org.functionstream.fsflow.service;

import io.functionmesh.compute.mesh.models.V1alpha1FunctionMesh;
import io.kubernetes.client.openapi.ApiException;
import java.io.IOException;
import okhttp3.Response;

public interface KubernetesService {
    Response createFunctionMesh(String namespace, V1alpha1FunctionMesh functionMesh) throws ApiException, IOException;
    Response replaceFunctionMesh(String namespace, String name, V1alpha1FunctionMesh functionMesh)
            throws IOException, ApiException;
    Response deleteFunctionMesh(String namespace, String name) throws IOException, ApiException;
}
