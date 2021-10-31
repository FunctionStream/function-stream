package org.functionstream.fsflow.service;

import io.functionmesh.compute.mesh.models.V1alpha1FunctionMesh;
import io.kubernetes.client.openapi.ApiException;
import java.io.IOException;
import okhttp3.Response;

/**
 * The KubernetesService is responsible for interacting directly with the Kubernetes cluster and for managing the
 * resources deployed on the Kubernetes cluster.
 * This service is not responsible for performing security checks on incoming CRDs; these should be performed in the
 * {@link DeployDefinitionService}.
 * The name of a function mesh is globally unique within a single namespace.
 * Since a function flow corresponds to a single function mesh, the names of the function flow and the function mesh are
 * the same
 */
public interface KubernetesService {
    /**
     * Deploy the function mesh CRD to the kubernetes cluster
     *
     * @param namespace    The namespace in the cluster
     * @param functionMesh The CRD of the function mesh
     * @return Response
     */
    Response createFunctionMesh(String namespace, V1alpha1FunctionMesh functionMesh) throws ApiException, IOException;

    /**
     * Update or replace the function mesh in the kubernetes cluster
     *
     * @param namespace    The namespace in the cluster
     * @param name         The name of the function flow/function mesh
     * @param functionMesh The CRD of the function mesh
     * @return Response
     */
    Response replaceFunctionMesh(String namespace, String name, V1alpha1FunctionMesh functionMesh)
            throws IOException, ApiException;

    /**
     * Delete the function mesh
     *
     * @param namespace The namespace in the cluster
     * @param name      The name of the function flow/function mesh
     * @return Response
     */
    Response deleteFunctionMesh(String namespace, String name) throws IOException, ApiException;
}
