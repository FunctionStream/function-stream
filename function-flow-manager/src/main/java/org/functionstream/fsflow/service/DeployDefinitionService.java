package org.functionstream.fsflow.service;

import io.functionmesh.compute.mesh.models.V1alpha1FunctionMesh;
import java.io.IOException;
import org.functionstream.fsflow.entity.DeployDefinitionEntity;

/**
 * DeployDefinitionService is responsible for converting DeployDefinitionEntity into V1alpha1FunctionMesh CRD.
 * During the conversion process, the incoming entity is checked for various parameters to ensure that the CRD provided
 * to Kubernetes cluster is generated correctly.
 */
public interface DeployDefinitionService {
    /**
     * Convert DeployDefinitionEntity into V1alpha1FunctionMesh CRD.
     *
     * @param name             CRD name. This name will be set to the metadata and will be overwritten if the name
     *                         exists in the metadata of the incoming DeployDefinitionEntity.
     * @param deployDefinition The deployment definition entity obtained from the front-end
     * @return Function Mesh CRD used to deploy to the kubernetes clusters
     * @throws IOException This exception is thrown if there is a problem with json and yaml coding and decoding.
     */
    V1alpha1FunctionMesh getFunctionMeshCRD(String name, DeployDefinitionEntity deployDefinition) throws IOException;
}
