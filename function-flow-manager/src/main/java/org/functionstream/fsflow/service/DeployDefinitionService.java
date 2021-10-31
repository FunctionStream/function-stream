package org.functionstream.fsflow.service;

import io.functionmesh.compute.mesh.models.V1alpha1FunctionMesh;
import java.io.IOException;
import org.functionstream.fsflow.entity.DeployDefinitionEntity;

public interface DeployDefinitionService {
    V1alpha1FunctionMesh getFunctionMeshCRD(String name, DeployDefinitionEntity deployDefinition) throws IOException;
}
