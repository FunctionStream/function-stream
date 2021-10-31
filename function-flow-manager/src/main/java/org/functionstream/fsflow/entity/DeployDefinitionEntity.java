package org.functionstream.fsflow.entity;

import com.google.gson.JsonObject;
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class DeployDefinitionEntity {
    public String apiVersion;
    public String kind;
    public String version;

    @Getter
    @Setter
    @NoArgsConstructor
    public static class Metadata {
        public String name;
    }

    public DeployDefinitionEntity.Metadata metadata;

    @Getter
    @Setter
    @NoArgsConstructor
    private static class Spec {
        public List<JsonObject> functions;
    }

    public DeployDefinitionEntity.Spec spec;
}
