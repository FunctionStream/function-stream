package org.functionstream.fsflow.entity;

import com.google.gson.JsonObject;
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * The deployment definition entity obtained from the front end.
 */
@Getter
@Setter
@NoArgsConstructor
public class DeployDefinitionEntity {
    public String apiVersion;
    public String kind;
    public String version;
    public DeployDefinitionEntity.Metadata metadata;
    public DeployDefinitionEntity.Spec spec;

    @Getter
    @Setter
    @NoArgsConstructor
    public static class Metadata {
        public String name;
    }

    @Getter
    @Setter
    @NoArgsConstructor
    private static class Spec {
        public List<JsonObject> functions;
    }
}
