package org.functionstream.fsflow.service.definition;

import java.util.List;

public class FlowDeployDefinition {
    public String name;
    public List<Function> functions;

    public static class Function {
        public String name;
        public String image;
        public String className;
        public int replicas;
        public int maxReplicas;
    }
}
