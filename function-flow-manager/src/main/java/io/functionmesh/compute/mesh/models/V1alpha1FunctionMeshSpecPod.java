/*
 * Kubernetes
 * No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)
 *
 * The version of the OpenAPI document: v1.15.12
 * 
 *
 * NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).
 * https://openapi-generator.tech
 * Do not edit the class manually.
 */


package io.functionmesh.compute.mesh.models;

import java.util.Objects;
import java.util.Arrays;
import com.google.gson.TypeAdapter;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.functionmesh.compute.mesh.models.V1alpha1FunctionMeshSpecPodAffinity;
import io.functionmesh.compute.mesh.models.V1alpha1FunctionMeshSpecPodAutoScalingBehavior;
import io.functionmesh.compute.mesh.models.V1alpha1FunctionMeshSpecPodAutoScalingMetrics;
import io.functionmesh.compute.mesh.models.V1alpha1FunctionMeshSpecPodImagePullSecrets;
import io.functionmesh.compute.mesh.models.V1alpha1FunctionMeshSpecPodInitContainers;
import io.functionmesh.compute.mesh.models.V1alpha1FunctionMeshSpecPodSecurityContext1;
import io.functionmesh.compute.mesh.models.V1alpha1FunctionMeshSpecPodTolerations;
import io.functionmesh.compute.mesh.models.V1alpha1FunctionMeshSpecPodVolumes;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * V1alpha1FunctionMeshSpecPod
 */
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", date = "2021-10-07T12:35:29.394Z[Etc/UTC]")
public class V1alpha1FunctionMeshSpecPod {
  public static final String SERIALIZED_NAME_AFFINITY = "affinity";
  @SerializedName(SERIALIZED_NAME_AFFINITY)
  private V1alpha1FunctionMeshSpecPodAffinity affinity;

  public static final String SERIALIZED_NAME_ANNOTATIONS = "annotations";
  @SerializedName(SERIALIZED_NAME_ANNOTATIONS)
  private Map<String, String> annotations = null;

  public static final String SERIALIZED_NAME_AUTO_SCALING_BEHAVIOR = "autoScalingBehavior";
  @SerializedName(SERIALIZED_NAME_AUTO_SCALING_BEHAVIOR)
  private V1alpha1FunctionMeshSpecPodAutoScalingBehavior autoScalingBehavior;

  public static final String SERIALIZED_NAME_AUTO_SCALING_METRICS = "autoScalingMetrics";
  @SerializedName(SERIALIZED_NAME_AUTO_SCALING_METRICS)
  private List<V1alpha1FunctionMeshSpecPodAutoScalingMetrics> autoScalingMetrics = null;

  public static final String SERIALIZED_NAME_BUILTIN_AUTOSCALER = "builtinAutoscaler";
  @SerializedName(SERIALIZED_NAME_BUILTIN_AUTOSCALER)
  private List<String> builtinAutoscaler = null;

  public static final String SERIALIZED_NAME_IMAGE_PULL_SECRETS = "imagePullSecrets";
  @SerializedName(SERIALIZED_NAME_IMAGE_PULL_SECRETS)
  private List<V1alpha1FunctionMeshSpecPodImagePullSecrets> imagePullSecrets = null;

  public static final String SERIALIZED_NAME_INIT_CONTAINERS = "initContainers";
  @SerializedName(SERIALIZED_NAME_INIT_CONTAINERS)
  private List<V1alpha1FunctionMeshSpecPodInitContainers> initContainers = null;

  public static final String SERIALIZED_NAME_LABELS = "labels";
  @SerializedName(SERIALIZED_NAME_LABELS)
  private Map<String, String> labels = null;

  public static final String SERIALIZED_NAME_NODE_SELECTOR = "nodeSelector";
  @SerializedName(SERIALIZED_NAME_NODE_SELECTOR)
  private Map<String, String> nodeSelector = null;

  public static final String SERIALIZED_NAME_SECURITY_CONTEXT = "securityContext";
  @SerializedName(SERIALIZED_NAME_SECURITY_CONTEXT)
  private V1alpha1FunctionMeshSpecPodSecurityContext1 securityContext;

  public static final String SERIALIZED_NAME_SERVICE_ACCOUNT_NAME = "serviceAccountName";
  @SerializedName(SERIALIZED_NAME_SERVICE_ACCOUNT_NAME)
  private String serviceAccountName;

  public static final String SERIALIZED_NAME_SIDECARS = "sidecars";
  @SerializedName(SERIALIZED_NAME_SIDECARS)
  private List<V1alpha1FunctionMeshSpecPodInitContainers> sidecars = null;

  public static final String SERIALIZED_NAME_TERMINATION_GRACE_PERIOD_SECONDS = "terminationGracePeriodSeconds";
  @SerializedName(SERIALIZED_NAME_TERMINATION_GRACE_PERIOD_SECONDS)
  private Long terminationGracePeriodSeconds;

  public static final String SERIALIZED_NAME_TOLERATIONS = "tolerations";
  @SerializedName(SERIALIZED_NAME_TOLERATIONS)
  private List<V1alpha1FunctionMeshSpecPodTolerations> tolerations = null;

  public static final String SERIALIZED_NAME_VOLUMES = "volumes";
  @SerializedName(SERIALIZED_NAME_VOLUMES)
  private List<V1alpha1FunctionMeshSpecPodVolumes> volumes = null;


  public V1alpha1FunctionMeshSpecPod affinity(V1alpha1FunctionMeshSpecPodAffinity affinity) {
    
    this.affinity = affinity;
    return this;
  }

   /**
   * Get affinity
   * @return affinity
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public V1alpha1FunctionMeshSpecPodAffinity getAffinity() {
    return affinity;
  }


  public void setAffinity(V1alpha1FunctionMeshSpecPodAffinity affinity) {
    this.affinity = affinity;
  }


  public V1alpha1FunctionMeshSpecPod annotations(Map<String, String> annotations) {
    
    this.annotations = annotations;
    return this;
  }

  public V1alpha1FunctionMeshSpecPod putAnnotationsItem(String key, String annotationsItem) {
    if (this.annotations == null) {
      this.annotations = new HashMap<>();
    }
    this.annotations.put(key, annotationsItem);
    return this;
  }

   /**
   * Get annotations
   * @return annotations
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public Map<String, String> getAnnotations() {
    return annotations;
  }


  public void setAnnotations(Map<String, String> annotations) {
    this.annotations = annotations;
  }


  public V1alpha1FunctionMeshSpecPod autoScalingBehavior(V1alpha1FunctionMeshSpecPodAutoScalingBehavior autoScalingBehavior) {
    
    this.autoScalingBehavior = autoScalingBehavior;
    return this;
  }

   /**
   * Get autoScalingBehavior
   * @return autoScalingBehavior
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public V1alpha1FunctionMeshSpecPodAutoScalingBehavior getAutoScalingBehavior() {
    return autoScalingBehavior;
  }


  public void setAutoScalingBehavior(V1alpha1FunctionMeshSpecPodAutoScalingBehavior autoScalingBehavior) {
    this.autoScalingBehavior = autoScalingBehavior;
  }


  public V1alpha1FunctionMeshSpecPod autoScalingMetrics(List<V1alpha1FunctionMeshSpecPodAutoScalingMetrics> autoScalingMetrics) {
    
    this.autoScalingMetrics = autoScalingMetrics;
    return this;
  }

  public V1alpha1FunctionMeshSpecPod addAutoScalingMetricsItem(V1alpha1FunctionMeshSpecPodAutoScalingMetrics autoScalingMetricsItem) {
    if (this.autoScalingMetrics == null) {
      this.autoScalingMetrics = new ArrayList<>();
    }
    this.autoScalingMetrics.add(autoScalingMetricsItem);
    return this;
  }

   /**
   * Get autoScalingMetrics
   * @return autoScalingMetrics
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public List<V1alpha1FunctionMeshSpecPodAutoScalingMetrics> getAutoScalingMetrics() {
    return autoScalingMetrics;
  }


  public void setAutoScalingMetrics(List<V1alpha1FunctionMeshSpecPodAutoScalingMetrics> autoScalingMetrics) {
    this.autoScalingMetrics = autoScalingMetrics;
  }


  public V1alpha1FunctionMeshSpecPod builtinAutoscaler(List<String> builtinAutoscaler) {
    
    this.builtinAutoscaler = builtinAutoscaler;
    return this;
  }

  public V1alpha1FunctionMeshSpecPod addBuiltinAutoscalerItem(String builtinAutoscalerItem) {
    if (this.builtinAutoscaler == null) {
      this.builtinAutoscaler = new ArrayList<>();
    }
    this.builtinAutoscaler.add(builtinAutoscalerItem);
    return this;
  }

   /**
   * Get builtinAutoscaler
   * @return builtinAutoscaler
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public List<String> getBuiltinAutoscaler() {
    return builtinAutoscaler;
  }


  public void setBuiltinAutoscaler(List<String> builtinAutoscaler) {
    this.builtinAutoscaler = builtinAutoscaler;
  }


  public V1alpha1FunctionMeshSpecPod imagePullSecrets(List<V1alpha1FunctionMeshSpecPodImagePullSecrets> imagePullSecrets) {
    
    this.imagePullSecrets = imagePullSecrets;
    return this;
  }

  public V1alpha1FunctionMeshSpecPod addImagePullSecretsItem(V1alpha1FunctionMeshSpecPodImagePullSecrets imagePullSecretsItem) {
    if (this.imagePullSecrets == null) {
      this.imagePullSecrets = new ArrayList<>();
    }
    this.imagePullSecrets.add(imagePullSecretsItem);
    return this;
  }

   /**
   * Get imagePullSecrets
   * @return imagePullSecrets
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public List<V1alpha1FunctionMeshSpecPodImagePullSecrets> getImagePullSecrets() {
    return imagePullSecrets;
  }


  public void setImagePullSecrets(List<V1alpha1FunctionMeshSpecPodImagePullSecrets> imagePullSecrets) {
    this.imagePullSecrets = imagePullSecrets;
  }


  public V1alpha1FunctionMeshSpecPod initContainers(List<V1alpha1FunctionMeshSpecPodInitContainers> initContainers) {
    
    this.initContainers = initContainers;
    return this;
  }

  public V1alpha1FunctionMeshSpecPod addInitContainersItem(V1alpha1FunctionMeshSpecPodInitContainers initContainersItem) {
    if (this.initContainers == null) {
      this.initContainers = new ArrayList<>();
    }
    this.initContainers.add(initContainersItem);
    return this;
  }

   /**
   * Get initContainers
   * @return initContainers
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public List<V1alpha1FunctionMeshSpecPodInitContainers> getInitContainers() {
    return initContainers;
  }


  public void setInitContainers(List<V1alpha1FunctionMeshSpecPodInitContainers> initContainers) {
    this.initContainers = initContainers;
  }


  public V1alpha1FunctionMeshSpecPod labels(Map<String, String> labels) {
    
    this.labels = labels;
    return this;
  }

  public V1alpha1FunctionMeshSpecPod putLabelsItem(String key, String labelsItem) {
    if (this.labels == null) {
      this.labels = new HashMap<>();
    }
    this.labels.put(key, labelsItem);
    return this;
  }

   /**
   * Get labels
   * @return labels
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public Map<String, String> getLabels() {
    return labels;
  }


  public void setLabels(Map<String, String> labels) {
    this.labels = labels;
  }


  public V1alpha1FunctionMeshSpecPod nodeSelector(Map<String, String> nodeSelector) {
    
    this.nodeSelector = nodeSelector;
    return this;
  }

  public V1alpha1FunctionMeshSpecPod putNodeSelectorItem(String key, String nodeSelectorItem) {
    if (this.nodeSelector == null) {
      this.nodeSelector = new HashMap<>();
    }
    this.nodeSelector.put(key, nodeSelectorItem);
    return this;
  }

   /**
   * Get nodeSelector
   * @return nodeSelector
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public Map<String, String> getNodeSelector() {
    return nodeSelector;
  }


  public void setNodeSelector(Map<String, String> nodeSelector) {
    this.nodeSelector = nodeSelector;
  }


  public V1alpha1FunctionMeshSpecPod securityContext(V1alpha1FunctionMeshSpecPodSecurityContext1 securityContext) {
    
    this.securityContext = securityContext;
    return this;
  }

   /**
   * Get securityContext
   * @return securityContext
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public V1alpha1FunctionMeshSpecPodSecurityContext1 getSecurityContext() {
    return securityContext;
  }


  public void setSecurityContext(V1alpha1FunctionMeshSpecPodSecurityContext1 securityContext) {
    this.securityContext = securityContext;
  }


  public V1alpha1FunctionMeshSpecPod serviceAccountName(String serviceAccountName) {
    
    this.serviceAccountName = serviceAccountName;
    return this;
  }

   /**
   * Get serviceAccountName
   * @return serviceAccountName
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public String getServiceAccountName() {
    return serviceAccountName;
  }


  public void setServiceAccountName(String serviceAccountName) {
    this.serviceAccountName = serviceAccountName;
  }


  public V1alpha1FunctionMeshSpecPod sidecars(List<V1alpha1FunctionMeshSpecPodInitContainers> sidecars) {
    
    this.sidecars = sidecars;
    return this;
  }

  public V1alpha1FunctionMeshSpecPod addSidecarsItem(V1alpha1FunctionMeshSpecPodInitContainers sidecarsItem) {
    if (this.sidecars == null) {
      this.sidecars = new ArrayList<>();
    }
    this.sidecars.add(sidecarsItem);
    return this;
  }

   /**
   * Get sidecars
   * @return sidecars
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public List<V1alpha1FunctionMeshSpecPodInitContainers> getSidecars() {
    return sidecars;
  }


  public void setSidecars(List<V1alpha1FunctionMeshSpecPodInitContainers> sidecars) {
    this.sidecars = sidecars;
  }


  public V1alpha1FunctionMeshSpecPod terminationGracePeriodSeconds(Long terminationGracePeriodSeconds) {
    
    this.terminationGracePeriodSeconds = terminationGracePeriodSeconds;
    return this;
  }

   /**
   * Get terminationGracePeriodSeconds
   * @return terminationGracePeriodSeconds
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public Long getTerminationGracePeriodSeconds() {
    return terminationGracePeriodSeconds;
  }


  public void setTerminationGracePeriodSeconds(Long terminationGracePeriodSeconds) {
    this.terminationGracePeriodSeconds = terminationGracePeriodSeconds;
  }


  public V1alpha1FunctionMeshSpecPod tolerations(List<V1alpha1FunctionMeshSpecPodTolerations> tolerations) {
    
    this.tolerations = tolerations;
    return this;
  }

  public V1alpha1FunctionMeshSpecPod addTolerationsItem(V1alpha1FunctionMeshSpecPodTolerations tolerationsItem) {
    if (this.tolerations == null) {
      this.tolerations = new ArrayList<>();
    }
    this.tolerations.add(tolerationsItem);
    return this;
  }

   /**
   * Get tolerations
   * @return tolerations
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public List<V1alpha1FunctionMeshSpecPodTolerations> getTolerations() {
    return tolerations;
  }


  public void setTolerations(List<V1alpha1FunctionMeshSpecPodTolerations> tolerations) {
    this.tolerations = tolerations;
  }


  public V1alpha1FunctionMeshSpecPod volumes(List<V1alpha1FunctionMeshSpecPodVolumes> volumes) {
    
    this.volumes = volumes;
    return this;
  }

  public V1alpha1FunctionMeshSpecPod addVolumesItem(V1alpha1FunctionMeshSpecPodVolumes volumesItem) {
    if (this.volumes == null) {
      this.volumes = new ArrayList<>();
    }
    this.volumes.add(volumesItem);
    return this;
  }

   /**
   * Get volumes
   * @return volumes
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public List<V1alpha1FunctionMeshSpecPodVolumes> getVolumes() {
    return volumes;
  }


  public void setVolumes(List<V1alpha1FunctionMeshSpecPodVolumes> volumes) {
    this.volumes = volumes;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    V1alpha1FunctionMeshSpecPod v1alpha1FunctionMeshSpecPod = (V1alpha1FunctionMeshSpecPod) o;
    return Objects.equals(this.affinity, v1alpha1FunctionMeshSpecPod.affinity) &&
        Objects.equals(this.annotations, v1alpha1FunctionMeshSpecPod.annotations) &&
        Objects.equals(this.autoScalingBehavior, v1alpha1FunctionMeshSpecPod.autoScalingBehavior) &&
        Objects.equals(this.autoScalingMetrics, v1alpha1FunctionMeshSpecPod.autoScalingMetrics) &&
        Objects.equals(this.builtinAutoscaler, v1alpha1FunctionMeshSpecPod.builtinAutoscaler) &&
        Objects.equals(this.imagePullSecrets, v1alpha1FunctionMeshSpecPod.imagePullSecrets) &&
        Objects.equals(this.initContainers, v1alpha1FunctionMeshSpecPod.initContainers) &&
        Objects.equals(this.labels, v1alpha1FunctionMeshSpecPod.labels) &&
        Objects.equals(this.nodeSelector, v1alpha1FunctionMeshSpecPod.nodeSelector) &&
        Objects.equals(this.securityContext, v1alpha1FunctionMeshSpecPod.securityContext) &&
        Objects.equals(this.serviceAccountName, v1alpha1FunctionMeshSpecPod.serviceAccountName) &&
        Objects.equals(this.sidecars, v1alpha1FunctionMeshSpecPod.sidecars) &&
        Objects.equals(this.terminationGracePeriodSeconds, v1alpha1FunctionMeshSpecPod.terminationGracePeriodSeconds) &&
        Objects.equals(this.tolerations, v1alpha1FunctionMeshSpecPod.tolerations) &&
        Objects.equals(this.volumes, v1alpha1FunctionMeshSpecPod.volumes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(affinity, annotations, autoScalingBehavior, autoScalingMetrics, builtinAutoscaler, imagePullSecrets, initContainers, labels, nodeSelector, securityContext, serviceAccountName, sidecars, terminationGracePeriodSeconds, tolerations, volumes);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1FunctionMeshSpecPod {\n");
    sb.append("    affinity: ").append(toIndentedString(affinity)).append("\n");
    sb.append("    annotations: ").append(toIndentedString(annotations)).append("\n");
    sb.append("    autoScalingBehavior: ").append(toIndentedString(autoScalingBehavior)).append("\n");
    sb.append("    autoScalingMetrics: ").append(toIndentedString(autoScalingMetrics)).append("\n");
    sb.append("    builtinAutoscaler: ").append(toIndentedString(builtinAutoscaler)).append("\n");
    sb.append("    imagePullSecrets: ").append(toIndentedString(imagePullSecrets)).append("\n");
    sb.append("    initContainers: ").append(toIndentedString(initContainers)).append("\n");
    sb.append("    labels: ").append(toIndentedString(labels)).append("\n");
    sb.append("    nodeSelector: ").append(toIndentedString(nodeSelector)).append("\n");
    sb.append("    securityContext: ").append(toIndentedString(securityContext)).append("\n");
    sb.append("    serviceAccountName: ").append(toIndentedString(serviceAccountName)).append("\n");
    sb.append("    sidecars: ").append(toIndentedString(sidecars)).append("\n");
    sb.append("    terminationGracePeriodSeconds: ").append(toIndentedString(terminationGracePeriodSeconds)).append("\n");
    sb.append("    tolerations: ").append(toIndentedString(tolerations)).append("\n");
    sb.append("    volumes: ").append(toIndentedString(volumes)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }

}

