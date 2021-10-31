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
import io.functionmesh.compute.mesh.models.V1alpha1FunctionMeshSpecPodExternalMetric;
import io.functionmesh.compute.mesh.models.V1alpha1FunctionMeshSpecPodExternalTarget;
import io.functionmesh.compute.mesh.models.V1alpha1FunctionMeshSpecPodObjectDescribedObject;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.IOException;

/**
 * V1alpha1FunctionMeshSpecPodObject
 */
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", date = "2021-10-07T12:35:29.394Z[Etc/UTC]")
public class V1alpha1FunctionMeshSpecPodObject {
  public static final String SERIALIZED_NAME_DESCRIBED_OBJECT = "describedObject";
  @SerializedName(SERIALIZED_NAME_DESCRIBED_OBJECT)
  private V1alpha1FunctionMeshSpecPodObjectDescribedObject describedObject;

  public static final String SERIALIZED_NAME_METRIC = "metric";
  @SerializedName(SERIALIZED_NAME_METRIC)
  private V1alpha1FunctionMeshSpecPodExternalMetric metric;

  public static final String SERIALIZED_NAME_TARGET = "target";
  @SerializedName(SERIALIZED_NAME_TARGET)
  private V1alpha1FunctionMeshSpecPodExternalTarget target;


  public V1alpha1FunctionMeshSpecPodObject describedObject(V1alpha1FunctionMeshSpecPodObjectDescribedObject describedObject) {
    
    this.describedObject = describedObject;
    return this;
  }

   /**
   * Get describedObject
   * @return describedObject
  **/
  @ApiModelProperty(required = true, value = "")

  public V1alpha1FunctionMeshSpecPodObjectDescribedObject getDescribedObject() {
    return describedObject;
  }


  public void setDescribedObject(V1alpha1FunctionMeshSpecPodObjectDescribedObject describedObject) {
    this.describedObject = describedObject;
  }


  public V1alpha1FunctionMeshSpecPodObject metric(V1alpha1FunctionMeshSpecPodExternalMetric metric) {
    
    this.metric = metric;
    return this;
  }

   /**
   * Get metric
   * @return metric
  **/
  @ApiModelProperty(required = true, value = "")

  public V1alpha1FunctionMeshSpecPodExternalMetric getMetric() {
    return metric;
  }


  public void setMetric(V1alpha1FunctionMeshSpecPodExternalMetric metric) {
    this.metric = metric;
  }


  public V1alpha1FunctionMeshSpecPodObject target(V1alpha1FunctionMeshSpecPodExternalTarget target) {
    
    this.target = target;
    return this;
  }

   /**
   * Get target
   * @return target
  **/
  @ApiModelProperty(required = true, value = "")

  public V1alpha1FunctionMeshSpecPodExternalTarget getTarget() {
    return target;
  }


  public void setTarget(V1alpha1FunctionMeshSpecPodExternalTarget target) {
    this.target = target;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    V1alpha1FunctionMeshSpecPodObject v1alpha1FunctionMeshSpecPodObject = (V1alpha1FunctionMeshSpecPodObject) o;
    return Objects.equals(this.describedObject, v1alpha1FunctionMeshSpecPodObject.describedObject) &&
        Objects.equals(this.metric, v1alpha1FunctionMeshSpecPodObject.metric) &&
        Objects.equals(this.target, v1alpha1FunctionMeshSpecPodObject.target);
  }

  @Override
  public int hashCode() {
    return Objects.hash(describedObject, metric, target);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1FunctionMeshSpecPodObject {\n");
    sb.append("    describedObject: ").append(toIndentedString(describedObject)).append("\n");
    sb.append("    metric: ").append(toIndentedString(metric)).append("\n");
    sb.append("    target: ").append(toIndentedString(target)).append("\n");
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

