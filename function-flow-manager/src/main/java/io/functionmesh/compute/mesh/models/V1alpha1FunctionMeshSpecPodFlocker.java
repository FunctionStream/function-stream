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
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.IOException;

/**
 * V1alpha1FunctionMeshSpecPodFlocker
 */
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", date = "2021-10-07T12:35:29.394Z[Etc/UTC]")
public class V1alpha1FunctionMeshSpecPodFlocker {
  public static final String SERIALIZED_NAME_DATASET_NAME = "datasetName";
  @SerializedName(SERIALIZED_NAME_DATASET_NAME)
  private String datasetName;

  public static final String SERIALIZED_NAME_DATASET_U_U_I_D = "datasetUUID";
  @SerializedName(SERIALIZED_NAME_DATASET_U_U_I_D)
  private String datasetUUID;


  public V1alpha1FunctionMeshSpecPodFlocker datasetName(String datasetName) {
    
    this.datasetName = datasetName;
    return this;
  }

   /**
   * Get datasetName
   * @return datasetName
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public String getDatasetName() {
    return datasetName;
  }


  public void setDatasetName(String datasetName) {
    this.datasetName = datasetName;
  }


  public V1alpha1FunctionMeshSpecPodFlocker datasetUUID(String datasetUUID) {
    
    this.datasetUUID = datasetUUID;
    return this;
  }

   /**
   * Get datasetUUID
   * @return datasetUUID
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public String getDatasetUUID() {
    return datasetUUID;
  }


  public void setDatasetUUID(String datasetUUID) {
    this.datasetUUID = datasetUUID;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    V1alpha1FunctionMeshSpecPodFlocker v1alpha1FunctionMeshSpecPodFlocker = (V1alpha1FunctionMeshSpecPodFlocker) o;
    return Objects.equals(this.datasetName, v1alpha1FunctionMeshSpecPodFlocker.datasetName) &&
        Objects.equals(this.datasetUUID, v1alpha1FunctionMeshSpecPodFlocker.datasetUUID);
  }

  @Override
  public int hashCode() {
    return Objects.hash(datasetName, datasetUUID);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1FunctionMeshSpecPodFlocker {\n");
    sb.append("    datasetName: ").append(toIndentedString(datasetName)).append("\n");
    sb.append("    datasetUUID: ").append(toIndentedString(datasetUUID)).append("\n");
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

