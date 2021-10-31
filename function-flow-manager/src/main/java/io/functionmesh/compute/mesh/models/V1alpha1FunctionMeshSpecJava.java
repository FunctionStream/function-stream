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
 * V1alpha1FunctionMeshSpecJava
 */
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", date = "2021-10-07T12:35:29.394Z[Etc/UTC]")
public class V1alpha1FunctionMeshSpecJava {
  public static final String SERIALIZED_NAME_EXTRA_DEPENDENCIES_DIR = "extraDependenciesDir";
  @SerializedName(SERIALIZED_NAME_EXTRA_DEPENDENCIES_DIR)
  private String extraDependenciesDir;

  public static final String SERIALIZED_NAME_JAR = "jar";
  @SerializedName(SERIALIZED_NAME_JAR)
  private String jar;

  public static final String SERIALIZED_NAME_JAR_LOCATION = "jarLocation";
  @SerializedName(SERIALIZED_NAME_JAR_LOCATION)
  private String jarLocation;


  public V1alpha1FunctionMeshSpecJava extraDependenciesDir(String extraDependenciesDir) {
    
    this.extraDependenciesDir = extraDependenciesDir;
    return this;
  }

   /**
   * Get extraDependenciesDir
   * @return extraDependenciesDir
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public String getExtraDependenciesDir() {
    return extraDependenciesDir;
  }


  public void setExtraDependenciesDir(String extraDependenciesDir) {
    this.extraDependenciesDir = extraDependenciesDir;
  }


  public V1alpha1FunctionMeshSpecJava jar(String jar) {
    
    this.jar = jar;
    return this;
  }

   /**
   * Get jar
   * @return jar
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public String getJar() {
    return jar;
  }


  public void setJar(String jar) {
    this.jar = jar;
  }


  public V1alpha1FunctionMeshSpecJava jarLocation(String jarLocation) {
    
    this.jarLocation = jarLocation;
    return this;
  }

   /**
   * Get jarLocation
   * @return jarLocation
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public String getJarLocation() {
    return jarLocation;
  }


  public void setJarLocation(String jarLocation) {
    this.jarLocation = jarLocation;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    V1alpha1FunctionMeshSpecJava v1alpha1FunctionMeshSpecJava = (V1alpha1FunctionMeshSpecJava) o;
    return Objects.equals(this.extraDependenciesDir, v1alpha1FunctionMeshSpecJava.extraDependenciesDir) &&
        Objects.equals(this.jar, v1alpha1FunctionMeshSpecJava.jar) &&
        Objects.equals(this.jarLocation, v1alpha1FunctionMeshSpecJava.jarLocation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(extraDependenciesDir, jar, jarLocation);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1FunctionMeshSpecJava {\n");
    sb.append("    extraDependenciesDir: ").append(toIndentedString(extraDependenciesDir)).append("\n");
    sb.append("    jar: ").append(toIndentedString(jar)).append("\n");
    sb.append("    jarLocation: ").append(toIndentedString(jarLocation)).append("\n");
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

