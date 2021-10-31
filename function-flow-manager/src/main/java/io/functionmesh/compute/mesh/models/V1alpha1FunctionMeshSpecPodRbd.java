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
import io.functionmesh.compute.mesh.models.V1alpha1FunctionMeshSpecPodImagePullSecrets;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * V1alpha1FunctionMeshSpecPodRbd
 */
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", date = "2021-10-07T12:35:29.394Z[Etc/UTC]")
public class V1alpha1FunctionMeshSpecPodRbd {
  public static final String SERIALIZED_NAME_FS_TYPE = "fsType";
  @SerializedName(SERIALIZED_NAME_FS_TYPE)
  private String fsType;

  public static final String SERIALIZED_NAME_IMAGE = "image";
  @SerializedName(SERIALIZED_NAME_IMAGE)
  private String image;

  public static final String SERIALIZED_NAME_KEYRING = "keyring";
  @SerializedName(SERIALIZED_NAME_KEYRING)
  private String keyring;

  public static final String SERIALIZED_NAME_MONITORS = "monitors";
  @SerializedName(SERIALIZED_NAME_MONITORS)
  private List<String> monitors = new ArrayList<>();

  public static final String SERIALIZED_NAME_POOL = "pool";
  @SerializedName(SERIALIZED_NAME_POOL)
  private String pool;

  public static final String SERIALIZED_NAME_READ_ONLY = "readOnly";
  @SerializedName(SERIALIZED_NAME_READ_ONLY)
  private Boolean readOnly;

  public static final String SERIALIZED_NAME_SECRET_REF = "secretRef";
  @SerializedName(SERIALIZED_NAME_SECRET_REF)
  private V1alpha1FunctionMeshSpecPodImagePullSecrets secretRef;

  public static final String SERIALIZED_NAME_USER = "user";
  @SerializedName(SERIALIZED_NAME_USER)
  private String user;


  public V1alpha1FunctionMeshSpecPodRbd fsType(String fsType) {
    
    this.fsType = fsType;
    return this;
  }

   /**
   * Get fsType
   * @return fsType
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public String getFsType() {
    return fsType;
  }


  public void setFsType(String fsType) {
    this.fsType = fsType;
  }


  public V1alpha1FunctionMeshSpecPodRbd image(String image) {
    
    this.image = image;
    return this;
  }

   /**
   * Get image
   * @return image
  **/
  @ApiModelProperty(required = true, value = "")

  public String getImage() {
    return image;
  }


  public void setImage(String image) {
    this.image = image;
  }


  public V1alpha1FunctionMeshSpecPodRbd keyring(String keyring) {
    
    this.keyring = keyring;
    return this;
  }

   /**
   * Get keyring
   * @return keyring
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public String getKeyring() {
    return keyring;
  }


  public void setKeyring(String keyring) {
    this.keyring = keyring;
  }


  public V1alpha1FunctionMeshSpecPodRbd monitors(List<String> monitors) {
    
    this.monitors = monitors;
    return this;
  }

  public V1alpha1FunctionMeshSpecPodRbd addMonitorsItem(String monitorsItem) {
    this.monitors.add(monitorsItem);
    return this;
  }

   /**
   * Get monitors
   * @return monitors
  **/
  @ApiModelProperty(required = true, value = "")

  public List<String> getMonitors() {
    return monitors;
  }


  public void setMonitors(List<String> monitors) {
    this.monitors = monitors;
  }


  public V1alpha1FunctionMeshSpecPodRbd pool(String pool) {
    
    this.pool = pool;
    return this;
  }

   /**
   * Get pool
   * @return pool
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public String getPool() {
    return pool;
  }


  public void setPool(String pool) {
    this.pool = pool;
  }


  public V1alpha1FunctionMeshSpecPodRbd readOnly(Boolean readOnly) {
    
    this.readOnly = readOnly;
    return this;
  }

   /**
   * Get readOnly
   * @return readOnly
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public Boolean getReadOnly() {
    return readOnly;
  }


  public void setReadOnly(Boolean readOnly) {
    this.readOnly = readOnly;
  }


  public V1alpha1FunctionMeshSpecPodRbd secretRef(V1alpha1FunctionMeshSpecPodImagePullSecrets secretRef) {
    
    this.secretRef = secretRef;
    return this;
  }

   /**
   * Get secretRef
   * @return secretRef
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public V1alpha1FunctionMeshSpecPodImagePullSecrets getSecretRef() {
    return secretRef;
  }


  public void setSecretRef(V1alpha1FunctionMeshSpecPodImagePullSecrets secretRef) {
    this.secretRef = secretRef;
  }


  public V1alpha1FunctionMeshSpecPodRbd user(String user) {
    
    this.user = user;
    return this;
  }

   /**
   * Get user
   * @return user
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public String getUser() {
    return user;
  }


  public void setUser(String user) {
    this.user = user;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    V1alpha1FunctionMeshSpecPodRbd v1alpha1FunctionMeshSpecPodRbd = (V1alpha1FunctionMeshSpecPodRbd) o;
    return Objects.equals(this.fsType, v1alpha1FunctionMeshSpecPodRbd.fsType) &&
        Objects.equals(this.image, v1alpha1FunctionMeshSpecPodRbd.image) &&
        Objects.equals(this.keyring, v1alpha1FunctionMeshSpecPodRbd.keyring) &&
        Objects.equals(this.monitors, v1alpha1FunctionMeshSpecPodRbd.monitors) &&
        Objects.equals(this.pool, v1alpha1FunctionMeshSpecPodRbd.pool) &&
        Objects.equals(this.readOnly, v1alpha1FunctionMeshSpecPodRbd.readOnly) &&
        Objects.equals(this.secretRef, v1alpha1FunctionMeshSpecPodRbd.secretRef) &&
        Objects.equals(this.user, v1alpha1FunctionMeshSpecPodRbd.user);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fsType, image, keyring, monitors, pool, readOnly, secretRef, user);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1FunctionMeshSpecPodRbd {\n");
    sb.append("    fsType: ").append(toIndentedString(fsType)).append("\n");
    sb.append("    image: ").append(toIndentedString(image)).append("\n");
    sb.append("    keyring: ").append(toIndentedString(keyring)).append("\n");
    sb.append("    monitors: ").append(toIndentedString(monitors)).append("\n");
    sb.append("    pool: ").append(toIndentedString(pool)).append("\n");
    sb.append("    readOnly: ").append(toIndentedString(readOnly)).append("\n");
    sb.append("    secretRef: ").append(toIndentedString(secretRef)).append("\n");
    sb.append("    user: ").append(toIndentedString(user)).append("\n");
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

