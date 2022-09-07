package org.functionstream.fsflow.service;


public interface TokenService {
    String generateToken(String id);

    void setToken(String key, String value);

    String getToken(String key);

    Boolean removeToken(String key);

}
