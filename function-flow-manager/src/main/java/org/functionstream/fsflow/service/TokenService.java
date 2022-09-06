package org.functionstream.fsflow.service;

import org.functionstream.fsflow.entity.UserEntity;

public interface TokenService {
    String generateToken(String id);

    void setToken(String key, String value);

    String getToken(String key);

    void removeToken(String key);

}
