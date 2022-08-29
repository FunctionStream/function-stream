package org.functionstream.fsflow.service;

import org.functionstream.fsflow.entity.UserEntity;

public interface TokenService {
    //生成
    String generateToken(String id);

    //修改
    void setToken(String key, String value);

    //获取
    String getToken(String key);

    //注销
    void removeToken(String key);

    UserEntity login(String username, String password);
}
