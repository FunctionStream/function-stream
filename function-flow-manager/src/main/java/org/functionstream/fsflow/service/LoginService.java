package org.functionstream.fsflow.service;

import org.functionstream.fsflow.entity.UserEntity;

public interface LoginService {
    UserEntity login(String username, String password);
}
