package org.functionstream.fsflow.service.impl;

import lombok.SneakyThrows;
import org.functionstream.fsflow.entity.UserEntity;
import org.functionstream.fsflow.exception.LoginException;
import org.functionstream.fsflow.service.LoginService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class LoginServiceImpl implements LoginService{
    @Value("${function-stream.account}")
    private String account_p;
    @Value("${function-stream.password}")
    private String password_p;

    @SneakyThrows
    @Override
    public UserEntity login(String username, String password) {
        if (!username.isEmpty() && !password.isEmpty()) {
            if (username.equals(account_p) && password.equals(password_p)) {
                return new UserEntity(username, password);
            }
        }
        throw new LoginException("fail login");
    }
}
