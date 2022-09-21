package org.functionstream.fsflow.exception;

public class LoginException extends Exception{
    private String message;

    public LoginException() {
    }

    public LoginException(String message) {
        super(message);
    }
}
