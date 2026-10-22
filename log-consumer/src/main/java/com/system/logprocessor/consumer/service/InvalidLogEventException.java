package com.system.logprocessor.consumer.service;

public class InvalidLogEventException extends RuntimeException {

    public InvalidLogEventException(String message) {
        super(message);
    }

    public InvalidLogEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
