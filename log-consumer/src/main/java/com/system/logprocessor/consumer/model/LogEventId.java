package com.system.logprocessor.consumer.model;

import java.io.Serializable;
import java.util.Objects;

public class LogEventId implements Serializable {

    private String id;
    private String organizationId;

    public LogEventId() {}

    public LogEventId(String id, String organizationId) {
        this.id = id;
        this.organizationId = organizationId;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof LogEventId that)) {
            return false;
        }
        return Objects.equals(id, that.id) && Objects.equals(organizationId, that.organizationId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, organizationId);
    }
}
