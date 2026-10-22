package com.system.logprocessor.consumer.repository;

import java.time.Instant;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.system.logprocessor.consumer.model.LogEvent;
import com.system.logprocessor.consumer.model.LogEventId;

@Repository
public interface LogEventRepository extends JpaRepository<LogEvent, LogEventId> {

    @Modifying
    @Transactional
    @Query(value = """
            INSERT INTO log_events (id, organization_id, level, message, source, timestamp, processed_at)
            VALUES (:id, :organizationId, :level, :message, :source, :timestamp, :processedAt)
            ON CONFLICT (organization_id, id) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(
            @Param("id") String id,
            @Param("organizationId") String organizationId,
            @Param("level") String level,
            @Param("message") String message,
            @Param("source") String source,
            @Param("timestamp") Instant timestamp,
            @Param("processedAt") Instant processedAt);
}
