package com.system.logprocessor.consumer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.system.logprocessor.consumer.model.LogEvent;

@Repository
public interface LogEventRepository extends JpaRepository<LogEvent, String> {

}