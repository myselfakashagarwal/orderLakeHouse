package org.hackctl.order.orderlakehouse.ingestor.kafka.hdfs.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;

@Slf4j
@Configuration
public class HdfsConfig {

    @Value("${hdfs.uri}")
    private String hdfsUri;

    @Bean
    public FileSystem hdfsFileSystem() {
        try {
            org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
            config.set("fs.defaultFS", hdfsUri);
            config.set("dfs.replication", "2");
            FileSystem fs = FileSystem.get(URI.create(hdfsUri), config);
            log.info("HDFS FileSystem connected: {}", hdfsUri);
            return fs;
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to HDFS at: " + hdfsUri, e);
        }
    }
}