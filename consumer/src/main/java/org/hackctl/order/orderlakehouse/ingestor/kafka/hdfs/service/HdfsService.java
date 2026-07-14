package org.hackctl.order.orderlakehouse.ingestor.kafka.hdfs.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.hackctl.order.orderlakehouse.ingestor.kafka.hdfs.dto.OrderRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

@Slf4j
@Service
public class HdfsService {

    @Autowired
    private FileSystem hdfsFileSystem;

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${hdfs.base-path}")
    private String basePath;

    /**
     * Writes order to HDFS at:
     * /orders/{regionId}/{businessId}/{stateId}/{year}/{month}/{day}/orders.json
     */
    public void writeOrder(OrderRequest order) throws IOException {
        String hdfsFilePath = buildFilePath(order);
        Path dirPath = new Path(hdfsFilePath).getParent();
        Path filePath = new Path(hdfsFilePath);

        // 1. Create directories if not exist
        if (!hdfsFileSystem.exists(dirPath)) {
            hdfsFileSystem.mkdirs(dirPath);
            log.info("Created HDFS directory: {}", dirPath);
        }

        // 2. Serialize order to JSON line
        String jsonLine = objectMapper.writeValueAsString(order) + "\n";

        // 3. Append to file if exists, create if not
        FSDataOutputStream outputStream;
        if (hdfsFileSystem.exists(filePath)) {
            outputStream = hdfsFileSystem.append(filePath);
            log.debug("Appending to existing HDFS file: {}", filePath);
        } else {
            outputStream = hdfsFileSystem.create(filePath);
            log.info("Created new HDFS file: {}", filePath);
        }

        try {
            outputStream.writeBytes(jsonLine);
            outputStream.flush();
        } finally {
            outputStream.close();
        }

        log.info("Written order to HDFS: {} | businessId={} stateId={}",
                hdfsFilePath, order.getBusinessId(), order.getStateId());
    }

    /**
     * Builds: /orders/{regionId}/{businessId}/{stateId}/{year}/{month}/{day}/orders.json
     */
    private String buildFilePath(OrderRequest order) {
        OffsetDateTime orderTime = OffsetDateTime.parse(order.getOrderTime(),
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX"));

        String year  = String.valueOf(orderTime.getYear());
        String month = String.format("%02d", orderTime.getMonthValue());
        String day   = String.format("%02d", orderTime.getDayOfMonth());

        return String.format("%s/%s/%s/%s/%s/%s/%s/orders.json",
                basePath,
                order.getRegionId(),
                order.getBusinessId(),
                order.getStateId(),
                year,
                month,
                day
        );
    }
}