package com.dlut.hbase.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HBaseConnectionManager {
    private static Configuration config;
    private static Connection connection;
    
    private static final String ZOOKEEPER_QUORUM = "master,slave1,slave2";
    private static final String ZOOKEEPER_PORT = "2181";
    
    public static Configuration getConfiguration() {
        if (config == null) {
            config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
            config.set("hbase.zookeeper.property.clientPort", ZOOKEEPER_PORT);
            config.set("hbase.client.retries.number", "3");
            config.set("hbase.rpc.timeout", "5000");
            config.set("hbase.client.operation.timeout", "10000");
        }
        return config;
    }
    
    public static Connection getConnection() {
        try {
            if (connection == null || connection.isClosed()) {
                connection = ConnectionFactory.createConnection(getConfiguration());
                System.out.println("HBase connection established successfully");
            }
        } catch (Exception e) {
            System.err.println("Failed to create HBase connection: " + e.getMessage());
            e.printStackTrace();
        }
        return connection;
    }
    
    public static void closeConnection() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
                System.out.println("HBase connection closed");
            }
        } catch (Exception e) {
            System.err.println("Error closing connection: " + e.getMessage());
        }
    }
}