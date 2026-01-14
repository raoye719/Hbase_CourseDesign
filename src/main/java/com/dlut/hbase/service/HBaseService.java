package com.dlut.hbase.service;

import com.dlut.hbase.config.HBaseConnectionManager;
import com.dlut.hbase.util.ResultPrinter;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class HBaseService {
    private static final String TABLE_NAME = "user_behavior_log";  // 我们自己的表名
    private static final String CSV_PATH = "user_behavior.csv";    // 原始CSV文件路径（放项目根目录或改成绝对路径）
    private final TableName tableName;
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public HBaseService() {
        this.tableName = TableName.valueOf(TABLE_NAME);
    }

    public void createTable() throws Exception {
        Connection conn = HBaseConnectionManager.getConnection();
        Admin admin = conn.getAdmin();

        if (admin.tableExists(tableName)) {
            System.out.println("表 " + TABLE_NAME + " 已存在，无需重复创建");
            admin.close();
            return;
        }

        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
        builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of("info"));
        builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of("action"));
        admin.createTable(builder.build());
        admin.close();
        System.out.println("表 " + TABLE_NAME + " 创建成功");
    }

    public void importFromCsv() throws Exception {
        Connection conn = HBaseConnectionManager.getConnection();
        Table table = conn.getTable(tableName);

        BufferedReader br = new BufferedReader(new FileReader(CSV_PATH));
        String line;
        boolean first = true;
        int count = 0;

        while ((line = br.readLine()) != null) {
            if (first) { first = false; continue; } // 跳过表头
            String[] fields = line.split(",");
            String userId = fields[0].trim();
            String username = fields[1].trim();
            String age = fields[2].trim();
            String gender = fields[3].trim();
            String behaviorType = fields[4].trim();
            String timestampStr = fields[5].trim();
            String productId = fields[6].trim();
            String productName = fields[7].trim();
            String category = fields[8].trim();
            String price = fields[9].trim();
            String ip = fields[10].trim();

            long timestamp = sdf.parse(timestampStr).getTime();
            long reverseTime = Long.MAX_VALUE - timestamp;
            String rowKey = userId + "_" + reverseTime + "_" + behaviorType + "_" + productId;

            Put put = new Put(Bytes.toBytes(rowKey));
            // info列族
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("username"), Bytes.toBytes(username));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(age));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("gender"), Bytes.toBytes(gender));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ip"), Bytes.toBytes(ip));
            // action列族
            put.addColumn(Bytes.toBytes("action"), Bytes.toBytes("behavior_type"), Bytes.toBytes(behaviorType));
            put.addColumn(Bytes.toBytes("action"), Bytes.toBytes("timestamp"), Bytes.toBytes(timestampStr));
            put.addColumn(Bytes.toBytes("action"), Bytes.toBytes("product_id"), Bytes.toBytes(productId));
            put.addColumn(Bytes.toBytes("action"), Bytes.toBytes("product_name"), Bytes.toBytes(productName));
            put.addColumn(Bytes.toBytes("action"), Bytes.toBytes("category"), Bytes.toBytes(category));
            put.addColumn(Bytes.toBytes("action"), Bytes.toBytes("price"), Bytes.toBytes(price));

            table.put(put);
            count++;
        }
        table.close();
        br.close();
        System.out.println("成功导入 " + count + " 条记录到表 " + TABLE_NAME);
    }

    public void queryRecentBehaviors(String userId, int limit) throws Exception {
        Connection conn = HBaseConnectionManager.getConnection();
        Table table = conn.getTable(tableName);
        String startRow = userId + "_";
        String stopRow = userId + "`";

        Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));
        scan.setLimit(limit);

        ResultScanner scanner = table.getScanner(scan);
        System.out.println("=== 用户 " + userId + " 最近 " + limit + " 条行为（时间倒序） ===");
        for (Result result : scanner) {
            ResultPrinter.print(result);
        }
        scanner.close();
        table.close();
    }

    public void countBehavior(String userId, String type) throws Exception {
        Connection conn = HBaseConnectionManager.getConnection();
        Table table = conn.getTable(tableName);
        String startRow = userId + "_";
        String stopRow = userId + "`";

        Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));
        int count = 0;
        for (Result result : table.getScanner(scan)) {
            byte[] value = result.getValue(Bytes.toBytes("action"), Bytes.toBytes("behavior_type"));
            if (value != null && type.equalsIgnoreCase(Bytes.toString(value))) {
                count++;
            }
        }
        table.close();
        System.out.println("用户 " + userId + " 的 " + type + " 行为次数： " + count);
    }

    public void filterBehaviorsByType(String userId, String behaviorType) throws Exception {
        Connection conn = HBaseConnectionManager.getConnection();
        Table table = conn.getTable(tableName);
        String startRow = userId + "_";
        String stopRow = userId + "`";

        Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));
        System.out.println("=== 用户 " + userId + " 的所有 " + behaviorType.toUpperCase() + " 行为（时间倒序） ===");
        int count = 0;
        for (Result result : table.getScanner(scan)) {
            byte[] value = result.getValue(Bytes.toBytes("action"), Bytes.toBytes("behavior_type"));
            if (value != null && behaviorType.equalsIgnoreCase(Bytes.toString(value))) {
                ResultPrinter.print(result);
                count++;
            }
        }
        if (count == 0) {
            System.out.println("未找到匹配记录");
        } else {
            System.out.println("共找到 " + count + " 条记录");
        }
        table.close();
    }

    public void deleteUserBehaviors(String userId) throws Exception {
        Connection conn = HBaseConnectionManager.getConnection();
        Table table = conn.getTable(tableName);
        String startRow = userId + "_";
        String stopRow = userId + "`";

        Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));
        int deleted = 0;
        for (Result result : table.getScanner(scan)) {
            Delete delete = new Delete(result.getRow());
            table.delete(delete);
            deleted++;
        }
        table.close();
        System.out.println("已删除用户 " + userId + " 的 " + deleted + " 条记录");
    }
}