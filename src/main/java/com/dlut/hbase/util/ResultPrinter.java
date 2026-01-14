package com.dlut.hbase.util;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class ResultPrinter {
    public static void print(Result result) {
        String rowKey = Bytes.toString(result.getRow());
        System.out.print("RowKey: " + rowKey + " -> ");
        for (Cell cell : result.rawCells()) {
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.print(family + ":" + qualifier + "=" + value + "  ");
        }
        System.out.println();
    }
}