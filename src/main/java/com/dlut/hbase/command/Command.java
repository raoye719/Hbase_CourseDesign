package com.dlut.hbase.command;

public interface Command {
    void execute() throws Exception;
    String getName();        // 用于UI显示按钮文字
    String getDescription(); // 执行后提示信息
}