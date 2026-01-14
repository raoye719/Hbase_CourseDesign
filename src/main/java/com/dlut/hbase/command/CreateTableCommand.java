package com.dlut.hbase.command;

import com.dlut.hbase.service.HBaseService;

public class CreateTableCommand implements Command {
    private final HBaseService service;

    public CreateTableCommand(HBaseService service) {
        this.service = service;
    }

    @Override public void execute() throws Exception { service.createTable(); }
    @Override public String getName() { return "创建表"; }
    @Override public String getDescription() { return "表创建完成"; }
}