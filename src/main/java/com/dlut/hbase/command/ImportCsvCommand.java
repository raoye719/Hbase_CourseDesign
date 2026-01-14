package com.dlut.hbase.command;

import com.dlut.hbase.service.HBaseService;

public class ImportCsvCommand implements Command {
    private final HBaseService service;

    public ImportCsvCommand(HBaseService service) {
        this.service = service;
    }

    @Override public void execute() throws Exception { service.importFromCsv(); }
    @Override public String getName() { return "导入CSV数据"; }
    @Override public String getDescription() { return "数据导入完成"; }
}
