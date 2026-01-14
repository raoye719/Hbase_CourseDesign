package com.dlut.hbase.command;

import com.dlut.hbase.service.HBaseService;

public class CountBehaviorCommand implements Command {

    private final HBaseService service;

    private String userId;

    private String type;

    public CountBehaviorCommand(HBaseService service, String userId, String type) {
    this.service = service;
    this.userId = userId;
    this.type = type;
    }

    @Override
    public void execute() throws Exception {
        service.countBehavior(userId, type);
    }

    @Override
    public String getName() {
        return "统计行为次数";
    }

    @Override
    public String getDescription() {
        return String.format("统计用户%s的%s行为的次数", userId, type);
    }
}
