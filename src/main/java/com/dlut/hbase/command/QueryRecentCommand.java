package com.dlut.hbase.command;

import com.dlut.hbase.service.HBaseService;

public class QueryRecentCommand implements Command{

    private final HBaseService service;

    private String userId;

    private int limit;

    public QueryRecentCommand(HBaseService service, String userId, int limit) {
        this.service = service;
        this.userId = userId;
        this.limit = limit;

    }

    @Override
    public void execute() throws Exception {
        service.queryRecentBehaviors(userId, limit);
    }

    @Override
    public String getName() {
        return "查询最近N条数据";
    }

    @Override
    public String getDescription() {
        return String.format("用户%s的数据查询", userId);
    }
}
