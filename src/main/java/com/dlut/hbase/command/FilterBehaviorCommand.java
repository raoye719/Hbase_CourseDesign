package com.dlut.hbase.command;

import com.dlut.hbase.service.HBaseService;

public class FilterBehaviorCommand implements Command {
    private final HBaseService service;
    private final String userId;
    private final String behaviorType;

    public FilterBehaviorCommand(HBaseService service, String userId, String behaviorType) {
        this.service = service;
        this.userId = userId;
        this.behaviorType = behaviorType;
    }

    @Override
    public void execute() throws Exception {
        service.filterBehaviorsByType(userId, behaviorType);
    }

    @Override
    public String getName() {
        return "过滤行为类型";
    }

    @Override
    public String getDescription() {
        return "过滤用户 " + userId + " 的 " + behaviorType.toUpperCase() + " 行为完成";
    }
}