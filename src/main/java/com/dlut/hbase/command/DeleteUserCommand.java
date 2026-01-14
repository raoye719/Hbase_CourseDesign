package com.dlut.hbase.command;

import com.dlut.hbase.service.HBaseService;

public class DeleteUserCommand implements Command {

    private final HBaseService service;

    private String userId;

    public DeleteUserCommand(HBaseService service, String userId) {
        this.service = service;
        this.userId = userId;
    }

    @Override
    public void execute() throws Exception {
        service.deleteUserBehaviors(userId);
    }

    @Override
    public String getName() {
        return "删除用户记录";
    }

    @Override
    public String getDescription() {
        return String.format("删除用户%s的记录成功", userId);
    }
}
