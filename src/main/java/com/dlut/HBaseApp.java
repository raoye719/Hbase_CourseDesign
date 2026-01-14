package com.dlut;

import com.dlut.hbase.command.*;
import com.dlut.hbase.service.HBaseService;

import javax.swing.*;
import java.awt.*;

public class HBaseApp {
    public static void main(String[] args) {
        SwingUtilities.invokeLater(HBaseApp::createAndShowGUI);
    }

    private static void createAndShowGUI() {
        JFrame frame = new JFrame("HBase 用户行为日志管理系统");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(900, 750);
        frame.setLayout(new BorderLayout(10, 20));

        try {
            HBaseService service = new HBaseService();

            // ============ 顶部标题 ============
            JLabel title = new JLabel("HBase 用户行为日志管理系统", JLabel.CENTER);
            title.setFont(new Font("微软雅黑", Font.BOLD, 30));
            title.setBorder(BorderFactory.createEmptyBorder(20, 0, 20, 0));
            frame.add(title, BorderLayout.NORTH);

            // ============ 中央主面板 ============
            JPanel mainPanel = new JPanel();
            mainPanel.setLayout(new BoxLayout(mainPanel, BoxLayout.Y_AXIS));
            mainPanel.setBorder(BorderFactory.createEmptyBorder(20, 40, 40, 40));

            // ============ 全局共享的用户ID输入框 ============
            JPanel userIdPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 10, 10));
            userIdPanel.add(new JLabel("用户ID："));
            JTextField userIdField = new JTextField(20);
            userIdField.setFont(new Font("微软雅黑", Font.PLAIN, 18));
            userIdField.setPreferredSize(new Dimension(200, 45));
            userIdPanel.add(userIdField);
            userIdPanel.setAlignmentX(Component.LEFT_ALIGNMENT);
            mainPanel.add(userIdPanel);
            mainPanel.add(Box.createVerticalStrut(30));

            // ============ 1. 创建表 + 导入CSV ============
            JPanel topButtons = new JPanel(new FlowLayout(FlowLayout.LEFT, 30, 10));
            topButtons.add(createStyledButton("创建表", new CreateTableCommand(service)));
            topButtons.add(createStyledButton("导入CSV数据", new ImportCsvCommand(service)));
            topButtons.setAlignmentX(Component.LEFT_ALIGNMENT);
            mainPanel.add(topButtons);
            mainPanel.add(Box.createVerticalStrut(30));

            // ============ 2. 查询最近N条行为 ============
            JPanel queryPanel = createTitledPanel("查询最近行为");
            queryPanel.add(new JLabel("最近N条："));
            JTextField limitField = new JTextField("5", 8);
            limitField.setFont(new Font("微软雅黑", Font.PLAIN, 18));
            limitField.setPreferredSize(new Dimension(100, 45));
            queryPanel.add(limitField);
            queryPanel.add(Box.createHorizontalStrut(30));

            JButton queryBtn = createStyledButton("执行查询", null);
            queryBtn.addActionListener(e -> executeCommand(
                    new QueryRecentCommand(service, userIdField.getText().trim(),
                            Integer.parseInt(limitField.getText().trim()))
            ));
            queryPanel.add(queryBtn);
            mainPanel.add(queryPanel);
            mainPanel.add(Box.createVerticalStrut(20));

            // ============ 3. 统计行为次数 ============
            JPanel countPanel = createTitledPanel("统计行为次数");
            countPanel.add(new JLabel("类型(view/buy)："));
            JTextField typeCountField = new JTextField("view", 12);
            typeCountField.setFont(new Font("微软雅黑", Font.PLAIN, 18));
            countPanel.add(typeCountField);
            countPanel.add(Box.createHorizontalStrut(30));

            JButton countBtn = createStyledButton("执行统计", null);
            countBtn.addActionListener(e -> executeCommand(
                    new CountBehaviorCommand(service, userIdField.getText().trim(), typeCountField.getText().trim())
            ));
            countPanel.add(countBtn);
            mainPanel.add(countPanel);
            mainPanel.add(Box.createVerticalStrut(20));

            // ============ 4. 过滤指定行为类型 ============
            JPanel filterPanel = createTitledPanel("过滤指定行为类型");
            filterPanel.add(new JLabel("类型(view/buy)："));
            JTextField typeFilterField = new JTextField("view", 12);
            typeFilterField.setFont(new Font("微软雅黑", Font.PLAIN, 18));
            filterPanel.add(typeFilterField);
            filterPanel.add(Box.createHorizontalStrut(30));

            JButton filterBtn = createStyledButton("执行过滤", null);
            filterBtn.addActionListener(e -> executeCommand(
                    new FilterBehaviorCommand(service, userIdField.getText().trim(), typeFilterField.getText().trim())
            ));
            filterPanel.add(filterBtn);
            mainPanel.add(filterPanel);
            mainPanel.add(Box.createVerticalStrut(20));

            // ============ 5. 删除用户所有记录 ============
            JPanel deletePanel = createTitledPanel("删除用户所有记录");
            JButton deleteBtn = createStyledButton("确认删除", null);
            deleteBtn.setForeground(Color.RED);
            deleteBtn.setFont(new Font("微软雅黑", Font.BOLD, 18));
            deleteBtn.addActionListener(e -> {
                String uid = userIdField.getText().trim();
                if (uid.isEmpty()) {
                    JOptionPane.showMessageDialog(frame, "请先输入用户ID！");
                    return;
                }
                int confirm = JOptionPane.showConfirmDialog(frame,
                        "警告：确定要永久删除用户 " + uid + " 的所有行为记录吗？\n此操作不可恢复！",
                        "确认删除", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE);
                if (confirm == JOptionPane.YES_OPTION) {
                    executeCommand(new DeleteUserCommand(service, uid));
                }
            });
            deletePanel.add(deleteBtn);
            mainPanel.add(deletePanel);

            // ============ 添加滚动支持 ============
            JScrollPane scrollPane = new JScrollPane(mainPanel);
            scrollPane.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED);
            frame.add(scrollPane, BorderLayout.CENTER);

        } catch (Exception e) {
            JOptionPane.showMessageDialog(null, "系统初始化失败: " + e.getMessage());
            e.printStackTrace();
        }

        frame.setLocationRelativeTo(null);
        frame.setVisible(true);
    }

    // 辅助：创建带标题的面板
    private static JPanel createTitledPanel(String titleText) {
        JPanel panel = new JPanel(new FlowLayout(FlowLayout.LEFT, 15, 15));
        panel.setBorder(BorderFactory.createTitledBorder(
                BorderFactory.createEtchedBorder(), titleText,
                0, 0, new Font("微软雅黑", Font.BOLD, 20), new Color(0, 102, 204)
        ));
        panel.setAlignmentX(Component.LEFT_ALIGNMENT);
        panel.setMaximumSize(new Dimension(Integer.MAX_VALUE, 100));
        return panel;
    }

    // 辅助：创建统一风格按钮
    private static JButton createStyledButton(String text, Command command) {
        JButton btn = new JButton(text);
        btn.setFont(new Font("微软雅黑", Font.BOLD, 18));
        btn.setPreferredSize(new Dimension(200, 50));
        btn.setFocusPainted(false);
        if (command != null) {
            btn.addActionListener(e -> executeCommand(command));
        }
        return btn;
    }

    // 统一执行命令 + 友好提示
    private static void executeCommand(Command command) {
        try {
            command.execute();
            JOptionPane.showMessageDialog(null, command.getDescription() + "\n操作成功！", "成功", JOptionPane.INFORMATION_MESSAGE);
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(null, "操作失败：\n" + ex.getMessage(), "错误", JOptionPane.ERROR_MESSAGE);
            ex.printStackTrace();
        }
    }
}