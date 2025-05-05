#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Telegram Bot API Token
# 从BotFather获取的API令牌 - 需要替换为您自己的令牌
BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"

# 管理员用户ID
# 可以预先设置管理员的Telegram用户ID（数字格式）
# 如果设置为None，则第一个使用/set_admin命令的用户将成为管理员
ADMIN_USER_ID = None  # 例如: 123456789

# 初始操作人用户名列表
# 这些用户将被自动授权为操作人
# 格式为用户名（不带@符号）
INITIAL_OPERATORS = [
    # "username1",
    # "username2",
]

# 时区设置
TIMEZONE = "Asia/Shanghai"

# 每日重置检查间隔（秒）
RESET_CHECK_INTERVAL = 3600  # 每小时检查一次 