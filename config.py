#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Telegram Bot API Token
# 从BotFather获取的API令牌 - 需要替换为您自己的令牌
BOT_TOKEN = "8087490170:AAGkIL_s_NywMN0z6uyx7Jty6r66Ej9SfS0"

# 管理员用户ID
# 可以预先设置管理员的Telegram用户ID（数字格式）
# 如果设置为None，则第一个使用/set_admin命令的用户将成为管理员
ADMIN_USER_ID = [1844353808, 7997704196]  # 支持多个全局管理员ID

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