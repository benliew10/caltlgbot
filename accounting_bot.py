#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 为python-telegram-bot添加缺失的imghdr模块替代品
import sys
import os
import json  # 用于美化日志输出和数据持久化
import datetime
import pytz
import re
import logging
from telegram import Update, ParseMode, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext, CallbackQueryHandler
import signal
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

class ImghdrModule:
    def what(self, *args, **kwargs):
        return None
sys.modules['imghdr'] = ImghdrModule()

# 导入配置文件
from config import BOT_TOKEN, ADMIN_USER_ID, INITIAL_OPERATORS, TIMEZONE, RESET_CHECK_INTERVAL

# 设置详细的日志记录
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.DEBUG,  # 将日志级别改为DEBUG以获取更多信息
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 将全局单一账单改为按聊天ID存储的多账单
# 全局数据结构改为字典，键为聊天ID
chat_accounting = {}

# 授权群组列表
authorized_groups = set()

# 全局变量声明，添加处理过的消息ID缓存
processed_message_ids = set()  # 已处理过的消息ID缓存
MAX_PROCESSED_MESSAGES = 100  # 最大缓存消息数量

def get_chat_accounting(chat_id):
    """获取或创建聊天的账单记录"""
    global chat_accounting
    
    # 如果该聊天的数据还不存在，创建一个新的
    if chat_id not in chat_accounting:
        logger.info(f"为聊天 {chat_id} 创建新的账单记录")
        chat_accounting[chat_id] = {
            'deposits': [],  # 充值记录列表
            'withdrawals': [],  # 提款记录列表
            'rate': 0.0,  # 默认费率0%
            'fixed_rate': 0.0,  # 默认汇率0
            'users': {},  # 用户分类
        }
    
    return chat_accounting[chat_id]

def reset_chat_accounting(chat_id):
    """重置指定聊天的账单数据"""
    global chat_accounting
    chat_accounting[chat_id] = {
        'deposits': [],
        'users': {},
        'withdrawals': [],
        'rate': 0.0,
        'fixed_rate': 1.0,
    }
    logger.info(f"聊天 {chat_id} 的账单数据已重置")
    save_data()

def check_date_change(context: CallbackContext):
    """检查日期变更，执行每日重置和清理旧记录"""
    logger.info("检查日期变更...")
    global chat_accounting
    
    # 获取当前时间和日期
    current_time = get_current_time()
    current_date = get_current_date()
    
    # 检查上次重置的日期
    last_reset_date = context.bot_data.get('last_reset_date', None)
    
    if last_reset_date != current_date:
        logger.info(f"检测到日期变更: {last_reset_date} -> {current_date}")
        
        # 更新上次重置日期
        context.bot_data['last_reset_date'] = current_date
        
        # 为每个群组归档当天数据并重置当前账单
        for chat_id, chat_data in list(chat_accounting.items()):
            try:
                # 归档当天的数据
                archive_chat_accounting_history(chat_id, last_reset_date)
                
                # 保存当前的费率和汇率设置
                current_rate = chat_data.get('rate', 0.0)
                current_fixed_rate = chat_data.get('fixed_rate', 0.0)
                
                # 重置当前群组的账单，但保留汇率和费率设置
                chat_accounting[chat_id] = {
                    'deposits': [],
                    'withdrawals': [],
                    'users': {},
                    'rate': current_rate,
                    'fixed_rate': current_fixed_rate
                }
                
                logger.info(f"已重置群组 {chat_id} 的当日账单，保留费率={current_rate}%和汇率={current_fixed_rate}")
            except Exception as e:
                logger.error(f"重置群组 {chat_id} 账单时出错: {e}", exc_info=True)
        
        # 清理超过7天的记录
        clean_old_records()
    else:
        logger.info(f"日期未变更，当前日期: {current_date}")
    
    logger.info(f"下次检查将在 {RESET_CHECK_INTERVAL} 秒后进行")

def archive_chat_accounting_history(chat_id, date_str):
    """将当天的账单数据归档到历史记录中"""
    global chat_accounting
    
    if not date_str:
        logger.warning(f"无法归档群组 {chat_id} 的账单，日期为空")
        return
    
    try:
        # 确保该群组的数据存在
        if chat_id not in chat_accounting:
            logger.info(f"群组 {chat_id} 没有账单数据，无需归档")
            return
        
        chat_data = chat_accounting[chat_id]
        
        # 初始化历史记录存储
        if 'history' not in chat_data:
            chat_data['history'] = {}
        
        # 把当天的存取款记录复制到历史记录中
        chat_data['history'][date_str] = {
            'deposits': chat_data['deposits'].copy(),
            'withdrawals': chat_data['withdrawals'].copy(),
            'rate': chat_data.get('rate', 0.0),
            'fixed_rate': chat_data.get('fixed_rate', 0.0)
        }
        
        logger.info(f"已归档群组 {chat_id} 在 {date_str} 的账单数据: {len(chat_data['deposits'])} 笔入款, {len(chat_data['withdrawals'])} 笔出款")
        
    except Exception as e:
        logger.error(f"归档群组 {chat_id} 的账单历史时出错: {e}", exc_info=True)

def clean_old_records():
    """清理超过7天的历史记录"""
    global chat_accounting
    
    try:
        # 获取7天前的日期
        seven_days_ago = (datetime.datetime.now(timezone) - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
        
        logger.info(f"开始清理7天前 ({seven_days_ago}) 的历史记录")
        
        # 遍历所有群组
        for chat_id, chat_data in chat_accounting.items():
            if 'history' in chat_data:
                # 统计要删除的记录数量
                records_to_delete = [date for date in chat_data['history'] if date < seven_days_ago]
                
                # 删除超过7天的记录
                for date in records_to_delete:
                    if date in chat_data['history']:
                        del chat_data['history'][date]
                        logger.info(f"已删除群组 {chat_id} 在 {date} 的历史记录")
        
        logger.info("历史记录清理完成")
        
    except Exception as e:
        logger.error(f"清理历史记录时出错: {e}", exc_info=True)

# 将全局操作人集合改为按群组存储的字典
# 键为chat_id，值为该群的操作人集合
group_operators = {}  # 群组特定的操作人
admin_user_id = ADMIN_USER_ID  # Admin user ID who can manage operators

# Timezone setting (China timezone)
timezone = pytz.timezone(TIMEZONE)

def get_current_time():
    """Get the current time in HH:MM format."""
    now = datetime.datetime.now(timezone)
    return now.strftime("%H:%M")

def get_current_date():
    """Get the current date in YYYY-MM-DD format."""
    now = datetime.datetime.now(timezone)
    return now.strftime("%Y-%m-%d")

def is_global_admin(user_id, username):
    """检查用户是否是全局管理员"""
    global admin_user_id
    
    # 检查用户ID是否在admin_user_id列表中
    if isinstance(admin_user_id, list):
        if user_id in admin_user_id:
            return True
    else:
        # 兼容处理单个管理员ID的情况
        if user_id == admin_user_id:
            return True
    
    return False

def is_operator(username, chat_id):
    """检查用户是否是特定群的操作人"""
    global group_operators
    
    # 检查该群是否有操作人记录
    if chat_id in group_operators:
        return username in group_operators[chat_id]
    
    return False

def is_authorized(update: Update) -> bool:
    """Check if user is authorized to use the bot."""
    global admin_user_id, group_operators, authorized_groups
    
    # 获取用户信息
    user = update.effective_user
    chat = update.effective_chat
    
    # 如果是私聊，只允许全局管理员
    if chat.type == 'private':
        return is_global_admin(user.id, user.username)
    
    # 如果是群聊，检查群是否已授权
    if chat.type in ['group', 'supergroup']:
        # 如果用户是全局管理员，允许在任何群使用
        if is_global_admin(user.id, user.username):
            return True
        
        # 如果群组未授权，且命令不是授权群组命令，则拒绝
        if chat.id not in authorized_groups:
            # 检查是否是授权群命令，只有全局管理员可以执行
            if hasattr(update, 'message') and update.message and update.message.text:
                if update.message.text.strip() == '授权群' and is_global_admin(user.id, user.username):
                    return True
            logger.debug(f"群组 {chat.id} ({chat.title}) 未授权")
            return False
        
        # 检查用户是否在该群的操作员列表中
        if is_operator(user.username, chat.id):
            logger.debug(f"用户 {user.id} (@{user.username}) 是群 {chat.id} 的操作员")
            return True
    
    # 否则不允许
    logger.debug(f"用户 {user.id} (@{user.username}) 未授权，管理员: {admin_user_id}, 此群操作员: {group_operators.get(chat.id, set())}")
    return False

# 添加群组列表配置
GROUP_LIST = [
    "1259供凯越 Q群红包 抖音转账",
    "4451供JQ 微信群95",
    "BB团队佳琪供",
    "JQ代理 码接",
    "JQ小代理群",
    "佳琪 大额群码 125",
    "佳琪 自存 二存 三存 四存 五存码",
    "佳琪群引导吧二存三存 24H",
    "公群408供凯越 QQ 微信群红包94",
    "凯越 对接7003直付通汇率10"
]

# 添加回之前删除的process_deposit函数
def process_deposit(update, context, text):
    """处理入款命令：+100 或 +100/7.2 格式"""
    chat_id = update.effective_chat.id
    chat_type = update.effective_chat.type
    chat_title = getattr(update.effective_chat, 'title', 'Private Chat')
    user_id = update.effective_user.id
    username = update.effective_user.username
    
    # 检查群组是否已授权
    if chat_type in ['group', 'supergroup'] and chat_id not in authorized_groups:
        logger.warning(f"未授权群组 {chat_id} ({chat_title}) 尝试使用入款命令")
        update.message.reply_text("❌ 此群组未授权，请联系管理员进行授权")
        return
    
    logger.info(f"处理入款命令: {text}, 聊天: {chat_id} ({chat_title}), 用户: {user_id} (@{username})")
    
    try:
        # 去掉+号
        amount_text = text[1:].strip()
        
        # 检查是否包含汇率设置
        if '/' in amount_text:
            parts = amount_text.split('/', 1)
            amount = float(parts[0])
            rate = float(parts[1])
            logger.info(f"入款带汇率: 金额={amount}, 汇率={rate}")
            
            # 设置汇率
            get_chat_accounting(chat_id)['fixed_rate'] = rate
            
            # 添加入款记录
            add_deposit_record(update, amount)
            
            # 不再发送确认消息，直接显示账单
        else:
            # 普通入款
            amount = float(amount_text)
            logger.info(f"普通入款: 金额={amount}")
            
            # 添加入款记录
            add_deposit_record(update, amount)
            
            # 不再发送确认消息，直接显示账单
        
        # 显示更新后的账单
        logger.info(f"入款完成，显示账单摘要")
        summary(update, context)
        
    except ValueError as e:
        logger.error(f"入款金额格式错误: {e}, 命令: {text}")
        update.message.reply_text("❌ 入款金额必须是数字")
    except Exception as e:
        logger.error(f"处理入款时出错: {e}, 命令: {text}", exc_info=True)
        update.message.reply_text(f"❌ 处理入款时出错: {str(e)}")

# 添加回之前删除的process_withdrawal函数
def process_withdrawal(update, context, text):
    """处理减款命令：-100 或 -100/7.2 格式"""
    chat_id = update.effective_chat.id
    chat_type = update.effective_chat.type
    chat_title = getattr(update.effective_chat, 'title', 'Private Chat')
    user_id = update.effective_user.id
    username = update.effective_user.username
    
    # 检查群组是否已授权
    if chat_type in ['group', 'supergroup'] and chat_id not in authorized_groups:
        logger.warning(f"未授权群组 {chat_id} ({chat_title}) 尝试使用出款命令")
        update.message.reply_text("❌ 此群组未授权，请联系管理员进行授权")
        return
    
    logger.info(f"处理减款命令: {text}, 聊天: {chat_id} ({chat_title}), 用户: {user_id} (@{username})")
    
    try:
        # 去掉-号
        amount_text = text[1:].strip()
        
        # 检查是否包含汇率设置
        if '/' in amount_text:
            parts = amount_text.split('/', 1)
            amount = float(parts[0])
            rate = float(parts[1])
            logger.info(f"减款带汇率: 金额={amount}, 汇率={rate}")
            
            # 设置汇率
            get_chat_accounting(chat_id)['fixed_rate'] = rate
            
            # 添加负入款记录
            add_negative_deposit_record(update, amount)
            
            # 不再发送确认消息，直接显示账单
        else:
            # 普通减款
            amount = float(amount_text)
            logger.info(f"普通减款: 金额={amount}")
            
            # 添加负入款记录
            add_negative_deposit_record(update, amount)
            
            # 不再发送确认消息，直接显示账单
        
        # 显示更新后的账单
        logger.info(f"减款完成，显示账单摘要")
        summary(update, context)
        
    except ValueError as e:
        logger.error(f"减款金额格式错误: {e}, 命令: {text}")
        update.message.reply_text("❌ 减款金额必须是数字")
    except Exception as e:
        logger.error(f"处理减款时出错: {e}, 命令: {text}", exc_info=True)
        update.message.reply_text(f"❌ 处理减款时出错: {str(e)}")

# 添加回之前删除的add_deposit_record函数
def add_deposit_record(update, amount):
    """添加入款记录到chat_accounting"""
    # 获取聊天ID
    chat_id = update.effective_chat.id
    
    # 获取该聊天的账单数据
    chat_data = get_chat_accounting(chat_id)
    
    rate = chat_data.get('fixed_rate', 1.0)
    # 使用除法计算USD等值
    usd_equivalent = amount / rate if rate != 0 else 0
    
    # 获取用户显示名称
    user = update.effective_user
    if user.first_name and user.last_name:
        display_name = f"{user.first_name} {user.last_name}"
    elif user.first_name:
        display_name = user.first_name
    elif user.username:
        display_name = user.username
    else:
        display_name = str(user.id)
    
    # 检查是否是回复某条消息的入款
    responder = None
    if update.message and update.message.reply_to_message:
        # 获取被回复的消息
        reply_msg = update.message.reply_to_message
        
        # 尝试从被回复的消息中提取发送者信息
        if reply_msg.forward_sender_name:  # 如果是转发的消息
            responder = reply_msg.forward_sender_name
        elif reply_msg.caption:  # 如果消息有标题（通常是图片或文件）
            # 尝试从caption中提取用户信息
            if ' ' in reply_msg.caption:
                parts = reply_msg.caption.split(' ')
                if len(parts) >= 3:  # 例如格式: qb280209 179 佳琪
                    responder = parts[2]  # 取第三部分作为用户名
                else:
                    responder = reply_msg.caption  # 使用完整caption
            else:
                responder = reply_msg.caption
        elif reply_msg.from_user:  # 如果有原始发送者
            if reply_msg.from_user.first_name and reply_msg.from_user.last_name:
                responder = f"{reply_msg.from_user.first_name} {reply_msg.from_user.last_name}"
            elif reply_msg.from_user.first_name:
                responder = reply_msg.from_user.first_name
            elif reply_msg.from_user.username:
                responder = reply_msg.from_user.username
            else:
                responder = str(reply_msg.from_user.id)
    
    # 创建入款记录
    deposit_record = {
        'amount': amount,
        'usd_equivalent': usd_equivalent,
        'time': datetime.datetime.now(timezone).strftime('%Y-%m-%d %H:%M:%S'),
        'user': display_name,
        'responder': responder  # 添加回复者信息
    }
    
    # 添加到入款列表
    chat_data['deposits'].append(deposit_record)
    
    # 记录详细日志
    logger.info(f"聊天 {chat_id} 新增入款记录: {json.dumps(deposit_record)}")
    logger.info(f"聊天 {chat_id} 当前入款总数: {len(chat_data['deposits'])}条")
    save_data()

# 添加回之前删除的add_negative_deposit_record函数
def add_negative_deposit_record(update, amount):
    """添加负值入款记录（减款）到chat_accounting"""
    # 获取聊天ID
    chat_id = update.effective_chat.id
    
    # 获取该聊天的账单数据
    chat_data = get_chat_accounting(chat_id)
    
    rate = chat_data.get('fixed_rate', 1.0)
    # 使用除法计算USD等值 (负值)
    usd_equivalent = (-amount) / rate if rate != 0 else 0
    
    # 获取用户显示名称
    user = update.effective_user
    if user.first_name and user.last_name:
        display_name = f"{user.first_name} {user.last_name}"
    elif user.first_name:
        display_name = user.first_name
    elif user.username:
        display_name = user.username
    else:
        display_name = str(user.id)
    
    # 创建负入款记录
    deposit_record = {
        'amount': -amount,  # 负值
        'usd_equivalent': usd_equivalent,
        'time': datetime.datetime.now(timezone).strftime('%Y-%m-%d %H:%M:%S'),
        'user': display_name
    }
    
    # 添加到入款列表
    chat_data['deposits'].append(deposit_record)
    
    # 记录详细日志
    logger.info(f"聊天 {chat_id} 新增减款记录: {json.dumps(deposit_record)}")
    logger.info(f"聊天 {chat_id} 当前入款总数: {len(chat_data['deposits'])}条")
    save_data()

# 添加回之前删除的handle_other_commands函数
def handle_other_commands(update, context, text):
    """处理其他格式的命令，如"回100"、"下发100"等"""
    logger.info(f"处理其他命令: {text}")
    
    # 获取聊天ID
    chat_id = update.effective_chat.id
    
    # 回款命令 - 格式: 回100
    match = re.match(r'^回(\d+(\.\d+)?)$', text)
    if match:
        # 检查权限
        if not is_authorized(update):
            logger.warning(f"未授权用户 {update.effective_user.id} (@{update.effective_user.username}) 尝试使用回款命令")
            return True
            
        try:
            amount = float(match.group(1))
            logger.info(f"处理回款: {amount} USDT")
            
            # 记录出款
            add_withdrawal_record(update, amount)
            
            # 获取该聊天的账单数据用于显示汇率
            chat_id = update.effective_chat.id
            chat_data = get_chat_accounting(chat_id)
            rate = chat_data.get('fixed_rate', 1.0)
            local_amount = amount * rate
            
            # 发送确认消息 - 使用普通文本而不是emoji
            update.message.reply_text("已回款")
            
            # 显示更新后的账单
            summary(update, context)
            return True
        except Exception as e:
            logger.error(f"处理回款出错: {e}", exc_info=True)
            update.message.reply_text(f"处理回款出错: {str(e)}")
            return True
    
    # 下发命令 - 格式: 下发100
    match = re.match(r'^下发(\d+(\.\d+)?)$', text)
    if match:
        # 检查权限
        if not is_authorized(update):
            logger.warning(f"未授权用户 {update.effective_user.id} (@{update.effective_user.username}) 尝试使用下发命令")
            return True
            
        try:
            amount = float(match.group(1))
            logger.info(f"处理下发: {amount} USDT")
            
            # 记录出款
            add_withdrawal_record(update, amount)
            
            # 获取该聊天的账单数据用于显示汇率
            chat_id = update.effective_chat.id
            chat_data = get_chat_accounting(chat_id)
            rate = chat_data.get('fixed_rate', 1.0)
            local_amount = amount * rate
            
            # 发送确认消息 - 使用普通文本而不是emoji
            update.message.reply_text("已下发")
            
            # 显示更新后的账单
            summary(update, context)
            return True
        except Exception as e:
            logger.error(f"处理下发出错: {e}", exc_info=True)
            update.message.reply_text(f"处理下发出错: {str(e)}")
            return True
    
    # 设置费率 - 格式: 设置费率5%
    match = re.match(r'^设置费率(\d+(\.\d+)?)%$', text)
    if match:
        # 检查权限
        if not is_authorized(update):
            logger.warning(f"未授权用户 {update.effective_user.id} (@{update.effective_user.username}) 尝试使用设置费率命令")
            return True
            
        try:
            rate = float(match.group(1))
            logger.info(f"设置费率: {rate}%")
            
            # 设置费率
            get_chat_accounting(chat_id)['rate'] = rate
            
            # 发送确认消息
            update.message.reply_text(f"✅ 已设置费率: {rate}%")
            
            # 显示更新后的账单
            summary(update, context)
            return True
        except Exception as e:
            logger.error(f"设置费率出错: {e}", exc_info=True)
            update.message.reply_text(f"❌ 设置费率出错: {str(e)}")
            return True
    
    # 设置汇率 - 格式: 设置汇率7.2
    match = re.match(r'^设置汇率(\d+(\.\d+)?)$', text)
    if match:
        # 检查权限
        if not is_authorized(update):
            logger.warning(f"未授权用户 {update.effective_user.id} (@{update.effective_user.username}) 尝试使用设置汇率命令")
            return True
            
        try:
            rate = float(match.group(1))
            logger.info(f"设置汇率: {rate}")
            
            # 设置汇率
            get_chat_accounting(chat_id)['fixed_rate'] = rate
            
            # 发送确认消息
            update.message.reply_text(f"✅ 已设置汇率: {rate}")
            
            # 显示更新后的账单
            summary(update, context)
            return True
        except Exception as e:
            logger.error(f"设置汇率出错: {e}", exc_info=True)
            update.message.reply_text(f"❌ 设置汇率出错: {str(e)}")
            return True
    
    # 导出昨日账单命令 - 允许所有用户使用
    if text == '导出昨日账单':
        try:
            export_yesterday_bill(update, context)
            return True
        except Exception as e:
            logger.error(f"导出昨日账单出错: {e}", exc_info=True)
            update.message.reply_text(f"❌ 导出昨日账单出错: {str(e)}")
            return True
    
    # 如果没有匹配任何已知命令模式
    logger.info(f"消息不匹配任何已知命令模式: {text}")
    return False

# 添加回之前删除的handle_admin_commands函数
def handle_admin_commands(update, context, text):
    """处理管理员专属命令"""
    global group_operators
    
    user_id = update.effective_user.id
    username = update.effective_user.username
    chat_id = update.effective_chat.id
    
    # 检查用户是否为全局管理员，只有全局管理员可以执行以下命令
    if not is_global_admin(user_id, username):
        logger.warning(f"未授权用户 {user_id} (@{username}) 尝试使用管理员命令: {text}")
        update.message.reply_text("❌ 只有全局管理员才能执行此命令")
        return True
    
    # 重置授权人
    if text == '重置授权人':
        # 确保群组在字典中存在
        if chat_id not in group_operators:
            group_operators[chat_id] = set()
        else:
            group_operators[chat_id].clear()
        
        # 添加初始操作人
        for op in INITIAL_OPERATORS:
            group_operators[chat_id].add(op)
            
        operators_list = ", ".join(f"@{op}" for op in group_operators[chat_id]) if group_operators[chat_id] else "无"
        logger.info(f"已重置群 {chat_id} 的授权人: {operators_list}")
        update.message.reply_text(f'已重置此群授权人: {operators_list}')
        save_data()
        return True
    
    # 通过回复消息设置操作人
    if text.strip() == '设置操作人' and update.message.reply_to_message:
        replied_user = update.message.reply_to_message.from_user
        if replied_user and replied_user.username:
            # 确保群组在字典中存在
            if chat_id not in group_operators:
                group_operators[chat_id] = set()
                
            group_operators[chat_id].add(replied_user.username)
            logger.info(f"已通过回复消息添加群 {chat_id} 的操作人: @{replied_user.username}")
            update.message.reply_text(f'已添加此群操作人: @{replied_user.username}')
            save_data()
            return True
        else:
            update.message.reply_text("无法设置操作人：被回复的用户没有用户名")
            return True
    
    # 添加操作人
    match = re.match(r'^设置操作人\s+@(\w+)$', text)
    if match:
        username = match.group(1)
        
        # 确保群组在字典中存在
        if chat_id not in group_operators:
            group_operators[chat_id] = set()
            
        group_operators[chat_id].add(username)
        logger.info(f"已添加群 {chat_id} 的操作人: @{username}")
        update.message.reply_text(f'已添加此群操作人: @{username}')
        save_data()
        return True
    
    # 通过回复消息删除操作人
    if text.strip() == '删除操作人' and update.message.reply_to_message:
        replied_user = update.message.reply_to_message.from_user
        if replied_user and replied_user.username:
            if chat_id in group_operators and replied_user.username in group_operators[chat_id]:
                group_operators[chat_id].remove(replied_user.username)
                logger.info(f"已通过回复消息删除群 {chat_id} 的操作人: @{replied_user.username}")
                update.message.reply_text(f'已删除此群操作人: @{replied_user.username}')
            else:
                logger.info(f"尝试删除不存在的群 {chat_id} 操作人: @{replied_user.username}")
                update.message.reply_text(f'此群操作人 @{replied_user.username} 不存在')
            save_data()
            return True
        else:
            update.message.reply_text("无法删除操作人：被回复的用户没有用户名")
            return True
    
    # 删除操作人
    match = re.match(r'^删除操作人\s+@(\w+)$', text)
    if match:
        username = match.group(1)
        if chat_id in group_operators and username in group_operators[chat_id]:
            group_operators[chat_id].remove(username)
            logger.info(f"已删除群 {chat_id} 的操作人: @{username}")
            update.message.reply_text(f'已删除此群操作人: @{username}')
        else:
            logger.info(f"尝试删除不存在的群 {chat_id} 操作人: @{username}")
            update.message.reply_text(f'此群操作人 @{username} 不存在')
        save_data()
        return True
    
    # 清空操作人
    if text == '清空操作人':
        if chat_id in group_operators:
            group_operators[chat_id].clear()
        else:
            group_operators[chat_id] = set()
            
        logger.info(f"已清空群 {chat_id} 的所有操作人")
        update.message.reply_text('已清空此群所有操作人')
        save_data()
        return True
    
    # 显示操作人
    if text == '显示操作人':
        operators_list = ", ".join(f"@{op}" for op in group_operators.get(chat_id, set())) if group_operators.get(chat_id, set()) else "无"
        logger.info(f"群 {chat_id} 当前操作人: {operators_list}")
        update.message.reply_text(f'此群当前操作人: {operators_list}')
        return True
    
    return False

# 添加回之前删除的add_withdrawal_record函数
def add_withdrawal_record(update, amount):
    """添加出款记录到chat_accounting，amount为USDT金额"""
    # 获取聊天ID
    chat_id = update.effective_chat.id
    
    # 获取该聊天的账单数据
    chat_data = get_chat_accounting(chat_id)
    
    rate = chat_data.get('fixed_rate', 1.0)
    # 将USDT金额乘以汇率得到本地货币金额
    local_amount = amount * rate
    
    # 获取用户显示名称
    user = update.effective_user
    if user.first_name and user.last_name:
        display_name = f"{user.first_name} {user.last_name}"
    elif user.first_name:
        display_name = user.first_name
    elif user.username:
        display_name = user.username
    else:
        display_name = str(user.id)
    
    # 创建出款记录
    withdrawal_record = {
        'amount': local_amount,  # 存储本地货币金额
        'usd_equivalent': amount,  # 存储原始USDT金额
        'time': datetime.datetime.now(timezone).strftime('%Y-%m-%d %H:%M:%S'),
        'user': display_name
    }
    
    # 添加到出款列表
    chat_data['withdrawals'].append(withdrawal_record)
    
    # 记录详细日志
    logger.info(f"聊天 {chat_id} 新增出款记录: {json.dumps(withdrawal_record)}")
    logger.info(f"聊天 {chat_id} 当前出款总数: {len(chat_data['withdrawals'])}条")
    save_data()

def handle_text_message(update: Update, context: CallbackContext) -> None:
    """处理文本消息，检查特殊格式的命令"""
    global processed_message_ids
    
    if update.message is None or update.message.text is None:
        return
    
    # 检查消息ID是否已被处理过，如果是则跳过
    message_id = update.message.message_id
    if message_id in processed_message_ids:
        logger.debug(f"跳过已处理的消息ID: {message_id}")
        return
    
    # 将当前消息ID添加到已处理集合中
    processed_message_ids.add(message_id)
    
    # 限制缓存大小，如果超过最大值，删除最早的消息ID
    if len(processed_message_ids) > MAX_PROCESSED_MESSAGES:
        processed_message_ids = set(list(processed_message_ids)[-MAX_PROCESSED_MESSAGES:])
    
    chat_id = update.effective_chat.id
    chat_type = update.effective_chat.type
    chat_title = getattr(update.effective_chat, 'title', 'Private Chat')
    user_id = update.effective_user.id
    username = update.effective_user.username
    message_text = update.message.text
    
    logger.debug(f"收到消息: '{message_text}' 来自 {chat_id} ({chat_title}), 用户: {user_id} (@{username})")
    
    # 检查是否是"授权群"指令，只有全局管理员可以执行
    if message_text.strip() == '授权群':
        # 检查用户是否是全局管理员
        if is_global_admin(user_id, username):
            # 检查是否是群聊
            if chat_type in ['group', 'supergroup']:
                # 添加到授权群组列表
                authorized_groups.add(chat_id)
                logger.info(f"群组 {chat_id} ({chat_title}) 已授权")
                update.message.reply_text(f"✅ 此群组已成功授权，可以开始使用机器人功能")
            else:
                update.message.reply_text("❌ 此命令只能在群组中使用")
        else:
            update.message.reply_text("❌ 只有管理员和操作员才能授权群组")
        return
    
    # 检查群组是否已授权
    if chat_type in ['group', 'supergroup'] and chat_id not in authorized_groups:
        # 如果是全局管理员发送的消息，允许处理
        if not is_global_admin(user_id, username):
            logger.debug(f"忽略未授权群组 {chat_id} ({chat_title}) 的消息")
            return
    
    # 处理快捷指令
    # 检查是否是群聊中的机器人命令
    if chat_type in ['group', 'supergroup'] and '@' in message_text:
        # 提取命令和机器人名称
        parts = message_text.split('@', 1)
        command = parts[0]
        bot_username = parts[1] if len(parts) > 1 else ''
        
        # 如果命令是针对其他机器人的，忽略
        if bot_username and bot_username != context.bot.username:
            return
        
        # 如果是对当前机器人的命令，去掉@部分
        message_text = command
    
    # 处理USDT地址查询
    if message_text.strip() == "查询" and update.message.reply_to_message:
        logger.info(f"检测到USDT查询请求")
        handle_usdt_query(update, context)
        return
    
    # 处理管理员命令
    if message_text.strip() in ['设置操作人', '删除操作人', '显示操作人', '重置授权人', '清空操作人'] or message_text.strip().startswith('设置操作人') or message_text.strip().startswith('删除操作人'):
        # 验证用户是否是全局管理员（不包括操作员）
        if is_global_admin(user_id, username):
            handle_admin_commands(update, context, message_text.strip())
        else:
            logger.warning(f"非全局管理员 {user_id} (@{username}) 尝试使用管理员命令: {message_text.strip()}")
            update.message.reply_text("❌ 只有全局管理员才能执行此命令")
        return
    
    # 处理财务类命令 - 只有全局管理员或操作员可以使用
    if message_text.strip() in ['财务', '财务账单', '财务统计', '显示财务', '账单统计', '财务查账'] or message_text.strip().startswith('财务'):
        if is_authorized(update):
            # 这里可以调用相应的财务账单功能
            if message_text.strip() == '财务' or message_text.strip() == '财务账单':
                summary(update, context)
            elif message_text.strip() == '财务统计':
                show_financial_summary(update, context)
            elif message_text.strip() == '财务查账':
                # 使用日期选择功能直接查看账单
                logger.info("显示财务查账")
                send_date_selection_first(update, context)
            else:
                # 对于其他财务命令，默认显示财务摘要
                summary(update, context)
        else:
            logger.warning(f"未授权用户 {user_id} (@{username}) 尝试使用财务账单功能")
            update.message.reply_text("❌ 只有管理员和操作员才能使用财务账单功能")
        return
    
    # 处理入款指令：+100 格式
    if message_text.strip().startswith('+'):
        if is_authorized(update):
            process_deposit(update, context, message_text)
        else:
            logger.warning(f"未授权用户 {user_id} (@{username}) 尝试使用入款命令")
        return
    
    # 处理出款指令：-100 格式
    if message_text.strip().startswith('-'):
        if is_authorized(update):
            process_withdrawal(update, context, message_text)
        else:
            logger.warning(f"未授权用户 {user_id} (@{username}) 尝试使用出款命令")
        return
    
    # 处理"导出全部账单"命令 - 允许所有用户使用
    if message_text.strip() == '导出全部账单':
        handle_export_all_bills_command(update, context)
        return
    
    # 处理计算器命令
    if message_text.strip().startswith('计算') or message_text.strip().startswith('calc'):
        calculation_result = handle_calculator(message_text)
        update.message.reply_text(calculation_result)
        return
    
    # 检查是否为数学表达式 (例如: 2+2, 5*3, etc.)
    if is_mathematical_expression(message_text):
        calculation_result = handle_calculator(message_text)
        update.message.reply_text(calculation_result)
        return
        
    # 处理其他命令格式
    if handle_other_commands(update, context, message_text):
        return
    
    # 如果是群聊中的消息，不处理普通消息
    if chat_type in ['group', 'supergroup']:
        return
    
    # 对于私聊，如果不是命令，提供帮助信息
    if chat_type == 'private':
        logger.debug(f"收到非命令消息: '{message_text}'")
        help_command(update, context)
        return

def handle_export_all_bills_command(update_or_query, context: CallbackContext) -> None:
    """处理文本命令'导出全部账单'"""
    logger.info("处理导出全部账单文本命令")
    
    # 检查是否是回调查询
    is_callback = hasattr(update_or_query, 'callback_query')
    
    if is_callback:
        query = update_or_query
        chat_id = query.message.chat_id
        update_object = query
    else:
        # 常规消息更新
        chat_id = update_or_query.effective_chat.id
        update_object = update_or_query
    
    try:
        # 获取群组信息
        chat = context.bot.get_chat(chat_id)
        chat_title = chat.title if chat.type in ['group', 'supergroup'] else "私聊"
        
        # 获取最近7天的日期列表
        dates = []
        for i in range(7):
            date = (datetime.datetime.now(timezone) - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
            dates.append(date)
        
        # 找出有记录的日期
        dates_with_records = []
        
        # 获取该聊天的账单数据
        chat_data = get_chat_accounting(chat_id)
        
        for date_str in dates:
            # 检查是否有该日期的记录
            has_records = False
            
            # 检查存款记录
            for deposit in chat_data['deposits']:
                record_date = deposit['time'].split(' ')[0]
                if record_date == date_str:
                    has_records = True
                    break
            
            # 检查提款记录，如果还没有找到记录
            if not has_records:
                for withdrawal in chat_data['withdrawals']:
                    record_date = withdrawal['time'].split(' ')[0]
                    if record_date == date_str:
                        has_records = True
                        break
            
            # 如果有该日期的记录，添加到列表
            if has_records:
                dates_with_records.append(date_str)
        
        # 如果没有找到任何有记录的日期
        if not dates_with_records:
            if is_callback:
                query.edit_message_text(f"{chat_title} 最近7天内没有任何记账记录")
            else:
                update_object.message.reply_text(f"{chat_title} 最近7天内没有任何记账记录")
            return
        
        # 创建日期选择按钮
        keyboard = []
        row = []
        for i, date in enumerate(dates_with_records):
            row.append(InlineKeyboardButton(date, callback_data=f"export_date_{date}_{chat_id}"))
            if (i + 1) % 2 == 0 or i == len(dates_with_records) - 1:  # 每两个日期一行，或者是最后一个日期
                keyboard.append(row)
                row = []
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # 发送日期选择界面
        if is_callback:
            query.edit_message_text(f"请选择要导出的日期:", reply_markup=reply_markup)
        else:
            update_object.message.reply_text(f"请选择要导出的日期:", reply_markup=reply_markup)
        
    except Exception as e:
        logger.error(f"展示导出日期选择界面时出错: {e}", exc_info=True)
        if is_callback:
            query.edit_message_text(f"显示日期选择界面时出错: {str(e)}")
        else:
            update_object.message.reply_text(f"显示日期选择界面时出错: {str(e)}")

def export_chat_all_days_to_txt(chat_id, chat_title, summary_text, date_list):
    """导出指定聊天在最近7天内的账单数据为TXT文件"""
    try:
        # 创建导出目录，如果不存在
        export_dir = "exports"
        if not os.path.exists(export_dir):
            os.makedirs(export_dir)
        
        # 创建安全的文件名
        safe_name = "".join([c if c.isalnum() else "_" for c in chat_title])
        timestamp = datetime.datetime.now(timezone).strftime("%Y%m%d_%H%M%S")
        file_path = os.path.join(export_dir, f"{safe_name}_7days_{timestamp}.txt")
        
        # 获取该聊天的账单数据
        chat_data = get_chat_accounting(chat_id)
        
        # 准备文件内容
        content = f"===== {chat_title} 财务账单 =====\n"
        content += f"导出时间: {datetime.datetime.now(timezone).strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        # 添加摘要部分
        content += summary_text + "\n"
        
        # 添加明细部分 - 按日期组织
        all_deposits = chat_data['deposits']
        all_withdrawals = chat_data['withdrawals']
        
        # 汇率
        rate = chat_data.get('fixed_rate', 1.0)
        
        # 按日期组织明细数据
        content += "\n===== 按日期明细 =====\n"
        for date_str in date_list:
            # 筛选指定日期的记录
            date_deposits = [d for d in all_deposits if d['time'].split(' ')[0] == date_str]
            date_withdrawals = [w for w in all_withdrawals if w['time'].split(' ')[0] == date_str]
            
            if not date_deposits and not date_withdrawals:
                continue  # 如果这一天没有记录，跳过
            
            # 添加日期标题
            content += f"\n----- {date_str} -----\n"
            
            # 入款记录
            content += "入款:\n"
            if date_deposits:
                for i, deposit in enumerate(sorted(date_deposits, key=lambda x: x.get('time', ''), reverse=True), 1):
                    amount = deposit['amount']
                    username = deposit['user']
                    time_str = deposit.get('time', '')
                    time_parts = time_str.split(' ')
                    if len(time_parts) > 1:
                        time_only = time_parts[1]
                    else:
                        time_only = "未知时间"
                    usd_equivalent = amount / rate if rate != 0 else 0
                    
                    content += f"  {i}. {time_only}, {username}, {amount:.2f}, USD等值: {usd_equivalent:.2f}\n"
            else:
                content += "  暂无入款记录\n"
            
            # 出款记录
            content += "出款:\n"
            if date_withdrawals:
                for i, withdrawal in enumerate(sorted(date_withdrawals, key=lambda x: x.get('time', ''), reverse=True), 1):
                    amount = withdrawal['amount']
                    username = withdrawal['user']
                    time_str = withdrawal.get('time', '')
                    time_parts = time_str.split(' ')
                    if len(time_parts) > 1:
                        time_only = time_parts[1]
                    else:
                        time_only = "未知时间"
                    usd_equivalent = withdrawal['usd_equivalent']
                    
                    content += f"  {i}. {time_only}, {username}, {amount:.2f}, USD等值: {usd_equivalent:.2f}\n"
            else:
                content += "  暂无出款记录\n"
        
        # 写入文件
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"已导出群组 {chat_id} 的账单数据到 {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"导出账单数据时出错: {e}", exc_info=True)
        return None

def handle_calculator(message_text):
    """处理计算器功能"""
    # 如果消息以"计算"或"calc"开头，去掉这个前缀
    if message_text.startswith('计算'):
        expression = message_text[2:].strip()
    elif message_text.startswith('calc'):
        expression = message_text[4:].strip()
    else:
        expression = message_text.strip()

    try:
        # 安全地评估数学表达式
        # 只允许安全的数学运算符和函数
        allowed_names = {
            'abs': abs, 'round': round, 'min': min, 'max': max,
            'pow': pow, 'sum': sum, 'int': int, 'float': float,
        }
        
        # 使用Python的ast模块确保表达式安全
        import ast
        import operator
        import math
        
        # 添加常用数学函数
        allowed_names.update({
            'sin': math.sin, 'cos': math.cos, 'tan': math.tan,
            'sqrt': math.sqrt, 'log': math.log, 'log10': math.log10,
            'exp': math.exp, 'pi': math.pi, 'e': math.e,
            'radians': math.radians, 'degrees': math.degrees,
        })
        
        # 定义运算符映射
        operators = {
            ast.Add: operator.add, ast.Sub: operator.sub,
            ast.Mult: operator.mul, ast.Div: operator.truediv,
            ast.Pow: operator.pow, ast.BitXor: operator.xor,
            ast.USub: operator.neg, ast.UAdd: operator.pos,
            ast.Mod: operator.mod, ast.FloorDiv: operator.floordiv,
        }
        
        # 定义安全的表达式计算函数
        def safe_eval(node):
            if isinstance(node, ast.Num):
                return node.n
            elif isinstance(node, ast.BinOp):
                return operators[type(node.op)](safe_eval(node.left), safe_eval(node.right))
            elif isinstance(node, ast.UnaryOp):
                return operators[type(node.op)](safe_eval(node.operand))
            elif isinstance(node, ast.Call):
                func_name = node.func.id
                if func_name not in allowed_names:
                    raise ValueError(f"函数 '{func_name}' 不允许使用")
                args = [safe_eval(arg) for arg in node.args]
                return allowed_names[func_name](*args)
            elif isinstance(node, ast.Name):
                if node.id not in allowed_names:
                    raise ValueError(f"变量 '{node.id}' 不允许使用")
                return allowed_names[node.id]
            elif isinstance(node, ast.Constant):  # Python 3.8+
                return node.value
            else:
                raise TypeError(f"不支持的表达式类型: {type(node)}")
        
        # 解析表达式
        parsed_expr = ast.parse(expression, mode='eval').body
        result = safe_eval(parsed_expr)
        
        # 格式化结果，避免显示过多小数位
        if isinstance(result, float):
            # 移除尾随的零
            result_str = f"{result:.10f}".rstrip('0').rstrip('.') if '.' in f"{result:.10f}" else f"{result}"
        else:
            result_str = str(result)
        
        return f"计算结果: {expression} = {result_str}"
    except Exception as e:
        logger.error(f"计算表达式时出错: {e}", exc_info=True)
        return f"计算错误: {str(e)}"

def extract_usdt_address(text):
    """从文本中提取USDT地址"""
    # 尝试匹配以太坊地址（0x开头，42个字符长度）
    eth_match = re.search(r'0x[a-fA-F0-9]{40}', text)
    if eth_match:
        return eth_match.group(0)
    
    # 尝试匹配波场地址（T开头，34个字符长度）
    trx_match = re.search(r'T[a-zA-Z0-9]{33}', text)
    if trx_match:
        return trx_match.group(0)
    
    # 如果都不匹配，尝试提取任何可能是地址的长字符串
    # 这只是一个备选方案，可能会误判
    address_match = re.search(r'[a-zA-Z0-9]{30,}', text)
    if address_match:
        return address_match.group(0)
    
    return None

def is_mathematical_expression(text):
    """检查文本是否是一个数学表达式"""
    import re
    # 简单检查是否包含数字和运算符
    text = text.strip()
    # 匹配包含数字和至少一个运算符的表达式
    pattern = r'^[\d\s\+\-\*\/\(\)\.\,\^\%]+$'
    if re.match(pattern, text):
        # 进一步检查是否至少包含一个运算符
        return any(op in text for op in ['+', '-', '*', '/', '(', ')', '^', '%'])
    return False

def help_command(update: Update, context: CallbackContext) -> None:
    """发送帮助信息"""
    # 检查是否是管理员
    is_admin = update.effective_user.id == admin_user_id
    is_operator = update.effective_user.username in group_operators.get(chat_id, set())
    is_manager = is_admin or is_operator
    
    # 检查是否是群聊
    is_group = update.effective_chat.type in ['group', 'supergroup']
    
    # 对于群聊，发送简短帮助信息
    if is_group:
        brief_help = "📱 *账务机器人使用指南*\n\n"
        brief_help += "查看完整帮助，请私聊机器人发送 /help\n"
        brief_help += "💰 可用的基本功能: 计算器，导出账单，查询余额\n"
        
        if is_manager:
            brief_help += "⚙️ 管理员可用: 入款、出款、财务统计等功能\n"
        
        brief_help += "\n💬 使用示例: 计算 1+2, 导出昨日账单, 导出全部账单"
        
        update.message.reply_text(brief_help, parse_mode=ParseMode.MARKDOWN)
        return
    
    # 对于私聊，发送完整帮助信息
    help_text = "🤖 账务机器人使用指南 🤖\n\n"
    
    # 添加普通用户命令
    help_text += "📊 *普通用户可用命令*: \n"
    help_text += "🧮 `1+2*3` - 直接输入数学表达式进行计算\n"
    help_text += "🧮 `计算 1+2*3` - 使用计算命令计算表达式\n"
    help_text += "📋 `导出昨日账单` - 导出昨天的账单记录\n" 
    help_text += "📋 `导出全部账单` - 导出最近7天的账单记录\n"
    help_text += "💰 回复USDT地址并发送 `查询` - 查询USDT余额\n\n"
    
    # 管理员和操作员命令
    if is_manager:
        help_text += "🔑 *管理员/操作员命令*: \n"
        help_text += "➕ `+100` - 记录入款100元\n"
        help_text += "➕ `+100/7.2` - 记录入款100美元，汇率7.2\n"
        help_text += "➖ `-100` - 记录出款100元\n"
        help_text += "➖ `-100/7.2` - 记录出款100美元，汇率7.2\n"
        help_text += "💸 `回100` - 记录回款100元并显示账单\n"
        help_text += "💸 `下发100` - 记录下发100元并显示账单\n"
        help_text += "📊 `财务` 或 `财务账单` - 显示当前财务状况\n"
        help_text += "📊 `财务统计` - 显示财务账单统计信息\n"
        help_text += "📊 `财务查账` - 选择日期查看财务账单\n"
        help_text += "⚙️ `设置费率5%` - 设置费率为5%\n"
        help_text += "⚙️ `设置汇率7.2` - 设置美元汇率为7.2\n"
        
        # 只有管理员才能使用的命令
        if is_admin:
            help_text += "\n🔐 *管理员专属命令*: \n"
            help_text += "👥 `授权群` - 授权当前群组使用机器人\n"
            help_text += "👤 `设置操作人 @xxx` - 添加群管理\n"
            help_text += "👤 回复某人消息并发送 `设置操作人` - 设置被回复的用户为操作人\n"
            help_text += "👤 `删除操作人 @xxx` - 删除群管理\n"
            help_text += "👥 `显示操作人` - 显示当前操作人列表\n"
            help_text += "🔄 `重置授权人` - 重置操作人为初始状态\n"
            help_text += "🧹 `清空操作人` - 清空所有操作人\n"
            help_text += "🔄 `/reset` - 重置当前群组的账单\n"
    
    # 发送帮助消息
    update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)

def start(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /start is issued."""
    if not is_authorized(update):
        return
        
    update.message.reply_text('记账机器人已启动，使用 /help 查看命令.')

def generate_chat_all_days_summary(chat_id, chat_title, date_list):
    """生成指定聊天在最近7天内的账单摘要"""
    logger.info(f"为聊天 {chat_id} ({chat_title}) 生成最近7天的账单摘要")
    
    # 获取该聊天的账单数据
    chat_data = get_chat_accounting(chat_id)
    
    # 按照用户要求的模板格式生成账单摘要
    summary_text = f"====== {chat_title} 最近7天账单 ======\n\n"
    
    # 计算总体统计数据
    all_deposits = chat_data['deposits']
    all_withdrawals = chat_data['withdrawals']
    
    deposit_total = sum(deposit['amount'] for deposit in all_deposits)
    deposit_count = len(all_deposits)
    
    withdrawal_total = sum(withdraw['amount'] for withdraw in all_withdrawals)
    withdrawal_count = len(all_withdrawals)
    
    # 汇率和费率部分
    rate = chat_data.get('fixed_rate', 1.0)
    fee_rate = chat_data.get('rate', 0.0)
    
    # 计算实际金额 - 使用除法计算
    actual_amount = deposit_total / rate if rate != 0 else 0
    
    # 计算应下发金额
    to_be_withdrawn = actual_amount
    already_withdrawn = withdrawal_total
    not_yet_withdrawn = to_be_withdrawn - already_withdrawn
    
    # 添加总体统计
    summary_text += f"总计统计：\n"
    summary_text += f"总入款：{deposit_count}笔，共计 {deposit_total:.2f}\n"
    summary_text += f"总下发：{withdrawal_count}笔，共计 {withdrawal_total:.2f}\n"
    summary_text += f"费率：{fee_rate}%\n"
    summary_text += f"固定汇率：{rate}\n"
    summary_text += f"应下发：{to_be_withdrawn:.2f}\n"
    summary_text += f"已下发：{already_withdrawn:.2f}\n"
    summary_text += f"未下发：{not_yet_withdrawn:.2f}\n\n"
    
    # 为每一天生成单独的统计
    summary_text += f"按日期统计：\n"
    
    for date_str in date_list:
        # 筛选指定日期的记录
        date_deposits = [d for d in all_deposits if d['time'].split(' ')[0] == date_str]
        date_withdrawals = [w for w in all_withdrawals if w['time'].split(' ')[0] == date_str]
        
        if not date_deposits and not date_withdrawals:
            continue  # 如果这一天没有记录，跳过
        
        # 该日期的统计数据
        day_deposit_total = sum(deposit['amount'] for deposit in date_deposits)
        day_deposit_count = len(date_deposits)
        
        day_withdrawal_total = sum(withdraw['amount'] for withdraw in date_withdrawals)
        day_withdrawal_count = len(date_withdrawals)
        
        # 添加日期标题
        summary_text += f"\n----- {date_str} -----\n"
        summary_text += f"入款：{day_deposit_count}笔，共计 {day_deposit_total:.2f}\n"
        
        # 统计每个用户在该日期的入款
        if day_deposit_count > 0:
            day_user_deposits = {}
            for deposit in date_deposits:
                username = deposit['user']
                amount = deposit['amount']
                if username not in day_user_deposits:
                    day_user_deposits[username] = 0
                day_user_deposits[username] += amount
            
            for username, amount in day_user_deposits.items():
                summary_text += f"  {username}: {amount:.2f}\n"
        
        summary_text += f"下发：{day_withdrawal_count}笔，共计 {day_withdrawal_total:.2f}\n"
        
        # 统计每个用户在该日期的出款
        if day_withdrawal_count > 0:
            day_user_withdrawals = {}
            for withdrawal in date_withdrawals:
                username = withdrawal['user']
                amount = withdrawal['amount']
                if username not in day_user_withdrawals:
                    day_user_withdrawals[username] = 0
                day_user_withdrawals[username] += amount
            
            for username, amount in day_user_withdrawals.items():
                summary_text += f"  {username}: {amount:.2f}\n"
    
    # 添加关于导出文件包含详细交易记录的提示
    summary_text += f"\n注：导出的文件中将包含每笔交易的详细记录。\n"
    
    return summary_text

def export_chat_all_days_to_txt(chat_id, chat_title, summary_text, date_list):
    """导出指定聊天在最近7天内的账单数据为TXT文件"""
    try:
        # 创建导出目录，如果不存在
        export_dir = "exports"
        if not os.path.exists(export_dir):
            os.makedirs(export_dir)
        
        # 创建安全的文件名
        safe_name = "".join([c if c.isalnum() else "_" for c in chat_title])
        timestamp = datetime.datetime.now(timezone).strftime("%Y%m%d_%H%M%S")
        file_path = os.path.join(export_dir, f"{safe_name}_7days_{timestamp}.txt")
        
        # 获取该聊天的账单数据
        chat_data = get_chat_accounting(chat_id)
        
        # 准备文件内容
        content = f"===== {chat_title} 财务账单 =====\n"
        content += f"导出时间: {datetime.datetime.now(timezone).strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        # 添加摘要部分
        content += summary_text + "\n"
        
        # 添加明细部分 - 按日期组织
        all_deposits = chat_data['deposits']
        all_withdrawals = chat_data['withdrawals']
        
        # 汇率
        rate = chat_data.get('fixed_rate', 1.0)
        
        # 按日期组织明细数据
        content += "\n===== 按日期明细 =====\n"
        for date_str in date_list:
            # 筛选指定日期的记录
            date_deposits = [d for d in all_deposits if d['time'].split(' ')[0] == date_str]
            date_withdrawals = [w for w in all_withdrawals if w['time'].split(' ')[0] == date_str]
            
            if not date_deposits and not date_withdrawals:
                continue  # 如果这一天没有记录，跳过
            
            # 添加日期标题
            content += f"\n----- {date_str} -----\n"
            
            # 入款记录
            content += "入款:\n"
            if date_deposits:
                for i, deposit in enumerate(sorted(date_deposits, key=lambda x: x.get('time', ''), reverse=True), 1):
                    amount = deposit['amount']
                    username = deposit['user']
                    time_str = deposit.get('time', '')
                    time_parts = time_str.split(' ')
                    if len(time_parts) > 1:
                        time_only = time_parts[1]
                    else:
                        time_only = "未知时间"
                    usd_equivalent = amount / rate if rate != 0 else 0
                    
                    content += f"  {i}. {time_only}, {username}, {amount:.2f}, USD等值: {usd_equivalent:.2f}\n"
            else:
                content += "  暂无入款记录\n"
            
            # 出款记录
            content += "出款:\n"
            if date_withdrawals:
                for i, withdrawal in enumerate(sorted(date_withdrawals, key=lambda x: x.get('time', ''), reverse=True), 1):
                    amount = withdrawal['amount']
                    username = withdrawal['user']
                    time_str = withdrawal.get('time', '')
                    time_parts = time_str.split(' ')
                    if len(time_parts) > 1:
                        time_only = time_parts[1]
                    else:
                        time_only = "未知时间"
                    usd_equivalent = withdrawal['usd_equivalent']
                    
                    content += f"  {i}. {time_only}, {username}, {amount:.2f}, USD等值: {usd_equivalent:.2f}\n"
            else:
                content += "  暂无出款记录\n"
        
        # 写入文件
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"已导出群组 {chat_id} 的账单数据到 {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"导出账单数据时出错: {e}", exc_info=True)
        return None

# 导出指定日期所有群组的统计数据
def export_all_groups_statistics(query, context, date_str):
    """导出指定日期所有群组的统计数据"""
    logger.info(f"导出 {date_str} 所有群组统计数据")
    
    # 创建返回按钮
    keyboard = [[InlineKeyboardButton("返回", callback_data="first_page")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # 更新消息，表示正在导出
    query.edit_message_text(f"正在导出 {date_str} 所有群组的统计数据...", reply_markup=reply_markup)
    
    # 查找所有在该日期有记录的群组
    groups_with_records = []
    for chat_id, chat_data in chat_accounting.items():
        try:
            chat = context.bot.get_chat(chat_id)
            chat_title = chat.title if chat.type in ['group', 'supergroup'] else f"私聊_{chat_id}"
            
            # 检查是否有该日期的记录
            has_records = False
            
            # 检查存款记录
            for deposit in chat_data['deposits']:
                record_date = deposit['time'].split(' ')[0]
                if record_date == date_str:
                    has_records = True
                    break
            
            # 检查提款记录，如果还没有找到记录
            if not has_records:
                for withdrawal in chat_data['withdrawals']:
                    record_date = withdrawal['time'].split(' ')[0]
                    if record_date == date_str:
                        has_records = True
                        break
            
            # 如果有该日期的记录，添加到列表
            if has_records:
                groups_with_records.append((chat_id, chat_title, chat_data))
        except Exception as e:
            logger.error(f"获取群组 {chat_id} 信息时出错: {e}")
    
    # 如果没有找到任何记录，显示提示消息
    if not groups_with_records:
        query.edit_message_text(f"在 {date_str} 没有找到任何群组的记账记录。", reply_markup=reply_markup)
        return
    
    # 总计统计数据
    total_deposit_amount = 0
    total_deposit_count = 0
    total_withdrawal_amount_local = 0
    total_withdrawal_amount_usdt = 0
    total_withdrawal_count = 0
    total_to_be_withdrawn = 0
    total_not_yet_withdrawn = 0
    
    # 所有用户的总计数据
    all_operators = {}  # 所有操作人统计
    all_operators_by_group = {}  # 按群组分类的操作人统计
    
    all_responders = {}  # 所有回复人统计
    all_responders_by_group = {}  # 按群组分类的回复人统计
    
    # 生成报表内容
    timestamp = datetime.datetime.now(timezone).strftime('%Y-%m-%d %H:%M:%S')
    summary_text = f"📊 {date_str} 所有群组财务统计 📊\n"
    summary_text += f"导出时间: {timestamp}\n\n"
    
    # 确保处理数据和显示都按照用户期望的顺序
    for chat_id, chat_title, chat_data in groups_with_records:
        # 筛选该日期的记录
        date_deposits = [d for d in chat_data['deposits'] if d['time'].split(' ')[0] == date_str]
        date_withdrawals = [w for w in chat_data['withdrawals'] if w['time'].split(' ')[0] == date_str]
        
        # 收集回复人统计数据
        deposit_by_group = 0  # 该群组的总入款
        for deposit in date_deposits:
            # 处理操作人
            operator = deposit.get('user', '未知操作人')
            amount = deposit['amount']
            deposit_by_group += amount
            
            # 按群组统计操作人
            if operator not in all_operators_by_group:
                all_operators_by_group[operator] = {}
            if chat_title not in all_operators_by_group[operator]:
                all_operators_by_group[operator][chat_title] = 0
            all_operators_by_group[operator][chat_title] += amount
            
            # 操作人总计
            if operator not in all_operators:
                all_operators[operator] = 0
            all_operators[operator] += amount
            
            # 处理回复人
            responder = deposit.get('responder', 'None')
            
            # 按群组统计回复人
            if responder not in all_responders_by_group:
                all_responders_by_group[responder] = {}
            if chat_title not in all_responders_by_group[responder]:
                all_responders_by_group[responder][chat_title] = 0
            all_responders_by_group[responder][chat_title] += amount
            
            # 回复人总计
            if responder not in all_responders:
                all_responders[responder] = 0
            all_responders[responder] += amount
    
    # 按照用户期望的顺序显示群组信息
    # 这里我们先收集所有群组的数据，然后再显示
    group_summaries = []
    
    for chat_id, chat_title, chat_data in groups_with_records:
        group_summary = f"[{chat_title}]\n"
        
        # 筛选该日期的记录
        date_deposits = [d for d in chat_data['deposits'] if d['time'].split(' ')[0] == date_str]
        date_withdrawals = [w for w in chat_data['withdrawals'] if w['time'].split(' ')[0] == date_str]
        
        # 汇率和费率部分
        rate = chat_data.get('fixed_rate', 1.0)
        fee_rate = chat_data.get('rate', 0.0)
        
        # 计算统计数据
        deposit_total = sum(deposit['amount'] for deposit in date_deposits)
        deposit_count = len(date_deposits)
        
        withdrawal_total_local = sum(withdraw['amount'] for withdraw in date_withdrawals)
        withdrawal_total_usdt = sum(withdraw['usd_equivalent'] for withdraw in date_withdrawals)
        withdrawal_count = len(date_withdrawals)
        
        # 计算实际金额
        actual_amount = deposit_total / rate if rate != 0 else 0
        
        # 计算应下发金额
        to_be_withdrawn = actual_amount
        already_withdrawn = withdrawal_total_usdt
        not_yet_withdrawn = to_be_withdrawn - already_withdrawn
        
        # 添加到总计
        total_deposit_amount += deposit_total
        total_deposit_count += deposit_count
        total_withdrawal_amount_local += withdrawal_total_local
        total_withdrawal_amount_usdt += withdrawal_total_usdt
        total_withdrawal_count += withdrawal_count
        total_to_be_withdrawn += to_be_withdrawn
        total_not_yet_withdrawn += not_yet_withdrawn
        
        # 简洁模式：只显示基本信息
        group_summary += f"费率：{fee_rate}%\n"
        group_summary += f"固定汇率：{rate}\n"
        group_summary += f"总入款：{deposit_total:.1f}\n"
        group_summary += f"应下发：{deposit_total:.1f}｜{to_be_withdrawn:.2f}U\n"
        group_summary += f"已下发：{withdrawal_total_local:.1f}｜{already_withdrawn:.2f}U\n"
        group_summary += f"未下发：{deposit_total-withdrawal_total_local:.1f}｜{not_yet_withdrawn:.2f}U\n\n"
        
        group_summaries.append(group_summary)
    
    # 将所有群组信息添加到摘要中
    for group_summary in group_summaries:
        summary_text += group_summary
    
    # 添加所有群组的总计统计
    summary_text += "\n📊 所有群组总计统计 📊\n\n"
    
    # 按操作人统计
    summary_text += "👨‍💼 操作人总统计\n"
    summary_text += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    if all_operators:
        for operator, total_amount in sorted(all_operators.items(), key=lambda x: x[1], reverse=True):
            # 先显示操作人的总金额
            summary_text += f"• {operator}: {total_amount:.2f}\n"
            
            # 显示该操作人在每个群组的入款金额
            if operator in all_operators_by_group:
                groups_with_amounts = []
                for group_name, amount in all_operators_by_group[operator].items():
                    groups_with_amounts.append(f"{group_name}: {amount:.2f}")
                
                group_amounts_str = ", ".join(groups_with_amounts)
                summary_text += f"  📋 群组: {group_amounts_str}\n"
    else:
        summary_text += "暂无操作记录\n"
    
    # 按回复人统计 - 这里按照用户的示例格式：每个回复人单独一行，不显示群组明细
    summary_text += "\n👤 回复人总统计\n"
    summary_text += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    if all_responders:
        for responder, total_amount in sorted(all_responders.items(), key=lambda x: x[1], reverse=True):
            # 只显示回复人和总金额，不包含群组详情
            summary_text += f"• {responder} {total_amount:.2f}\n"
    else:
        summary_text += "暂无回复记录\n"
    
    # 总计统计
    summary_text += "\n📈 总计统计\n"
    summary_text += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    summary_text += f"• 群组数量: {len(groups_with_records)}\n"
    summary_text += f"• 总入款: {total_deposit_count}笔，{total_deposit_amount:.2f}\n"
    summary_text += f"• 总出款: {total_withdrawal_count}笔，{total_withdrawal_amount_local:.2f}\n"
    summary_text += f"• 总应下发: {total_to_be_withdrawn:.2f}\n"
    summary_text += f"• 总未下发: {total_not_yet_withdrawn:.2f}\n"
    
    # 导出为TXT文件
    file_path = export_all_groups_statistics_to_txt(date_str, summary_text, groups_with_records)
    
    # 发送文件给用户
    if file_path:
        try:
            with open(file_path, 'rb') as file:
                context.bot.send_document(
                    chat_id=query.message.chat_id,
                    document=file,
                    filename=f"{date_str}_所有群组统计.txt",
                    caption=f"{date_str} 所有群组财务统计导出文件"
                )
            logger.info(f"已导出 {date_str} 所有群组统计数据到 {file_path}")
            
            # 更新消息，表示导出成功
            query.edit_message_text(f"已成功导出 {date_str} 所有群组的统计数据", reply_markup=reply_markup)
        except Exception as e:
            logger.error(f"发送文件时出错: {e}", exc_info=True)
            query.edit_message_text(f"导出统计数据时出错: {str(e)}", reply_markup=reply_markup)
    else:
        # 更新消息，表示导出失败
        query.edit_message_text(f"未能导出 {date_str} 所有群组的统计数据", reply_markup=reply_markup)

# 导出所有群组统计数据为TXT文件
def export_all_groups_statistics_to_txt(date_str, summary_text, groups_with_records):
    """导出所有群组在指定日期的统计数据为TXT文件"""
    try:
        # 创建导出目录，如果不存在
        export_dir = "exports"
        if not os.path.exists(export_dir):
            os.makedirs(export_dir)
        
        # 创建文件名
        timestamp = datetime.datetime.now(timezone).strftime("%Y%m%d_%H%M%S")
        file_path = os.path.join(export_dir, f"all_groups_{date_str}_{timestamp}.txt")
        
        # 使用与在线视图完全相同的格式（直接使用已经格式化好的summary_text）
        content = summary_text
        
        # 写入文件
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return file_path
    except Exception as e:
        logger.error(f"导出 {date_str} 所有群组统计数据时出错: {e}", exc_info=True)
        return None

# 添加导出当前账单的函数
def export_current_bill(query, context, chat_id):
    """导出当前聊天的账单为txt文件"""
    logger.info(f"导出聊天 {chat_id} 的账单")
    
    try:
        # 获取聊天信息
        chat = context.bot.get_chat(chat_id)
        chat_title = chat.title if chat.type in ['group', 'supergroup'] else "私聊"
        
        # 生成账单摘要
        summary_text = generate_group_summary(chat_title)
        
        # 只答复点击，不修改原消息
        query.answer("正在生成账单文件...")
        
        # 导出为TXT文件
        file_path = export_group_data_to_txt(chat_title, summary_text)
        
        # 发送文件给用户
        if file_path:
            with open(file_path, 'rb') as file:
                context.bot.send_document(
                    chat_id=query.message.chat_id,
                    document=file,
                    filename=f"{chat_title}_账单.txt",
                    caption=f"{chat_title} 财务账单导出文件"
                )
            logger.info(f"已导出 {chat_title} 的账单数据到 {file_path}")
        else:
            # 发送失败消息
            context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"未能导出 {chat_title} 的账单数据"
            )
    except Exception as e:
        logger.error(f"导出账单时出错: {e}", exc_info=True)
        context.bot.send_message(
            chat_id=query.message.chat_id,
            text=f"导出账单时出错: {str(e)}"
        )

def generate_group_summary(group_name):
    """生成指定群组的账单摘要"""
    logger.info(f"为群组 '{group_name}' 生成账单摘要")
    
    # 查找对应的聊天ID
    chat_id = None
    chat_data = None
    for cid, data in chat_accounting.items():
        try:
            chat = None
            try:
                from telegram import Bot
                bot = Bot(token=BOT_TOKEN)
                chat = bot.get_chat(cid)
                if chat.type in ['group', 'supergroup'] and chat.title == group_name:
                    chat_id = cid
                    chat_data = data
                    break
            except:
                # 如果无法获取聊天信息，则跳过
                continue
        except Exception as e:
            logger.error(f"检查聊天ID {cid} 时出错: {e}")
    
    if chat_id is None or chat_data is None:
        # 如果没有找到匹配的聊天ID，返回一个默认消息
        return f"群组 '{group_name}' 尚无记账数据。"
    
    # 从这里开始生成实际的账单摘要
    # 收款部分
    deposit_total = sum(deposit['amount'] for deposit in chat_data['deposits'])
    deposit_count = len(chat_data['deposits'])
    
    # 统计每个用户的入款
    user_deposits = {}
    for deposit in chat_data['deposits']:
        username = deposit['user']
        amount = deposit['amount']
        if username not in user_deposits:
            user_deposits[username] = 0
        user_deposits[username] += amount
    
    # 统计以回复用户为分类的入款信息
    responder_deposits = {}
    for deposit in chat_data['deposits']:
        # 只处理有回复者信息的记录
        if 'responder' in deposit and deposit['responder']:
            responder = deposit['responder']
            username = deposit['user']
            amount = deposit['amount']
            
            # 创建或更新此回复者的记录
            if responder not in responder_deposits:
                responder_deposits[responder] = {'total': 0, 'users': {}}
            
            responder_deposits[responder]['total'] += amount
            
            # 记录是哪个用户对这个回复者进行了入款
            if username not in responder_deposits[responder]['users']:
                responder_deposits[responder]['users'][username] = 0
            responder_deposits[responder]['users'][username] += amount
    
    # 计算用户分类数量
    user_count = len(chat_data['users'])
    responder_count = len(responder_deposits)
    
    # 汇率和费率部分
    rate = chat_data.get('fixed_rate', 1.0)
    fee_rate = chat_data.get('rate', 0.0)
    
    # 计算实际金额 - 使用除法计算
    actual_amount = deposit_total / rate if rate != 0 else 0
    
    # 出款部分 - 从withdrawals中提取USDT金额(usd_equivalent)
    withdrawal_total_local = sum(withdraw['amount'] for withdraw in chat_data['withdrawals'])
    withdrawal_total_usdt = sum(withdraw['usd_equivalent'] for withdraw in chat_data['withdrawals'])
    withdrawal_count = len(chat_data['withdrawals'])
    
    # 计算应下发金额（USDT）
    to_be_withdrawn = actual_amount
    already_withdrawn = withdrawal_total_usdt
    not_yet_withdrawn = to_be_withdrawn - already_withdrawn
    
    # 按照用户要求的模板格式生成账单摘要
    summary_text = f"====== {group_name} ======\n\n"
    
    summary_text += f"入款（{deposit_count}笔）：\n"
    if deposit_count > 0:
        # 按时间排序，最新的在前面
        sorted_deposits = sorted(chat_data['deposits'], key=lambda x: x.get('time', ''), reverse=True)
        # 获取最新的6笔入款记录
        latest_deposits = sorted_deposits[:6]
        
        # 显示每个入款记录及其回复人
        for deposit in latest_deposits:
            amount = deposit['amount']
            # 计算美元等值：金额除以汇率
            usd_equivalent = amount / rate if rate != 0 else 0
            responder = deposit.get('responder', '无回复人')
            
            # 提取时间戳中的小时和分钟
            time_str = deposit.get('time', '')
            time_parts = time_str.split(' ')
            if len(time_parts) > 1:
                time_part = time_parts[1]  # 获取时间部分 (HH:MM:SS)
                hour_min = ':'.join(time_part.split(':')[:2])  # 只保留小时和分钟
            else:
                hour_min = "00:00"  # 默认时间
                
            # 使用新的格式: HH:MM 金额/汇率 =美元等值 回复人
            responder_display = "" if responder is None or responder == "None" else responder
            summary_text += f"  {hour_min} {amount:.0f}/{rate} ={usd_equivalent:.2f} {responder_display}\n"
    else:
        summary_text += "  暂无入金\n"
    
    summary_text += f"\n分类（{responder_count}人）：\n"
    if responder_count > 0:
        for responder, data in responder_deposits.items():
            total_amount = data['total']
            # 对于每个回复者，只显示总金额，不显示来源
            summary_text += f"  {responder} {total_amount:.2f}\n"
    else:
        summary_text += "  暂无分类\n"
    
    summary_text += f"\n下发（{withdrawal_count}笔）：\n"
    if withdrawal_count > 0:
        user_withdrawals = {}
        for withdrawal in chat_data['withdrawals']:
            username = withdrawal['user']
            # 使用USDT金额而不是本地货币
            amount = withdrawal['usd_equivalent']
            if username not in user_withdrawals:
                user_withdrawals[username] = 0
            user_withdrawals[username] += amount
        
        for username, amount in user_withdrawals.items():
            summary_text += f"  {username}: {amount:.2f}\n"
    else:
        summary_text += "  暂无下发\n"
    
    summary_text += f"\n费率：{fee_rate}%\n"
    summary_text += f"固定汇率：{rate}\n"
    summary_text += f"总入款：{deposit_total:.2f}\n"
    summary_text += f"应下发：{deposit_total:.2f}｜{to_be_withdrawn:.2f}U\n"
    summary_text += f"已下发：{withdrawal_total_local:.2f}｜{already_withdrawn:.2f}U\n"
    summary_text += f"未下发：{deposit_total-withdrawal_total_local:.2f}｜{not_yet_withdrawn:.2f}U\n"
    
    # 添加提示信息，告知用户导出的账单中将包含明细
    summary_text += f"\n点击 [详细账单] 按钮导出完整账单，包含所有交易明细。\n"
    
    return summary_text

def export_group_data_to_txt(group_name, summary_text):
    """导出群组账单数据为TXT文件"""
    try:
        # 创建导出目录，如果不存在
        export_dir = "exports"
        if not os.path.exists(export_dir):
            os.makedirs(export_dir)
        
        # 创建安全的文件名
        safe_name = "".join([c if c.isalnum() else "_" for c in group_name])
        timestamp = datetime.datetime.now(timezone).strftime("%Y%m%d_%H%M%S")
        file_path = os.path.join(export_dir, f"{safe_name}_{timestamp}.txt")
        
        # 准备文件内容
        content = f"===== {group_name} 财务账单 =====\n"
        content += f"导出时间: {datetime.datetime.now(timezone).strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        # 添加摘要数据
        content += summary_text
        
        # 查找对应的聊天ID
        chat_id = None
        chat_data = None
        for cid, data in chat_accounting.items():
            try:
                chat = None
                try:
                    from telegram import Bot
                    bot = Bot(token=BOT_TOKEN)
                    chat = bot.get_chat(cid)
                    if chat.type in ['group', 'supergroup'] and chat.title == group_name:
                        chat_id = cid
                        chat_data = data
                        break
                except:
                    # 如果无法获取聊天信息，则跳过
                    continue
            except Exception as e:
                logger.error(f"检查聊天ID {cid} 时出错: {e}")
        
        if chat_id is not None and chat_data is not None:
            # 添加详细的入款记录
            content += "\n\n===== 入款明细 =====\n"
            if len(chat_data['deposits']) > 0:
                for i, deposit in enumerate(chat_data['deposits'], 1):
                    content += f"{i}. 时间: {deposit['time']}, "
                    content += f"金额: {deposit['amount']:.2f}, "
                    content += f"用户: {deposit['user']}, "
                    # 添加回复人信息
                    if 'responder' in deposit and deposit['responder']:
                        content += f"回复人: {deposit['responder']}, "
                    content += f"USD等值: {deposit['usd_equivalent']:.2f}\n"
            else:
                content += "暂无入款记录\n"
            
            # 添加详细的出款记录
            content += "\n===== 出款明细 =====\n"
            if len(chat_data['withdrawals']) > 0:
                for i, withdrawal in enumerate(chat_data['withdrawals'], 1):
                    content += f"{i}. 时间: {withdrawal['time']}, "
                    content += f"金额: {withdrawal['amount']:.2f}, "
                    content += f"用户: {withdrawal['user']}, "
                    content += f"USD等值: {withdrawal['usd_equivalent']:.2f}\n"
            else:
                content += "暂无出款记录\n"
        
        # 写入文件
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return file_path
    except Exception as e:
        logger.error(f"导出群组数据时出错: {e}", exc_info=True)
        return None

def summary(update: Update, context: CallbackContext) -> None:
    """Show accounting summary."""
    if not is_authorized(update):
        return
        
    chat_id = update.effective_chat.id
    chat_type = update.effective_chat.type
    chat_title = getattr(update.effective_chat, 'title', 'Private Chat')
    
    logger.info(f"生成聊天 {chat_id} ({chat_title}) 账单摘要")
    
    # 获取该聊天的账单数据
    chat_data = get_chat_accounting(chat_id)
    
    # 收款部分
    deposit_total = sum(deposit['amount'] for deposit in chat_data['deposits'])
    deposit_count = len(chat_data['deposits'])
    
    # 统计每个用户的入款
    user_deposits = {}
    for deposit in chat_data['deposits']:
        username = deposit['user']
        amount = deposit['amount']
        if username not in user_deposits:
            user_deposits[username] = 0
        user_deposits[username] += amount
    
    # 统计以回复用户为分类的入款信息
    responder_deposits = {}
    for deposit in chat_data['deposits']:
        # 只处理有回复者信息的记录
        if 'responder' in deposit and deposit['responder']:
            responder = deposit['responder']
            username = deposit['user']
            amount = deposit['amount']
            
            # 创建或更新此回复者的记录
            if responder not in responder_deposits:
                responder_deposits[responder] = {'total': 0, 'users': {}}
            
            responder_deposits[responder]['total'] += amount
            
            # 记录是哪个用户对这个回复者进行了入款
            if username not in responder_deposits[responder]['users']:
                responder_deposits[responder]['users'][username] = 0
            responder_deposits[responder]['users'][username] += amount
    
    # 计算用户分类数量
    user_count = len(chat_data['users'])
    responder_count = len(responder_deposits)
    
    # 汇率和费率部分
    rate = chat_data.get('fixed_rate', 1.0)
    fee_rate = chat_data.get('rate', 0.0)
    
    # 计算实际金额 - 使用除法计算
    actual_amount = deposit_total / rate if rate != 0 else 0
    
    # 出款部分 - 从withdrawals中提取USDT金额(usd_equivalent)
    withdrawal_total_local = sum(withdraw['amount'] for withdraw in chat_data['withdrawals'])
    withdrawal_total_usdt = sum(withdraw['usd_equivalent'] for withdraw in chat_data['withdrawals'])
    withdrawal_count = len(chat_data['withdrawals'])
    
    # 计算应下发金额（USDT）
    to_be_withdrawn = actual_amount
    already_withdrawn = withdrawal_total_usdt
    not_yet_withdrawn = to_be_withdrawn - already_withdrawn
    
    # 按照用户要求的模板格式生成账单摘要
    summary_text = f"====== {chat_title} ======\n\n"
    
    summary_text += f"入款（{deposit_count}笔）：\n"
    if deposit_count > 0:
        # 按时间排序，最新的在前面
        sorted_deposits = sorted(chat_data['deposits'], key=lambda x: x.get('time', ''), reverse=True)
        # 获取最新的6笔入款记录
        latest_deposits = sorted_deposits[:6]
        
        # 显示每个入款记录及其回复人
        for deposit in latest_deposits:
            amount = deposit['amount']
            # 计算美元等值：金额除以汇率
            usd_equivalent = amount / rate if rate != 0 else 0
            responder = deposit.get('responder', '无回复人')
            
            # 提取时间戳中的小时和分钟
            time_str = deposit.get('time', '')
            time_parts = time_str.split(' ')
            if len(time_parts) > 1:
                time_part = time_parts[1]  # 获取时间部分 (HH:MM:SS)
                hour_min = ':'.join(time_part.split(':')[:2])  # 只保留小时和分钟
            else:
                hour_min = "00:00"  # 默认时间
                
            # 使用新的格式: HH:MM 金额/汇率 =美元等值 回复人
            responder_display = "" if responder is None or responder == "None" else responder
            summary_text += f"  {hour_min} {amount:.0f}/{rate} ={usd_equivalent:.2f} {responder_display}\n"
    else:
        summary_text += "  暂无入金\n"
    
    summary_text += f"\n分类（{responder_count}人）：\n"
    if responder_count > 0:
        for responder, data in responder_deposits.items():
            total_amount = data['total']
            # 简化显示格式，只显示回复者和金额
            summary_text += f"  {responder} {total_amount:.2f}\n"
    else:
        summary_text += "  暂无分类\n"
    
    summary_text += f"\n下发（{withdrawal_count}笔）：\n"
    if withdrawal_count > 0:
        user_withdrawals = {}
        for withdrawal in chat_data['withdrawals']:
            username = withdrawal['user']
            # 使用USDT金额而不是本地货币
            amount = withdrawal['usd_equivalent']
            if username not in user_withdrawals:
                user_withdrawals[username] = 0
            user_withdrawals[username] += amount
        
        for username, amount in user_withdrawals.items():
            summary_text += f"  {username}: {amount:.2f}\n"
    else:
        summary_text += "  暂无下发\n"
    
    summary_text += f"\n费率：{fee_rate}%\n"
    summary_text += f"固定汇率：{rate}\n"
    summary_text += f"总入款：{deposit_total:.2f}\n"
    summary_text += f"应下发：{deposit_total:.2f}｜{to_be_withdrawn:.2f}U\n"
    summary_text += f"已下发：{withdrawal_total_local:.2f}｜{already_withdrawn:.2f}U\n"
    summary_text += f"未下发：{deposit_total-withdrawal_total_local:.2f}｜{not_yet_withdrawn:.2f}U\n"
    
    try:
        # 创建账单按钮
        keyboard = [[InlineKeyboardButton("详细账单", callback_data=f"export_bill_{chat_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # 使用reply_text确保消息总是发送，不管是否在群组中
        update.message.reply_text(summary_text, reply_markup=reply_markup)
        logger.info(f"已显示账单摘要，字符长度: {len(summary_text)}")
    except Exception as e:
        logger.error(f"发送账单摘要时出错: {e}", exc_info=True)
        try:
            # 尝试使用bot.send_message作为备选方案
            keyboard = [[InlineKeyboardButton("详细账单", callback_data=f"export_bill_{chat_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            context.bot.send_message(chat_id=chat_id, text=summary_text, reply_markup=reply_markup)
            logger.info(f"使用备选方法发送账单摘要成功")
        except Exception as e2:
            logger.error(f"备选方法发送账单摘要也失败: {e2}", exc_info=True)

def reset_command(update: Update, context: CallbackContext) -> None:
    """手动重置账单"""
    if not is_authorized(update):
        return
    
    # 获取聊天ID
    chat_id = update.effective_chat.id
    
    # 重置该聊天的账单
    reset_chat_accounting(chat_id)
    
    update.message.reply_text(f'已重置当前群组的账单')

def set_admin(update: Update, context: CallbackContext) -> None:
    """Set the admin user who can manage operators."""
    global admin_user_id
    # Only allow setting admin if no admin is set yet
    if admin_user_id is None:
        admin_user_id = update.effective_user.id
        update.message.reply_text(f'您已被设置为管理员，用户ID: {admin_user_id}')
    else:
        update.message.reply_text('管理员已经设置，无法更改')

def button_callback(update: Update, context: CallbackContext) -> None:
    """Handle button callbacks from inline keyboards."""
    query = update.callback_query
    data = query.data
    
    logger.info(f"收到按钮回调: {data}")
    
    # 确保回调处理后通知Telegram
    query.answer()
    
    # 处理导出当前账单按钮
    if data.startswith("export_bill_"):
        chat_id = int(data.split("_")[2])
        export_current_bill(query, context, chat_id)
        return
    
    # 处理特定日期导出按钮 ("导出全部账单"命令的日期选择)
    if data.startswith("export_date_"):
        parts = data.split("_")
        # 处理返回到日期选择的情况
        if parts[2] == "back":
            chat_id = int(parts[3])
            # 获取最近7天的日期列表，生成选择界面
            dates = []
            for i in range(7):
                date = (datetime.datetime.now(timezone) - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
                dates.append(date)
            
            # 创建日期选择按钮
            keyboard = []
            row = []
            for i, date in enumerate(dates):
                row.append(InlineKeyboardButton(date, callback_data=f"export_date_{date}_{chat_id}"))
                if (i + 1) % 2 == 0 or i == len(dates) - 1:  # 每两个日期一行，或者是最后一个日期
                    keyboard.append(row)
                    row = []
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # 更新消息，显示日期选择
            query.edit_message_text("请选择要导出的日期:", reply_markup=reply_markup)
            return
        else:
            # 正常处理导出指定日期数据
            date_str = parts[2]
            chat_id = int(parts[3])
            export_specific_date_for_chat(query, context, date_str, chat_id)
            return
    
    # 处理菜单的第一页
    if data == "first_page":
        keyboard = [
            [InlineKeyboardButton("查看所有群组当日统计", callback_data="all_groups_today")],
            [InlineKeyboardButton("按日期查看所有群组", callback_data="all_groups_by_date")],
            [InlineKeyboardButton("查看当前群组7天账单", callback_data="current_group_7days")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text("请选择要查看的账单类型:", reply_markup=reply_markup)
        return
    
    # 处理"查看所有群组当日统计"按钮
    if data == "all_groups_today":
        # 获取当前日期
        current_date = get_current_date()
        export_all_groups_statistics(query, context, current_date)
        return
    
    # 处理"按日期查看所有群组"按钮
    if data == "all_groups_by_date":
        send_date_selection(query, context)
        return
    
    # 处理"查看当前群组7天账单"按钮
    if data == "current_group_7days":
        # 获取当前聊天ID
        chat_id = query.message.chat_id
        export_current_group_all_bills(query, context)
        return
    
    # 处理日期选择按钮
    if data.startswith("date_"):
        date_str = data.split("_")[1]
        # 获取该日期的所有群组信息
        send_group_selection_for_date(query, context, date_str)
        return
    
    # 处理日期选择按钮 (来自财务查账命令)
    if data.startswith("select_date_"):
        date_str = data.split("_")[2]
        handle_date_selection(query, context, date_str)
        return
    
    # 处理群组选择按钮
    if data.startswith("group_"):
        parts = data.split("_")
        group_id = int(parts[1])
        date_str = parts[2] if len(parts) > 2 else None
        
        if date_str:
            # 如果提供了日期，导出该日期的群组账单
            export_group_by_selected_date(query, context, group_id)
        else:
            # 否则，导出当前群组账单
            export_current_bill(query, context, group_id)
        return
    
    # 处理返回按钮 (回到群组选择)
    if data.startswith("back_to_groups_for_date_"):
        date_str = data.split("_")[-1]
        send_group_selection_for_date(query, context, date_str)
        return
    
    # 处理返回按钮 (回到日期选择)
    if data == "back_to_dates":
        send_date_selection(query, context)
        return
    
    # 处理返回按钮 (回到日期选择，用于财务查账命令)
    if data == "back_to_dates_first":
        send_date_selection_first(query, context)
        return
        
    # 处理一键复制地址按钮
    if data.startswith("copy_address_"):
        # 提取USDT地址
        usdt_address = data[len("copy_address_"):]
        logger.info(f"用户请求复制地址: {usdt_address}")
        
        # 发送单独的消息，包含完整地址，方便用户复制
        context.bot.send_message(
            chat_id=query.message.chat_id,
            text=f"<code>{usdt_address}</code>\n\n👆 点击上方地址可复制",
            parse_mode=ParseMode.HTML
        )
        
        # 通知用户操作已完成
        query.answer("地址已发送，可直接复制")
        return
    
    # 如果回调数据不匹配任何已知模式
    logger.warning(f"未知的按钮回调数据: {data}")
    query.edit_message_text("抱歉，无法处理此请求。")

def export_specific_date_for_chat(query, context, date_str, chat_id):
    """导出特定日期的群组账单"""
    logger.info(f"导出群组 {chat_id} 在 {date_str} 的账单")
    
    try:
        # 获取群组信息
        chat = context.bot.get_chat(chat_id)
        chat_title = chat.title if chat.type in ['group', 'supergroup'] else "私聊"
        
        # 创建回调查询对象
        keyboard = [[InlineKeyboardButton("返回", callback_data=f"export_date_back_{chat_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # 更新消息，表示正在导出
        query.edit_message_text(f"正在导出 {chat_title} {date_str} 的账单数据...", reply_markup=reply_markup)
        
        # 检查账单是否为空
        if chat_id not in chat_accounting:
            # 创建空账单
            get_chat_accounting(chat_id)
        
        # 获取该聊天的账单数据
        chat_data = get_chat_accounting(chat_id)
        
        # 筛选指定日期的记录
        date_deposits = [d for d in chat_data['deposits'] if d['time'].split(' ')[0] == date_str]
        date_withdrawals = [w for w in chat_data['withdrawals'] if w['time'].split(' ')[0] == date_str]
        
        # 计算统计数据
        deposit_total = sum(deposit['amount'] for deposit in date_deposits)
        deposit_count = len(date_deposits)
        
        withdrawal_total_local = sum(withdraw['amount'] for withdraw in date_withdrawals)
        withdrawal_total_usdt = sum(withdraw['usd_equivalent'] for withdraw in date_withdrawals)
        withdrawal_count = len(date_withdrawals)
        
        # 统计每个用户的入款
        user_deposits = {}
        for deposit in date_deposits:
            username = deposit['user']
            amount = deposit['amount']
            if username not in user_deposits:
                user_deposits[username] = 0
            user_deposits[username] += amount
        
        # 统计每个用户的出款
        user_withdrawals = {}
        for withdrawal in date_withdrawals:
            username = withdrawal['user']
            amount = withdrawal['amount']
            if username not in user_withdrawals:
                user_withdrawals[username] = 0
            user_withdrawals[username] += amount
        
        # 汇率和费率部分
        rate = chat_data.get('fixed_rate', 1.0)
        fee_rate = chat_data.get('rate', 0.0)
        
        # 计算实际金额
        actual_amount = deposit_total / rate if rate != 0 else 0
        
        # 计算应下发金额
        to_be_withdrawn = actual_amount
        already_withdrawn = withdrawal_total_usdt
        not_yet_withdrawn = to_be_withdrawn - already_withdrawn
        
        # 生成账单摘要
        summary_text = f"====== {chat_title} {date_str} 账单 ======\n\n"
        
        summary_text += f"入款（{deposit_count}笔）：\n"
        if deposit_count > 0:
            for username, amount in user_deposits.items():
                summary_text += f"  {username}: {amount:.2f}\n"
        else:
            summary_text += "  暂无入金\n"
        
        summary_text += f"\n下发（{withdrawal_count}笔）：\n"
        if withdrawal_count > 0:
            for username, amount in user_withdrawals.items():
                summary_text += f"  {username}: {amount:.2f}\n"
        else:
            summary_text += "  暂无下发\n"
        
        summary_text += f"\n费率：{fee_rate}%\n"
        summary_text += f"固定汇率：{rate}\n"
        summary_text += f"总入款：{deposit_total:.2f}\n"
        summary_text += f"应下发：{deposit_total:.2f}｜{to_be_withdrawn:.2f}U\n"
        summary_text += f"已下发：{withdrawal_total_local:.2f}｜{already_withdrawn:.2f}U\n"
        summary_text += f"未下发：{deposit_total-withdrawal_total_local:.2f}｜{not_yet_withdrawn:.2f}U\n"
        
        # 导出为TXT文件
        file_path = export_group_date_data_to_txt(chat_title, date_str, summary_text, date_deposits, date_withdrawals)
        
        # 发送文件给用户
        if file_path:
            with open(file_path, 'rb') as file:
                context.bot.send_document(
                    chat_id=query.message.chat_id,
                    document=file,
                    filename=f"{chat_title}_{date_str}_账单.txt",
                    caption=f"{chat_title} {date_str} 财务账单导出文件"
                )
            logger.info(f"已导出 {chat_title} {date_str} 的账单数据到 {file_path}")
            
            # 更新消息，表示导出成功
            query.edit_message_text(f"已成功导出 {chat_title} {date_str} 的账单数据", reply_markup=reply_markup)
        else:
            # 更新消息，表示导出失败
            query.edit_message_text(f"未能导出 {chat_title} {date_str} 的账单数据", reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"导出群组日期账单时出错: {e}", exc_info=True)
        keyboard = [[InlineKeyboardButton("返回", callback_data=f"export_date_back_{chat_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(f"导出账单时出错: {str(e)}", reply_markup=reply_markup)

def send_group_selection_for_date(query, context, date_str):
    """发送指定日期的群组选择界面"""
    logger.info(f"为日期 {date_str} 发送群组选择界面")
    
    # 查找在该日期有记录的群组
    groups_with_records = []
    for chat_id, chat_data in chat_accounting.items():
        try:
            # 检查是否是群组
            chat = context.bot.get_chat(chat_id)
            if chat.type not in ['group', 'supergroup']:
                continue
            
            # 检查是否有该日期的记录
            has_records = False
            
            # 检查存款记录
            for deposit in chat_data['deposits']:
                record_date = deposit['time'].split(' ')[0]
                if record_date == date_str:
                    has_records = True
                    break
            
            # 检查提款记录，如果还没有找到记录
            if not has_records:
                for withdrawal in chat_data['withdrawals']:
                    record_date = withdrawal['time'].split(' ')[0]
                    if record_date == date_str:
                        has_records = True
                        break
            
            # 如果有该日期的记录，添加到列表
            if has_records:
                groups_with_records.append((chat_id, chat.title))
        except Exception as e:
            logger.error(f"获取群组 {chat_id} 信息时出错: {e}")
    
    # 如果没有找到任何有记录的群组
    if not groups_with_records:
        keyboard = [[InlineKeyboardButton("返回", callback_data="back_to_dates")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(f"在 {date_str} 没有找到任何群组的记账记录。", reply_markup=reply_markup)
        return
    
    # 创建群组选择按钮
    keyboard = []
    for chat_id, chat_title in groups_with_records:
        keyboard.append([InlineKeyboardButton(chat_title, callback_data=f"group_{chat_id}_{date_str}")])
    
    # 添加返回按钮
    keyboard.append([InlineKeyboardButton("返回", callback_data="back_to_dates")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # 更新消息
    query.edit_message_text(f"请选择要查看 {date_str} 账单的群组:", reply_markup=reply_markup)

def export_current_group_all_bills(query, context):
    """导出当前群组7天内的所有账单"""
    logger.info("导出当前群组7天账单")
    
    # 获取当前聊天ID
    chat_id = query.message.chat_id
    
    try:
        # 获取群组信息
        chat = context.bot.get_chat(chat_id)
        chat_title = chat.title if chat.type in ['group', 'supergroup'] else "私聊"
        
        # 获取最近7天的日期列表
        dates = []
        for i in range(7):
            date = (datetime.datetime.now(timezone) - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
            dates.append(date)
        
        # 创建回调查询对象
        keyboard = [[InlineKeyboardButton("返回", callback_data="first_page")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # 更新消息，表示正在导出
        query.edit_message_text(f"正在导出 {chat_title} 最近7天的账单数据...", reply_markup=reply_markup)
        
        # 检查账单是否为空
        if chat_id not in chat_accounting:
            # 创建空账单
            get_chat_accounting(chat_id)
        
        # 生成摘要
        summary_text = generate_chat_all_days_summary(chat_id, chat_title, dates)
        
        # 导出为TXT文件
        file_path = export_chat_all_days_to_txt(chat_id, chat_title, summary_text, dates)
        
        # 发送文件给用户
        if file_path:
            with open(file_path, 'rb') as file:
                context.bot.send_document(
                    chat_id=query.message.chat_id,
                    document=file,
                    filename=f"{chat_title}_7天账单.txt",
                    caption=f"{chat_title} 最近7天财务账单导出文件"
                )
            logger.info(f"已导出 {chat_title} 最近7天的账单数据到 {file_path}")
            
            # 更新消息，表示导出成功
            query.edit_message_text(f"已成功导出 {chat_title} 最近7天的账单数据", reply_markup=reply_markup)
        else:
            # 更新消息，表示导出失败
            query.edit_message_text(f"未能导出 {chat_title} 最近7天的账单数据", reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"导出群组7天账单时出错: {e}", exc_info=True)
        keyboard = [[InlineKeyboardButton("返回", callback_data="first_page")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(f"导出账单时出错: {str(e)}", reply_markup=reply_markup)

def show_all_bills_menu(update: Update, context: CallbackContext) -> None:
    """显示所有账单菜单"""
    if not is_authorized(update):
        return
    
    logger.info("显示所有账单菜单")
    
    # 创建菜单按钮
    keyboard = [
        [InlineKeyboardButton("查看所有群组当日统计", callback_data="all_groups_today")],
        [InlineKeyboardButton("按日期查看所有群组", callback_data="all_groups_by_date")],
        [InlineKeyboardButton("查看当前群组7天账单", callback_data="current_group_7days")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # 发送菜单消息
    update.message.reply_text("请选择要查看的账单类型:", reply_markup=reply_markup)

def send_date_selection(query, context):
    """发送日期选择界面"""
    logger.info("发送日期选择界面")
    
    # 获取最近7天的日期列表
    dates = []
    for i in range(7):
        date = (datetime.datetime.now(timezone) - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
        dates.append(date)
    
    # 找出有记录的日期
    dates_with_records = []
    for date_str in dates:
        # 检查所有群组在该日期是否有记录
        has_records = False
        for chat_id, chat_data in chat_accounting.items():
            try:
                # 检查存款记录
                for deposit in chat_data['deposits']:
                    record_date = deposit['time'].split(' ')[0]
                    if record_date == date_str:
                        has_records = True
                        break
                
                if has_records:
                    break
                
                # 检查提款记录
                for withdrawal in chat_data['withdrawals']:
                    record_date = withdrawal['time'].split(' ')[0]
                    if record_date == date_str:
                        has_records = True
                        break
                
                if has_records:
                    break
            except Exception as e:
                logger.error(f"检查聊天 {chat_id} 的记录时出错: {e}")
        
        if has_records:
            dates_with_records.append(date_str)
    
    # 如果没有找到任何有记录的日期
    if not dates_with_records:
        query.edit_message_text("没有找到任何日期的记账记录")
        return
    
    # 创建日期选择按钮
    keyboard = []
    row = []
    for i, date in enumerate(dates_with_records):
        row.append(InlineKeyboardButton(date, callback_data=f"date_{date}"))
        if (i + 1) % 2 == 0 or i == len(dates_with_records) - 1:  # 每两个日期一行，或者是最后一个日期
            keyboard.append(row)
            row = []
    
    # 添加返回按钮
    keyboard.append([InlineKeyboardButton("返回", callback_data="first_page")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # 更新消息
    query.edit_message_text("请选择要查看的日期:", reply_markup=reply_markup)

def show_financial_summary(update: Update, context: CallbackContext) -> None:
    """显示财务账单摘要"""
    if not is_authorized(update):
        return
    
    chat_id = update.effective_chat.id
    chat_type = update.effective_chat.type
    chat_title = getattr(update.effective_chat, 'title', 'Private Chat')
    
    logger.info(f"生成聊天 {chat_id} ({chat_title}) 财务账单摘要")
    
    # 获取该聊天的账单数据
    chat_data = get_chat_accounting(chat_id)
    
    # 收款部分 - 计算今日和总计
    today = datetime.datetime.now(timezone).strftime('%Y-%m-%d')
    
    # 筛选今日记录
    today_deposits = [d for d in chat_data['deposits'] if d['time'].split(' ')[0] == today]
    today_withdrawals = [w for w in chat_data['withdrawals'] if w['time'].split(' ')[0] == today]
    
    # 今日统计
    today_deposit_total = sum(deposit['amount'] for deposit in today_deposits)
    today_deposit_count = len(today_deposits)
    
    today_withdrawal_total = sum(withdraw['amount'] for withdraw in today_withdrawals)
    today_withdrawal_count = len(today_withdrawals)
    
    # 总计统计
    total_deposit_total = sum(deposit['amount'] for deposit in chat_data['deposits'])
    total_deposit_count = len(chat_data['deposits'])
    
    total_withdrawal_total = sum(withdraw['amount'] for withdraw in chat_data['withdrawals'])
    total_withdrawal_count = len(chat_data['withdrawals'])
    
    # 汇率和费率部分
    rate = chat_data.get('fixed_rate', 1.0)
    fee_rate = chat_data.get('rate', 0.0)
    
    # 计算实际金额 - 使用除法计算
    total_actual_amount = total_deposit_total / rate if rate != 0 else 0
    
    # 计算应下发金额
    total_to_be_withdrawn = total_actual_amount
    total_already_withdrawn = total_withdrawal_total
    total_not_yet_withdrawn = total_to_be_withdrawn - total_already_withdrawn
    
    # 按照用户要求的模板格式生成财务账单摘要
    summary_text = f"====== {chat_title} 财务账单 ======\n\n"
    
    summary_text += f"===== 今日 ({today}) =====\n"
    summary_text += f"入款: {today_deposit_count}笔，共计 {today_deposit_total:.2f}\n"
    
    # 统计每个用户今日的入款
    if today_deposit_count > 0:
        today_user_deposits = {}
        for deposit in today_deposits:
            username = deposit['user']
            amount = deposit['amount']
            if username not in today_user_deposits:
                today_user_deposits[username] = 0
            today_user_deposits[username] += amount
        
        for username, amount in today_user_deposits.items():
            summary_text += f"  {username}: {amount:.2f}\n"
    
    summary_text += f"出款: {today_withdrawal_count}笔，共计 {today_withdrawal_total:.2f}\n"
    
    # 统计每个用户今日的出款
    if today_withdrawal_count > 0:
        today_user_withdrawals = {}
        for withdrawal in today_withdrawals:
            username = withdrawal['user']
            amount = withdrawal['amount']
            if username not in today_user_withdrawals:
                today_user_withdrawals[username] = 0
            today_user_withdrawals[username] += amount
        
        for username, amount in today_user_withdrawals.items():
            summary_text += f"  {username}: {amount:.2f}\n"
    
    summary_text += f"\n===== 总计 =====\n"
    summary_text += f"总入款: {total_deposit_count}笔，共计 {total_deposit_total:.2f}\n"
    summary_text += f"总出款: {total_withdrawal_count}笔，共计 {total_withdrawal_total:.2f}\n"
    summary_text += f"费率: {fee_rate}%\n"
    summary_text += f"固定汇率: {rate}\n"
    summary_text += f"应下发: {total_to_be_withdrawn:.2f}\n"
    summary_text += f"已下发: {total_already_withdrawn:.2f}\n"
    summary_text += f"未下发: {total_not_yet_withdrawn:.2f}\n"
    
    # 创建按钮，提供导出功能
    keyboard = [
        [InlineKeyboardButton("导出详细财务账单", callback_data=f"export_bill_{chat_id}")],
        [InlineKeyboardButton("查看所有群组账单", callback_data="all_groups_today")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # 发送消息
    update.message.reply_text(summary_text, reply_markup=reply_markup)
    logger.info(f"已显示财务账单摘要，字符长度: {len(summary_text)}")

def deposit(update: Update, context: CallbackContext) -> None:
    """Record a deposit."""
    if not is_authorized(update):
        return
    
    if not context.args or len(context.args) < 1:
        update.message.reply_text('使用方法: /deposit <金额>')
        return
    
    try:
        amount = float(context.args[0])
        logger.info(f"处理 /deposit 命令: {amount}")
        
        # 添加入款记录
        add_deposit_record(update, amount)
        
        # 发送确认消息
        update.message.reply_text(f"✅ 已入款: +{amount}")
        
        # 显示更新后的账单
        summary(update, context)
    except ValueError:
        update.message.reply_text('金额必须是数字')

def withdraw(update: Update, context: CallbackContext) -> None:
    """Record a withdrawal."""
    if not is_authorized(update):
        return
    
    if not context.args or len(context.args) < 1:
        update.message.reply_text('使用方法: /withdraw <USDT金额>')
        return
    
    try:
        amount = float(context.args[0])
        logger.info(f"处理 /withdraw 命令: {amount} USDT")
        
        # 添加出款记录
        add_withdrawal_record(update, amount)
        
        # 获取该聊天的账单数据用于显示汇率
        chat_id = update.effective_chat.id
        chat_data = get_chat_accounting(chat_id)
        rate = chat_data.get('fixed_rate', 1.0)
        local_amount = amount * rate
        
        # 发送确认消息 - 简化的确认
        update.message.reply_text(f"✅ 已出款")
        
        # 显示更新后的账单
        summary(update, context)
    except ValueError:
        update.message.reply_text('金额必须是数字')

def user(update: Update, context: CallbackContext) -> None:
    """Record user classification."""
    if not is_authorized(update):
        return
        
    if len(context.args) < 3:
        update.message.reply_text('使用方法: /user [用户ID] [上分金额] [下分金额]')
        return
    
    # 获取聊天ID
    chat_id = update.effective_chat.id
    
    # 获取该聊天的账单数据
    chat_data = get_chat_accounting(chat_id)
    
    try:
        user_id = context.args[0]
        up_amount = float(context.args[1])
        down_amount = float(context.args[2])
        
        # Calculate balance
        balance = up_amount - down_amount
        
        # Record or update the user
        chat_data['users'][user_id] = {
            'up': up_amount,
            'down': down_amount,
            'balance': balance
        }
        
        update.message.reply_text(f'已记录用户分类: {user_id} - 上分:{up_amount} 下分:{down_amount} 余额:{balance:.2f}U')
        
        # Show summary after recording
        summary(update, context)
    except ValueError:
        update.message.reply_text('金额必须是数字')

def main() -> None:
    """Start the bot."""
    # 清除历史数据，确保每次启动时都使用新数据
    # 不需要重置特定聊天ID的数据，让系统在收到消息时自动创建
    global group_operators, authorized_groups
    
    logger.info("启动机器人...")
    
    # 加载保存的数据
    load_data()
    
    # 注册信号处理
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    
    # Create the Updater and pass it your bot's token
    updater = Updater(BOT_TOKEN)
    
    # 输出机器人信息
    bot_info = updater.bot.get_me()
    logger.info(f"机器人信息: ID={bot_info.id}, 用户名=@{bot_info.username}, 名称={bot_info.first_name}")
    logger.info(f"机器人配置: can_join_groups={bot_info.can_join_groups}, can_read_all_group_messages={bot_info.can_read_all_group_messages}")

    # Get the dispatcher to register handlers
    dispatcher = updater.dispatcher
    
    # 初始化所有已授权群组的操作人列表
    for chat_id in authorized_groups:
        if chat_id not in group_operators:
            group_operators[chat_id] = set(INITIAL_OPERATORS)
            logger.info(f"为群组 {chat_id} 初始化操作人列表: {group_operators[chat_id]}")
    
    # 为所有已有账单的群组初始化操作人列表
    for chat_id in chat_accounting.keys():
        if chat_id not in group_operators and chat_id in authorized_groups:
            group_operators[chat_id] = set(INITIAL_OPERATORS)
            logger.info(f"为已有账单的群组 {chat_id} 初始化操作人列表: {group_operators[chat_id]}")

    # Register command handlers
    dispatcher.add_handler(CommandHandler("start", start))
    dispatcher.add_handler(CommandHandler("help", help_command))
    dispatcher.add_handler(CommandHandler("deposit", deposit))
    dispatcher.add_handler(CommandHandler("withdraw", withdraw))
    dispatcher.add_handler(CommandHandler("user", user))
    dispatcher.add_handler(CommandHandler("rate", set_rate))
    dispatcher.add_handler(CommandHandler("fixed_rate", set_fixed_rate))
    dispatcher.add_handler(CommandHandler("summary", summary))
    dispatcher.add_handler(CommandHandler("reset", reset_command))
    
    # 添加计算器命令处理器
    dispatcher.add_handler(CommandHandler("calc", lambda update, context: update.message.reply_text(
        handle_calculator(" ".join(context.args)))))
    
    # 添加账单相关命令
    dispatcher.add_handler(CommandHandler("allbills", show_all_bills_menu))  # 所有账单命令
    dispatcher.add_handler(CommandHandler("financial", show_financial_summary))  # 财务账单命令
    dispatcher.add_handler(CommandHandler("income", show_income_statement))  # 收入财务账单命令，先选群组再选日期
    
    # 添加按钮回调处理器
    dispatcher.add_handler(CallbackQueryHandler(button_callback))
    
    # Allow anyone to set admin initially
    dispatcher.add_handler(CommandHandler("set_admin", set_admin))
    
    # 处理群聊中的所有消息，注意配置优先级
    dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_text_message), group=1)
    
    # 在启动前尝试删除任何可能存在的webhook
    updater.bot.delete_webhook()
    
    # 记录日志
    logger.info(f"已注册消息处理器")

    # Start a job to check date change every hour
    job_queue = updater.job_queue
    job_queue.run_repeating(check_date_change, interval=RESET_CHECK_INTERVAL, first=0)
    
    # 设置定时保存数据的任务
    job_queue.run_repeating(lambda context: save_data(), interval=300, first=60)  # 每5分钟保存一次
    logger.info("已设置每5分钟保存一次数据")
    
    logger.info(f"已设置每 {RESET_CHECK_INTERVAL} 秒检查日期变更")
    
    # 记录已加载的配置
    logger.info(f"管理员ID: {admin_user_id}")
    logger.info(f"初始操作人: {group_operators}")
    
    # 启动健康检查服务器，防止Render休眠
    threading.Thread(target=start_health_server, daemon=True).start()
    
    # 启动机器人，并设置使其处理群组中的所有消息
    logger.info("开始运行机器人...")
    
    # 确保使用所有可能的更新类型，特别是文本消息
    updater.start_polling(
        timeout=30,
        drop_pending_updates=True,
        allowed_updates=['message', 'edited_message', 'channel_post', 'edited_channel_post', 'callback_query']
    )
    logger.info("机器人已成功启动并正在监听消息...")
    updater.idle()

def set_rate(update: Update, context: CallbackContext) -> None:
    """Set the fee rate."""
    if not is_authorized(update):
        return
    
    if not context.args or len(context.args) < 1:
        update.message.reply_text('使用方法: /rate <费率百分比>')
        return
    
    # 获取聊天ID
    chat_id = update.effective_chat.id
    
    # 获取该聊天的账单数据
    chat_data = get_chat_accounting(chat_id)
    
    try:
        rate = float(context.args[0])
        chat_data['rate'] = rate
        logger.info(f"聊天 {chat_id} 设置费率: {rate}%")
        update.message.reply_text(f'已设置费率: {rate}%')
        
        # 保存数据
        save_data()
        
        summary(update, context)
    except ValueError:
        update.message.reply_text('费率必须是数字')

def set_fixed_rate(update: Update, context: CallbackContext) -> None:
    """Set the fixed exchange rate."""
    if not is_authorized(update):
        return
    
    if not context.args or len(context.args) < 1:
        update.message.reply_text('使用方法: /fixed_rate <汇率>')
        return
    
    # 获取聊天ID
    chat_id = update.effective_chat.id
    
    # 获取该聊天的账单数据
    chat_data = get_chat_accounting(chat_id)
    
    try:
        rate = float(context.args[0])
        chat_data['fixed_rate'] = rate
        logger.info(f"聊天 {chat_id} 设置汇率: {rate}")
        update.message.reply_text(f'已设置固定汇率: {rate}')
        summary(update, context)
    except ValueError:
        update.message.reply_text('汇率必须是数字')

def show_income_statement(update: Update, context: CallbackContext) -> None:
    """显示财务查账，先选择日期，再选择群组"""
    if not is_authorized(update):
        return
    
    logger.info("显示财务查账")
    send_date_selection_first(update, context)

def send_date_selection_first(update_or_query, context):
    """发送日期选择界面，作为第一步"""
    # 判断是update还是query
    is_query = hasattr(update_or_query, 'edit_message_text')
    
    # 获取最近7天的日期列表
    dates = []
    for i in range(7):
        date = (datetime.datetime.now(timezone) - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
        dates.append(date)
    
    # 找出有记录的日期
    dates_with_records = []
    for date_str in dates:
        # 检查所有群组在该日期是否有记录
        has_records = False
        for chat_id, chat_data in chat_accounting.items():
            try:
                # 检查存款记录
                for deposit in chat_data['deposits']:
                    record_date = deposit['time'].split(' ')[0]
                    if record_date == date_str:
                        has_records = True
                        break
                
                if has_records:
                    break
                
                # 检查提款记录
                for withdrawal in chat_data['withdrawals']:
                    record_date = withdrawal['time'].split(' ')[0]
                    if record_date == date_str:
                        has_records = True
                        break
                
                if has_records:
                    break
            except Exception as e:
                logger.error(f"检查聊天 {chat_id} 的记录时出错: {e}")
        
        if has_records:
            dates_with_records.append(date_str)
    
    # 如果没有找到任何有记录的日期
    if not dates_with_records:
        if is_query:
            update_or_query.edit_message_text("没有找到任何日期的记账记录")
        else:
            update_or_query.message.reply_text("没有找到任何日期的记账记录")
        return
    
    # 创建日期选择按钮
    keyboard = []
    row = []
    for i, date in enumerate(dates_with_records):
        row.append(InlineKeyboardButton(date, callback_data=f"select_date_{date}"))
        if (i + 1) % 2 == 0 or i == len(dates_with_records) - 1:  # 每两个日期一行，或者是最后一个日期
            keyboard.append(row)
            row = []
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # 发送消息
    if is_query:
        update_or_query.edit_message_text("请选择要查看的日期:", reply_markup=reply_markup)
    else:
        update_or_query.message.reply_text("请选择要查看的日期:", reply_markup=reply_markup)

def handle_date_selection(query, context, date_str):
    """处理日期选择回调，显示该日期所有群组统计"""
    logger.info(f"选择了日期 {date_str}")
    
    # 保存选择的日期到用户数据中
    context.user_data['selected_date'] = date_str
    logger.info(f"已将日期 {date_str} 保存到用户数据中")
    
    # 查找在该日期有记录的群组
    groups_with_records = []
    for chat_id, chat_data in chat_accounting.items():
        try:
            # 检查是否是群组
            chat = context.bot.get_chat(chat_id)
            if chat.type not in ['group', 'supergroup']:
                continue
            
            # 检查是否有该日期的记录
            has_records = False
            
            # 检查存款记录
            for deposit in chat_data['deposits']:
                record_date = deposit['time'].split(' ')[0]
                if record_date == date_str:
                    has_records = True
                    break
            
            # 检查提款记录，如果还没有找到记录
            if not has_records:
                for withdrawal in chat_data['withdrawals']:
                    record_date = withdrawal['time'].split(' ')[0]
                    if record_date == date_str:
                        has_records = True
                        break
            
            # 如果有该日期的记录，添加到列表
            if has_records:
                groups_with_records.append((chat_id, chat.title))
                logger.info(f"找到群组 {chat.title} ({chat_id}) 在日期 {date_str} 有记录")
        except Exception as e:
            logger.error(f"获取群组 {chat_id} 信息时出错: {e}")
    
    # 如果没有找到任何有记录的群组
    if not groups_with_records:
        query.edit_message_text(f"在 {date_str} 没有找到任何群组的记账记录")
        return
    
    # 直接显示所有群组统计数据
    export_all_groups_statistics(query, context, date_str)

def export_group_by_selected_date(query, context, chat_id):
    """根据选择的日期导出指定群组的账单"""
    date_str = context.user_data.get('selected_date')
    logger.info(f"从用户数据中获取日期: {date_str}")
    
    if not date_str:
        query.edit_message_text("日期信息缺失，请重新开始")
        return
    
    try:
        # 获取群组信息
        chat = context.bot.get_chat(chat_id)
        chat_title = chat.title
        
        logger.info(f"导出群组 {chat_title} ({chat_id}) 在 {date_str} 的账单")
        
        # 获取该聊天的账单数据
        chat_data = get_chat_accounting(chat_id)
        
        # 筛选指定日期的记录
        date_deposits = [d for d in chat_data['deposits'] if d['time'].split(' ')[0] == date_str]
        date_withdrawals = [w for w in chat_data['withdrawals'] if w['time'].split(' ')[0] == date_str]
        
        logger.info(f"群组 {chat_title} 在 {date_str} 有 {len(date_deposits)} 笔存款和 {len(date_withdrawals)} 笔提款")
        
        # 计算统计数据
        deposit_total = sum(deposit['amount'] for deposit in date_deposits)
        deposit_count = len(date_deposits)
        
        withdrawal_total_local = sum(withdraw['amount'] for withdraw in date_withdrawals)
        withdrawal_total_usdt = sum(withdraw['usd_equivalent'] for withdraw in date_withdrawals)
        withdrawal_count = len(date_withdrawals)
        
        # 统计每个用户的入款
        user_deposits = {}
        for deposit in date_deposits:
            username = deposit['user']
            amount = deposit['amount']
            if username not in user_deposits:
                user_deposits[username] = 0
            user_deposits[username] += amount
        
        # 统计每个用户的出款 (使用USDT金额)
        user_withdrawals = {}
        for withdrawal in date_withdrawals:
            username = withdrawal['user']
            # 使用USDT金额而不是本地货币
            amount = withdrawal['usd_equivalent']
            if username not in user_withdrawals:
                user_withdrawals[username] = 0
            user_withdrawals[username] += amount
        
        # 汇率和费率部分
        rate = chat_data.get('fixed_rate', 1.0)
        fee_rate = chat_data.get('rate', 0.0)
        
        # 计算实际金额
        actual_amount = deposit_total / rate if rate != 0 else 0
        
        # 计算应下发金额
        to_be_withdrawn = actual_amount
        already_withdrawn = withdrawal_total_usdt
        not_yet_withdrawn = to_be_withdrawn - already_withdrawn
        
        # 生成账单摘要
        summary_text = f"====== {chat_title} {date_str} 财务账单 ======\n\n"
        
        summary_text += f"入款（{deposit_count}笔）：\n"
        if deposit_count > 0:
            for username, amount in user_deposits.items():
                summary_text += f"  {username}: {amount:.2f}\n"
        else:
            summary_text += "  暂无入金\n"
        
        summary_text += f"\n下发（{withdrawal_count}笔）：\n"
        if withdrawal_count > 0:
            for username, amount in user_withdrawals.items():
                summary_text += f"  {username}: {amount:.2f}\n"
        else:
            summary_text += "  暂无下发\n"
        
        summary_text += f"\n费率：{fee_rate}%\n"
        summary_text += f"固定汇率：{rate}\n"
        summary_text += f"总入款：{deposit_total:.2f}\n"
        summary_text += f"应下发：{deposit_total:.2f}｜{to_be_withdrawn:.2f}U\n"
        summary_text += f"已下发：{withdrawal_total_local:.2f}｜{already_withdrawn:.2f}U\n"
        summary_text += f"未下发：{deposit_total-withdrawal_total_local:.2f}｜{not_yet_withdrawn:.2f}U\n"
        
        # 导出为TXT文件
        file_path = export_group_date_data_to_txt(chat_title, date_str, summary_text, date_deposits, date_withdrawals)
        
        # 发送文件给用户
        if file_path:
            with open(file_path, 'rb') as file:
                context.bot.send_document(
                    chat_id=query.message.chat_id,
                    document=file,
                    filename=f"{chat_title}_{date_str}_账单.txt",
                    caption=f"{chat_title} {date_str} 财务账单导出文件"
                )
            logger.info(f"已导出 {chat_title} 在 {date_str} 的账单数据到 {file_path}")
            
            # 更新消息，表示导出成功
            keyboard = [
                [InlineKeyboardButton("返回群组选择", callback_data=f"back_to_groups_for_date_{date_str}")],
                [InlineKeyboardButton("返回日期选择", callback_data="back_to_dates_first")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text(f"已成功导出 {chat_title} 在 {date_str} 的账单数据", reply_markup=reply_markup)
        else:
            # 更新消息，表示导出失败
            keyboard = [[InlineKeyboardButton("返回", callback_data=f"back_to_groups_for_date_{date_str}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text(f"未能导出 {chat_title} 在 {date_str} 的账单数据", reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"导出群组日期账单时出错: {e}", exc_info=True)
        keyboard = [[InlineKeyboardButton("返回", callback_data="back_to_dates_first")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(f"导出账单时出错: {str(e)}", reply_markup=reply_markup)

def export_group_date_data_to_txt(chat_title, date_str, summary_text, deposits, withdrawals):
    """将群组指定日期的账单数据导出为TXT文件"""
    try:
        # 创建导出文件夹
        export_dir = "exports"
        os.makedirs(export_dir, exist_ok=True)
        
        # 创建文件名
        timestamp = datetime.datetime.now(timezone).strftime("%Y%m%d%H%M%S")
        file_name = f"{chat_title}_{date_str}_{timestamp}.txt"
        file_path = os.path.join(export_dir, file_name)
        
        # 统计数据
        deposit_total = sum(deposit['amount'] for deposit in deposits)
        deposit_count = len(deposits)
        
        withdrawal_total_local = sum(withdraw['amount'] for withdraw in withdrawals)
        withdrawal_total_usdt = sum(withdraw.get('usd_equivalent', 0) for withdraw in withdrawals if 'usd_equivalent' in withdraw)
        withdrawal_count = len(withdrawals)
        
        # 查找对应的聊天数据以获取汇率和费率
        chat_id = None
        chat_data = None
        for cid, data in chat_accounting.items():
            try:
                chat = None
                try:
                    from telegram import Bot
                    bot = Bot(token=BOT_TOKEN)
                    chat = bot.get_chat(cid)
                    if chat.type in ['group', 'supergroup'] and chat.title == chat_title:
                        chat_id = cid
                        chat_data = data
                        break
                except:
                    # 如果无法获取聊天信息，则跳过
                    continue
            except Exception as e:
                logger.error(f"检查聊天ID {cid} 时出错: {e}")
        
        # 获取汇率和费率
        rate = chat_data.get('fixed_rate', 1.0) if chat_data else 1.0
        fee_rate = chat_data.get('rate', 0.0) if chat_data else 0.0
        
        with open(file_path, 'w', encoding='utf-8') as file:
            # 写入标题和导出时间
            now = datetime.datetime.now(timezone).strftime('%Y-%m-%d %H:%M:%S')
            file.write(f"===== {chat_title} {date_str} 财务账单 =====\n")
            file.write(f"导出时间: {now}\n\n")
            
            # 写入账单摘要 - 使用与详细账单相同的格式
            file.write(f"====== {chat_title} ======\n\n")
            
            # 显示入款部分
            file.write(f"入款（{deposit_count}笔）：\n")
            if deposit_count > 0:
                # 按时间排序，最新的在前面
                sorted_deposits = sorted(deposits, key=lambda x: x.get('time', ''), reverse=True)
                # 获取最新的6笔入款记录，如果不到6笔就显示全部
                latest_deposits = sorted_deposits[:min(6, len(sorted_deposits))]
                
                # 显示每个入款记录及其回复人
                for deposit in latest_deposits:
                    amount = deposit['amount']
                    # 计算美元等值：金额除以汇率
                    usd_equivalent = amount / rate if rate != 0 else 0
                    responder = deposit.get('responder', '无回复人')
                    
                    # 提取时间戳中的小时和分钟
                    time_str = deposit.get('time', '')
                    time_parts = time_str.split(' ')
                    if len(time_parts) > 1:
                        time_part = time_parts[1]  # 获取时间部分 (HH:MM:SS)
                        hour_min = ':'.join(time_part.split(':')[:2])  # 只保留小时和分钟
                    else:
                        hour_min = "00:00"  # 默认时间
                        
                    # 使用新的格式: HH:MM 金额/汇率 =美元等值 回复人
                    responder_display = "" if responder is None or responder == "None" else responder
                    file.write(f"  {hour_min} {amount:.0f}/{rate} ={usd_equivalent:.2f} {responder_display}\n")
            else:
                file.write("  暂无入金\n")
            
            # 统计以回复用户为分类的入款信息
            responder_deposits = {}
            for deposit in deposits:
                # 只处理有回复者信息的记录
                if 'responder' in deposit and deposit['responder']:
                    responder = deposit['responder']
                    username = deposit['user']
                    amount = deposit['amount']
                    
                    # 创建或更新此回复者的记录
                    if responder not in responder_deposits:
                        responder_deposits[responder] = {'total': 0, 'users': {}}
                    
                    responder_deposits[responder]['total'] += amount
                    
                    # 记录是哪个用户对这个回复者进行了入款
                    if username not in responder_deposits[responder]['users']:
                        responder_deposits[responder]['users'][username] = 0
                    responder_deposits[responder]['users'][username] += amount
            
            # 计算分类人数
            responder_count = len(responder_deposits)
            
            # 写入用户分类信息
            file.write(f"\n分类（{responder_count}人）：\n")
            if responder_count > 0:
                for responder, data in responder_deposits.items():
                    total_amount = data['total']
                    # 对于每个回复者，只显示总金额，不显示来源
                    file.write(f"  {responder} {total_amount:.2f}\n")
            else:
                file.write("  暂无分类\n")
            
            # 写入下发信息
            file.write(f"\n下发（{withdrawal_count}笔）：\n")
            if withdrawal_count > 0:
                # 统计每个用户的出款
                user_withdrawals = {}
                for withdrawal in withdrawals:
                    username = withdrawal['user']
                    # 使用USDT金额而不是本地货币
                    amount = withdrawal.get('usd_equivalent', withdrawal['amount'])
                    if username not in user_withdrawals:
                        user_withdrawals[username] = 0
                    user_withdrawals[username] += amount
                
                for username, amount in user_withdrawals.items():
                    file.write(f"  {username}: {amount:.2f}\n")
            else:
                file.write("  暂无下发\n")
            
            # 计算应下发金额（USDT）
            actual_amount = deposit_total / rate if rate != 0 else 0
            to_be_withdrawn = actual_amount
            already_withdrawn = withdrawal_total_usdt
            not_yet_withdrawn = to_be_withdrawn - already_withdrawn
            
            # 写入费率和汇率信息
            file.write(f"\n费率：{fee_rate}%\n")
            file.write(f"固定汇率：{rate}\n")
            file.write(f"总入款：{deposit_total:.2f}\n")
            file.write(f"应下发：{deposit_total:.2f}｜{to_be_withdrawn:.2f}U\n")
            file.write(f"已下发：{withdrawal_total_local:.2f}｜{already_withdrawn:.2f}U\n")
            file.write(f"未下发：{deposit_total-withdrawal_total_local:.2f}｜{not_yet_withdrawn:.2f}U\n\n")
            
            # 写入入款明细部分
            file.write("===== 入款明细 =====\n")
            if deposits:
                for i, deposit in enumerate(deposits, 1):
                    file.write(f"{i}. 时间: {deposit['time']}, 金额: {deposit['amount']:.2f}, 用户: {deposit['user']}")
                    if 'responder' in deposit and deposit['responder']:
                        responder_display = deposit['responder']
                        if responder_display and responder_display != "None":
                            file.write(f", 回复人: {responder_display}")
                    file.write(f", USD等值: {deposit.get('usd_equivalent', 0):.2f}\n")
            else:
                file.write("暂无入款记录\n")
            
            # 写入出款明细部分
            file.write("\n===== 出款明细 =====\n")
            if withdrawals:
                for i, withdrawal in enumerate(withdrawals, 1):
                    file.write(f"{i}. 时间: {withdrawal['time']}, 金额: {withdrawal['amount']:.2f}, ")
                    file.write(f"用户: {withdrawal['user']}, USD等值: {withdrawal.get('usd_equivalent', 0):.2f}\n")
            else:
                file.write("暂无出款记录\n")
        
        logger.info(f"已将 {chat_title} 的 {date_str} 账单数据导出到 {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"导出账单数据到TXT文件时出错: {e}", exc_info=True)
        return None

def export_yesterday_bill(update, context):
    """导出昨日所有群组的账单数据"""
    logger.info("导出昨日所有群组账单")
    
    # 计算昨天的日期
    yesterday = (datetime.datetime.now(timezone) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    
    # 获取当前聊天ID
    chat_id = update.effective_chat.id
    
    # 发送开始导出的消息
    status_message = update.message.reply_text(f"正在导出 {yesterday} 的账单数据...")
    
    try:
        # 获取群组信息
        chat = context.bot.get_chat(chat_id)
        chat_title = chat.title if chat.type in ['group', 'supergroup'] else "私聊"
        
        # 获取该聊天的账单数据
        chat_data = get_chat_accounting(chat_id)
        
        # 筛选指定日期的记录
        date_deposits = [d for d in chat_data['deposits'] if d['time'].split(' ')[0] == yesterday]
        date_withdrawals = [w for w in chat_data['withdrawals'] if w['time'].split(' ')[0] == yesterday]
        
        # 如果没有记录，通知用户
        if not date_deposits and not date_withdrawals:
            context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_message.message_id,
                text=f"在 {yesterday} 没有找到任何记账记录"
            )
            return
        
        # 计算统计数据
        deposit_total = sum(deposit['amount'] for deposit in date_deposits)
        deposit_count = len(date_deposits)
        
        withdrawal_total_local = sum(withdraw['amount'] for withdraw in date_withdrawals)
        withdrawal_total_usdt = sum(withdraw.get('usd_equivalent', 0) for withdraw in date_withdrawals)
        withdrawal_count = len(date_withdrawals)
        
        # 汇率和费率部分
        rate = chat_data.get('fixed_rate', 1.0)
        fee_rate = chat_data.get('rate', 0.0)
        
        # 生成摘要文本
        summary_text = f"正在处理 {chat_title} {yesterday} 的账单数据..."
        
        # 导出为TXT文件
        file_path = export_group_date_data_to_txt(chat_title, yesterday, summary_text, date_deposits, date_withdrawals)
        
        # 发送文件给用户
        if file_path:
            with open(file_path, 'rb') as file:
                context.bot.send_document(
                    chat_id=chat_id,
                    document=file,
                    filename=f"{chat_title}_{yesterday}_账单.txt",
                    caption=f"{chat_title} {yesterday} 财务账单导出文件"
                )
            logger.info(f"已导出 {chat_title} {yesterday} 的账单数据到 {file_path}")
            
            # 更新消息，表示导出成功
            context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_message.message_id,
                text=f"已成功导出 {chat_title} {yesterday} 的账单数据"
            )
        else:
            # 更新消息，表示导出失败
            context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_message.message_id,
                text=f"未能导出 {chat_title} {yesterday} 的账单数据"
            )
    except Exception as e:
        logger.error(f"导出昨日账单时出错: {e}", exc_info=True)
        update.message.reply_text(f"❌ 导出昨日账单时出错: {str(e)}")
        return

def query_trc20_usdt_balance(address):
    """查询TRC20-USDT余额（波场链）"""
    try:
        # 检查是否是波场地址
        if not address.startswith('T'):
            return None
        
        # 使用requests库调用TronGrid API查询余额
        import requests
        
        # USDT合约地址 (TRC20-USDT)
        contract_address = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
        
        # 构建API请求
        url = f"https://api.trongrid.io/v1/accounts/{address}/tokens"
        headers = {
            "Accept": "application/json",
            "User-Agent": "Telegram Bot"
        }
        
        # 发送请求
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            
            # 寻找USDT代币数据
            if 'data' in data:
                for token in data['data']:
                    if token.get('tokenId') == contract_address or token.get('contract_address') == contract_address:
                        # 找到USDT，提取余额
                        balance = float(token.get('balance', 0))
                        # TRC20代币通常有6位小数
                        balance = balance / 1000000
                        return balance
                
                # 如果循环完成但没有找到USDT，则余额为0
                return 0
            else:
                # 如果没有data字段，则可能账户不存在或没有代币
                return 0
        else:
            logger.error(f"TronGrid API返回错误代码: {response.status_code}")
            return 0
            
    except Exception as e:
        logger.error(f"查询TRC20-USDT余额时出错: {e}", exc_info=True)
        return 0

def query_trc20_usdt_balance(address):
    """查询TRC20-USDT余额（波场链）- 使用多个API源"""
    try:
        # 检查是否是波场地址
        if not address.startswith('T'):
            logger.warning(f"地址 {address} 不是波场地址")
            return None
        
        logger.info(f"开始查询地址 {address} 的USDT余额")
        
        # 使用requests库进行API调用
        import requests
        import json
        import time
        
        # USDT合约地址 (TRC20-USDT)
        contract_address = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
        
        # 1. 尝试使用Blockchair API (更可靠)
        try:
            logger.info(f"尝试使用Blockchair API查询地址 {address} 的USDT余额")
            blockchair_url = f"https://api.blockchair.com/tron/raw/address/{address}"
            blockchair_headers = {
                "Accept": "application/json",
                "User-Agent": "Telegram Bot/1.0"
            }
            
            blockchair_response = requests.get(blockchair_url, headers=blockchair_headers, timeout=15)
            
            if blockchair_response.status_code == 200:
                data = blockchair_response.json()
                
                # 检查是否有data和token_balances字段
                if 'data' in data and address in data['data'] and 'token_balances' in data['data'][address]:
                    token_balances = data['data'][address]['token_balances']
                    
                    # 查找USDT余额
                    for token in token_balances:
                        if token.get('contract') == contract_address or token.get('name') == 'Tether USD' or token.get('symbol') == 'USDT':
                            balance = float(token.get('balance', 0))
                            decimals = int(token.get('decimals', 6))
                            usdt_balance = balance / (10 ** decimals)
                            logger.info(f"Blockchair API查询成功: {usdt_balance} USDT")
                            return usdt_balance
                
                logger.warning("Blockchair API未返回USDT余额信息")
            else:
                logger.warning(f"Blockchair API返回错误: {blockchair_response.status_code}")
        except Exception as e:
            logger.error(f"使用Blockchair API查询出错: {str(e)}")
        
        # 2. 尝试使用TronScan API
        try:
            logger.info(f"尝试使用TronScan API查询地址 {address} 的USDT余额")
            tronscan_url = f"https://apilist.tronscan.org/api/account"
            tronscan_params = {
                "address": address
            }
            tronscan_headers = {
                "User-Agent": "Telegram Bot/1.0"
            }
            
            tronscan_response = requests.get(tronscan_url, params=tronscan_params, headers=tronscan_headers, timeout=15)
            
            if tronscan_response.status_code == 200:
                data = tronscan_response.json()
                
                # 检查trc20token_balances字段
                if 'trc20token_balances' in data:
                    for token in data['trc20token_balances']:
                        if token.get('tokenId') == contract_address or token.get('symbol') == 'USDT':
                            balance = float(token.get('balance', 0))
                            decimals = int(token.get('decimals', 6))
                            usdt_balance = balance / (10 ** decimals)
                            logger.info(f"TronScan API查询成功: {usdt_balance} USDT")
                            return usdt_balance
                
                logger.warning("TronScan API未返回USDT余额信息")
            else:
                logger.warning(f"TronScan API返回错误: {tronscan_response.status_code}")
        except Exception as e:
            logger.error(f"使用TronScan API查询出错: {str(e)}")
        
        # 3. 最后尝试使用TronGrid API (原实现)
        try:
            logger.info(f"尝试使用TronGrid API查询地址 {address} 的USDT余额")
            trongrid_url = f"https://api.trongrid.io/v1/accounts/{address}/tokens"
            trongrid_headers = {
                "Accept": "application/json",
                "User-Agent": "Telegram Bot/1.0"
            }
            
            trongrid_response = requests.get(trongrid_url, headers=trongrid_headers, timeout=15)
            
            if trongrid_response.status_code == 200:
                data = trongrid_response.json()
                
                # 寻找USDT代币数据
                if 'data' in data:
                    for token in data['data']:
                        if token.get('tokenId') == contract_address or token.get('contract_address') == contract_address:
                            # 找到USDT，提取余额
                            balance = float(token.get('balance', 0))
                            # TRC20代币通常有6位小数
                            usdt_balance = balance / 1000000
                            logger.info(f"TronGrid API查询成功: {usdt_balance} USDT")
                            return usdt_balance
                
                logger.warning("TronGrid API未返回USDT余额信息")
            else:
                logger.warning(f"TronGrid API返回错误: {trongrid_response.status_code}")
        except Exception as e:
            logger.error(f"使用TronGrid API查询出错: {str(e)}")
        
        # 所有API都查询失败，返回0
        logger.error(f"所有API查询地址 {address} 的USDT余额均失败")
        return 0
            
    except Exception as e:
        logger.error(f"查询TRC20-USDT余额时出错: {e}", exc_info=True)
        return 0

def handle_usdt_query(update, context):
    """处理USDT地址余额查询请求"""
    reply_to_message = update.message.reply_to_message
    
    # 检查回复的消息是否存在
    if not reply_to_message:
        update.message.reply_text("❌ 无法识别要查询的地址，请确保回复包含USDT地址的消息")
        return
    
    # 尝试从不同字段提取USDT地址
    usdt_address = None
    
    # 从文本中提取
    if reply_to_message.text:
        usdt_address = extract_usdt_address(reply_to_message.text)
    
    # 如果文本中没有找到，尝试从caption中提取
    if not usdt_address and reply_to_message.caption:
        usdt_address = extract_usdt_address(reply_to_message.caption)
    
    # 如果是转发的消息，尝试从转发信息中提取
    if not usdt_address and hasattr(reply_to_message, 'forward_from_message_id'):
        if reply_to_message.forward_text:
            usdt_address = extract_usdt_address(reply_to_message.forward_text)
        elif reply_to_message.forward_caption:
            usdt_address = extract_usdt_address(reply_to_message.forward_caption)
    
    # 尝试从实体(entities)中提取
    if not usdt_address and hasattr(reply_to_message, 'entities') and reply_to_message.entities:
        for entity in reply_to_message.entities:
            if entity.type in ['text_link', 'url', 'code', 'pre']:
                # 提取该实体对应的文本
                start = entity.offset
                end = entity.offset + entity.length
                entity_text = reply_to_message.text[start:end]
                usdt_address = extract_usdt_address(entity_text)
                if usdt_address:
                    break
    
    # 尝试从转发人名称提取
    if not usdt_address and reply_to_message.forward_sender_name:
        usdt_address = extract_usdt_address(reply_to_message.forward_sender_name)
    
    # 如果所有尝试都失败，作为最后的尝试，搜索整个消息字符串表示
    if not usdt_address:
        # 将消息对象转换为字符串，尝试提取任何看起来像地址的内容
        message_str = str(reply_to_message)
        usdt_address = extract_usdt_address(message_str)
    
    if not usdt_address:
        update.message.reply_text("❌ 未能在消息中找到有效的USDT地址，请确保回复包含正确格式的地址")
        return
    
    # 发送正在查询的消息
    status_message = update.message.reply_text(f"🔍 正在查询地址 {usdt_address} 的USDT余额，请稍候...")
    
    try:
        # 记录开始查询
        logger.info(f"开始查询地址 {usdt_address} 的USDT余额")
        
        # 查询TRC20-USDT余额（波场链）
        trc20_balance = query_trc20_usdt_balance(usdt_address)
        
        # 获取当前时间 (简短格式)
        current_time = datetime.datetime.now(timezone).strftime('%H:%M:%S')
        current_date = datetime.datetime.now(timezone).strftime('%Y-%m-%d')
        
        # 完全按照用户要求的简洁模板
        if trc20_balance is not None:
            balance_text = f"该地址余额：{trc20_balance:.6f} USDT\n\n"
        else:
            balance_text = f"查询失败：无法获取余额\n\n"
        
        # 添加完整地址信息
        balance_text += f"地址：{usdt_address}\n"
        balance_text += f"注意：请核对与您查询的地址是否一致"
        
        # 只进行一次消息更新
        context.bot.edit_message_text(
            chat_id=status_message.chat_id,
            message_id=status_message.message_id,
            text=balance_text,
            parse_mode=ParseMode.HTML
        )
        
        logger.info(f"已查询地址 {usdt_address} 的USDT余额: {trc20_balance}")
    except Exception as e:
        logger.error(f"查询USDT余额时出错: {e}", exc_info=True)
        context.bot.edit_message_text(
            chat_id=status_message.chat_id,
            message_id=status_message.message_id,
            text=f"❌ 查询USDT余额时出错: {str(e)}"
        )

def export_current_bill(query, context, chat_id):
    """导出当前账单为文件，包括入款和出款记录"""
    user = query.from_user
    
    # 检查是否有权限
    if not is_global_admin(user.id, user.username) and not is_operator(user.username, chat_id):
        query.edit_message_text("您没有权限查看此账单")
        logger.warning(f"用户 {user.id} (@{user.username}) 尝试未授权查看账单")
        return
    
    try:
        # 获取群聊信息
        chat = context.bot.get_chat(chat_id)
        chat_title = getattr(chat, 'title', f'Chat {chat_id}')
        
        # 更新消息，表示正在生成账单
        query.edit_message_text(f"正在生成 {chat_title} 账单...")
        
        # 获取该聊天的账单数据
        chat_data = get_chat_accounting(chat_id)
        
        # 生成摘要
        summary_text = generate_bill_summary(chat_id, chat_title, chat_data)
        
        # 导出为文件
        file_path = export_group_data_to_txt(chat_title, summary_text)
        
        if file_path:
            # 发送文件
            with open(file_path, 'rb') as f:
                context.bot.send_document(
                    chat_id=query.message.chat_id,
                    document=f,
                    filename=f"{chat_title}_账单.txt",
                    caption=f"{chat_title} 账单详情"
                )
            # 删除临时文件
            os.remove(file_path)
            logger.info(f"已发送群组 {chat_id} 的账单文件")
            
            # 更新消息
            keyboard = [[InlineKeyboardButton("查看历史账单", callback_data=f"view_history_{chat_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text("账单已导出为文件", reply_markup=reply_markup)
        else:
            query.edit_message_text("导出账单时出错")
            logger.error(f"导出群组 {chat_id} 账单时出错")
    except Exception as e:
        logger.error(f"导出当前账单时出错: {e}", exc_info=True)
        query.edit_message_text(f"导出账单时出错: {str(e)}")

def generate_bill_summary(chat_id, chat_title, chat_data):
    """生成账单摘要文本"""
    # 收款部分
    deposit_total = sum(deposit['amount'] for deposit in chat_data['deposits'])
    deposit_count = len(chat_data['deposits'])
    
    # 汇率和费率部分
    rate = chat_data.get('fixed_rate', 1.0)
    fee_rate = chat_data.get('rate', 0.0)
    
    # 计算实际金额
    actual_amount = deposit_total / rate if rate != 0 else 0
    
    # 出款部分
    withdrawal_total_local = sum(withdraw['amount'] for withdraw in chat_data['withdrawals'])
    withdrawal_total_usdt = sum(withdraw['usd_equivalent'] for withdraw in chat_data['withdrawals'])
    withdrawal_count = len(chat_data['withdrawals'])
    
    # 计算应下发金额
    to_be_withdrawn = actual_amount
    already_withdrawn = withdrawal_total_usdt
    not_yet_withdrawn = to_be_withdrawn - already_withdrawn
    
    # 生成摘要文本
    summary_text = f"====== {chat_title} 账单明细 ======\n\n"
    
    # 添加日期和时间信息
    current_date = datetime.datetime.now(timezone).strftime('%Y-%m-%d')
    current_time = datetime.datetime.now(timezone).strftime('%H:%M:%S')
    summary_text += f"生成时间: {current_date} {current_time}\n\n"
    
    # 添加统计信息
    summary_text += f"费率: {fee_rate}%\n"
    summary_text += f"固定汇率: {rate}\n"
    summary_text += f"总入款: {deposit_total:.2f}\n"
    summary_text += f"应下发: {to_be_withdrawn:.2f}U\n"
    summary_text += f"已下发: {already_withdrawn:.2f}U\n"
    summary_text += f"未下发: {not_yet_withdrawn:.2f}U\n\n"
    
    # 入款明细
    summary_text += f"===== 入款明细 =====\n"
    if deposit_count > 0:
        # 按时间排序
        sorted_deposits = sorted(chat_data['deposits'], key=lambda x: x.get('time', ''), reverse=True)
        
        # 显示每个入款记录
        for i, deposit in enumerate(sorted_deposits, 1):
            amount = deposit['amount']
            username = deposit['user']
            time_str = deposit.get('time', '未知时间')
            # 计算美元等值
            usd_equivalent = amount / rate if rate != 0 else 0
            
            summary_text += f"{i}. 时间: {time_str}, 金额: {amount:.2f}, 用户: {username}, USD等值: {usd_equivalent:.2f}\n"
    else:
        summary_text += "暂无入款记录\n"
    
    # 出款明细
    summary_text += f"\n===== 出款明细 =====\n"
    if withdrawal_count > 0:
        # 按时间排序
        sorted_withdrawals = sorted(chat_data['withdrawals'], key=lambda x: x.get('time', ''), reverse=True)
        
        # 显示每个出款记录
        for i, withdrawal in enumerate(sorted_withdrawals, 1):
            amount = withdrawal['amount']
            username = withdrawal['user']
            time_str = withdrawal.get('time', '未知时间')
            usd_equivalent = withdrawal['usd_equivalent']
            
            summary_text += f"{i}. 时间: {time_str}, 金额: {amount:.2f}, 用户: {username}, USD等值: {usd_equivalent:.2f}\n"
    else:
        summary_text += "暂无出款记录\n"
    
    return summary_text

def summary(update: Update, context: CallbackContext) -> None:
    """Show accounting summary."""
    if not is_authorized(update):
        return
        
    chat_id = update.effective_chat.id
    chat_type = update.effective_chat.type
    chat_title = getattr(update.effective_chat, 'title', 'Private Chat')
    
    logger.info(f"生成聊天 {chat_id} ({chat_title}) 账单摘要")
    
    # 获取该聊天的账单数据
    chat_data = get_chat_accounting(chat_id)
    
    # 收款部分
    deposit_total = sum(deposit['amount'] for deposit in chat_data['deposits'])
    deposit_count = len(chat_data['deposits'])
    
    # 统计每个用户的入款
    user_deposits = {}
    for deposit in chat_data['deposits']:
        username = deposit['user']
        amount = deposit['amount']
        if username not in user_deposits:
            user_deposits[username] = 0
        user_deposits[username] += amount
    
    # 统计以回复用户为分类的入款信息
    responder_deposits = {}
    for deposit in chat_data['deposits']:
        # 只处理有回复者信息的记录
        if 'responder' in deposit and deposit['responder']:
            responder = deposit['responder']
            username = deposit['user']
            amount = deposit['amount']
            
            # 创建或更新此回复者的记录
            if responder not in responder_deposits:
                responder_deposits[responder] = {'total': 0, 'users': {}}
            
            responder_deposits[responder]['total'] += amount
            
            # 记录是哪个用户对这个回复者进行了入款
            if username not in responder_deposits[responder]['users']:
                responder_deposits[responder]['users'][username] = 0
            responder_deposits[responder]['users'][username] += amount
    
    # 计算用户分类数量
    user_count = len(chat_data['users'])
    responder_count = len(responder_deposits)
    
    # 汇率和费率部分
    rate = chat_data.get('fixed_rate', 1.0)
    fee_rate = chat_data.get('rate', 0.0)
    
    # 计算实际金额 - 使用除法计算
    actual_amount = deposit_total / rate if rate != 0 else 0
    
    # 出款部分 - 从withdrawals中提取USDT金额(usd_equivalent)
    withdrawal_total_local = sum(withdraw['amount'] for withdraw in chat_data['withdrawals'])
    withdrawal_total_usdt = sum(withdraw['usd_equivalent'] for withdraw in chat_data['withdrawals'])
    withdrawal_count = len(chat_data['withdrawals'])
    
    # 计算应下发金额（USDT）
    to_be_withdrawn = actual_amount
    already_withdrawn = withdrawal_total_usdt
    not_yet_withdrawn = to_be_withdrawn - already_withdrawn
    
    # 按照用户要求的模板格式生成账单摘要
    summary_text = f"====== {chat_title} ======\n\n"
    
    summary_text += f"入款（{deposit_count}笔）：\n"
    if deposit_count > 0:
        # 按时间排序，最新的在前面
        sorted_deposits = sorted(chat_data['deposits'], key=lambda x: x.get('time', ''), reverse=True)
        # 获取最新的6笔入款记录
        latest_deposits = sorted_deposits[:6]
        
        # 显示每个入款记录及其回复人
        for deposit in latest_deposits:
            amount = deposit['amount']
            # 计算美元等值：金额除以汇率
            usd_equivalent = amount / rate if rate != 0 else 0
            responder = deposit.get('responder', '无回复人')
            
            # 提取时间戳中的小时和分钟
            time_str = deposit.get('time', '')
            time_parts = time_str.split(' ')
            if len(time_parts) > 1:
                time_part = time_parts[1]  # 获取时间部分 (HH:MM:SS)
                hour_min = ':'.join(time_part.split(':')[:2])  # 只保留小时和分钟
            else:
                hour_min = "00:00"  # 默认时间
                
            # 使用新的格式: HH:MM 金额/汇率 =美元等值 回复人
            responder_display = "" if responder is None or responder == "None" else responder
            summary_text += f"  {hour_min} {amount:.0f}/{rate} ={usd_equivalent:.2f} {responder_display}\n"
    else:
        summary_text += "  暂无入金\n"
    
    summary_text += f"\n分类（{responder_count}人）：\n"
    if responder_count > 0:
        for responder, data in responder_deposits.items():
            total_amount = data['total']
            # 简化显示格式，只显示回复者和金额
            summary_text += f"  {responder} {total_amount:.2f}\n"
    else:
        summary_text += "  暂无分类\n"
    
    summary_text += f"\n下发（{withdrawal_count}笔）：\n"
    if withdrawal_count > 0:
        user_withdrawals = {}
        for withdrawal in chat_data['withdrawals']:
            username = withdrawal['user']
            # 使用USDT金额而不是本地货币
            amount = withdrawal['usd_equivalent']
            if username not in user_withdrawals:
                user_withdrawals[username] = 0
            user_withdrawals[username] += amount
        
        for username, amount in user_withdrawals.items():
            summary_text += f"  {username}: {amount:.2f}\n"
    else:
        summary_text += "  暂无下发\n"
    
    summary_text += f"\n费率：{fee_rate}%\n"
    summary_text += f"固定汇率：{rate}\n"
    summary_text += f"总入款：{deposit_total:.2f}\n"
    summary_text += f"应下发：{deposit_total:.2f}｜{to_be_withdrawn:.2f}U\n"
    summary_text += f"已下发：{withdrawal_total_local:.2f}｜{already_withdrawn:.2f}U\n"
    summary_text += f"未下发：{deposit_total-withdrawal_total_local:.2f}｜{not_yet_withdrawn:.2f}U\n"
    
    try:
        # 创建账单和历史记录按钮
        keyboard = [
            [InlineKeyboardButton("详细账单", callback_data=f"export_bill_{chat_id}")],
            [InlineKeyboardButton("历史账单", callback_data=f"view_history_{chat_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # 使用reply_text确保消息总是发送，不管是否在群组中
        update.message.reply_text(summary_text, reply_markup=reply_markup)
        logger.info(f"已显示账单摘要，字符长度: {len(summary_text)}")
    except Exception as e:
        logger.error(f"发送账单摘要时出错: {e}", exc_info=True)
        try:
            # 尝试使用bot.send_message作为备选方案
            keyboard = [
                [InlineKeyboardButton("详细账单", callback_data=f"export_bill_{chat_id}")],
                [InlineKeyboardButton("历史账单", callback_data=f"view_history_{chat_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            context.bot.send_message(chat_id=chat_id, text=summary_text, reply_markup=reply_markup)
            logger.info(f"使用备选方法发送账单摘要成功")
        except Exception as e2:
            logger.error(f"备选方法发送账单摘要也失败: {e2}", exc_info=True)

def button_callback(update: Update, context: CallbackContext) -> None:
    """Handle button callbacks from inline keyboards."""
    query = update.callback_query
    data = query.data
    
    logger.info(f"收到按钮回调: {data}")
    
    # 确保回调处理后通知Telegram
    query.answer()
    
    # 处理导出当前账单按钮
    if data.startswith("export_bill_"):
        chat_id = int(data.split("_")[2])
        export_current_bill(query, context, chat_id)
    # 处理查看历史账单按钮
    elif data.startswith("view_history_"):
        chat_id = int(data.split("_")[2])
        show_history_selection(query, context, chat_id)
    # 处理历史账单日期选择
    elif data.startswith("history_"):
        parts = data.split("_")
        if len(parts) >= 3:
            chat_id = int(parts[1])
            date_str = parts[2]
            view_historical_bill(query, context, chat_id, date_str)
    # 处理取消按钮
    elif data == "cancel":
        query.edit_message_text("操作已取消")
    # 其他回调处理
    else:
        # 继续处理其他类型的回调
        if data.startswith("allbills_"):
            chat_id = int(data.split("_")[1])
            export_current_group_all_bills(query, context)
        elif data.startswith("export_all_bills_"):
            handle_export_all_bills_command(query, context)
        elif data.startswith("financial_"):
            show_financial_summary(query, context)
        elif data.startswith("select_date_"):
            date_str = data.split("_")[2]
            handle_date_selection(query, context, date_str)
        elif data.startswith("select_chat_"):
            parts = data.split("_")
            if len(parts) >= 4 and parts[1] == "date":
                date_str = parts[2]
                chat_id = int(parts[3])
                export_specific_date_for_chat(query, context, date_str, chat_id)
            else:
                chat_id = int(parts[2])
                export_current_bill(query, context, chat_id)
        elif data.startswith("income_statement_"):
            date_str = data.split("_")[2]
            export_all_groups_statistics(query, context, date_str)
        else:
            logger.warning(f"未知的回调数据: {data}")
            query.edit_message_text("未知的操作")

def show_history_selection(query, context, chat_id):
    """显示历史账单选择界面"""
    user = query.from_user
    
    # 检查是否有权限
    if not is_global_admin(user.id, user.username) and not is_operator(user.username, chat_id):
        query.edit_message_text("您没有权限查看历史账单")
        logger.warning(f"用户 {user.id} (@{user.username}) 尝试未授权查看历史账单")
        return
    
    try:
        # 获取群聊信息
        chat = context.bot.get_chat(chat_id)
        chat_title = getattr(chat, 'title', f'Chat {chat_id}')
        
        # 获取该聊天的账单数据
        chat_data = get_chat_accounting(chat_id)
        
        # 检查是否有历史记录
        if 'history' not in chat_data or not chat_data['history']:
            query.edit_message_text(f"{chat_title} 没有历史账单记录")
            return
        
        # 按日期排序
        dates = sorted(chat_data['history'].keys(), reverse=True)
        
        # 创建日期选择按钮
        keyboard = []
        for date in dates:
            keyboard.append([InlineKeyboardButton(date, callback_data=f"history_{chat_id}_{date}")])
        
        # 添加返回按钮
        keyboard.append([InlineKeyboardButton("返回", callback_data=f"cancel")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(f"请选择要查看的 {chat_title} 历史账单日期:", reply_markup=reply_markup)
        
    except Exception as e:
        logger.error(f"显示历史账单选择界面时出错: {e}", exc_info=True)
        query.edit_message_text(f"显示历史账单选择界面时出错: {str(e)}")

def view_historical_bill(query, context, chat_id, date_str):
    """查看特定日期的历史账单"""
    user = query.from_user
    
    # 检查是否有权限
    if not is_global_admin(user.id, user.username) and not is_operator(user.username, chat_id):
        query.edit_message_text("您没有权限查看历史账单")
        logger.warning(f"用户 {user.id} (@{user.username}) 尝试未授权查看历史账单")
        return
    
    try:
        # 获取群聊信息
        chat = context.bot.get_chat(chat_id)
        chat_title = getattr(chat, 'title', f'Chat {chat_id}')
        
        # 获取该聊天的账单数据
        chat_data = get_chat_accounting(chat_id)
        
        # 检查是否有该日期的历史记录
        if 'history' not in chat_data or date_str not in chat_data['history']:
            query.edit_message_text(f"{chat_title} 没有 {date_str} 的历史账单记录")
            return
        
        # 获取历史数据
        historical_data = chat_data['history'][date_str]
        
        # 生成摘要
        summary_text = generate_bill_summary(chat_id, f"{chat_title} ({date_str})", historical_data)
        
        # 导出为文件
        file_path = export_historical_data_to_txt(chat_title, date_str, summary_text)
        
        if file_path:
            # 发送文件
            with open(file_path, 'rb') as f:
                context.bot.send_document(
                    chat_id=query.message.chat_id,
                    document=f,
                    filename=f"{chat_title}_{date_str}_历史账单.txt",
                    caption=f"{chat_title} {date_str} 历史账单详情"
                )
            # 删除临时文件
            os.remove(file_path)
            logger.info(f"已发送群组 {chat_id} 的 {date_str} 历史账单文件")
            
            # 更新消息
            keyboard = [[InlineKeyboardButton("返回历史选择", callback_data=f"view_history_{chat_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text(f"{date_str} 历史账单已导出为文件", reply_markup=reply_markup)
        else:
            query.edit_message_text("导出历史账单时出错")
            logger.error(f"导出群组 {chat_id} 的 {date_str} 历史账单时出错")
    except Exception as e:
        logger.error(f"查看历史账单时出错: {e}", exc_info=True)
        query.edit_message_text(f"查看历史账单时出错: {str(e)}")

def export_historical_data_to_txt(chat_title, date_str, summary_text):
    """将历史账单数据导出为文本文件"""
    try:
        # 创建临时目录（如果不存在）
        os.makedirs('temp', exist_ok=True)
        
        # 创建文件名
        file_path = os.path.join('temp', f"{chat_title}_{date_str}_history.txt")
        
        # 写入文件
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(summary_text)
        
        return file_path
    except Exception as e:
        logger.error(f"导出历史数据到文本文件时出错: {e}", exc_info=True)
        return None

def save_data():
    """将账单数据保存到文件"""
    try:
        with open('bot_data.json', 'w', encoding='utf-8') as f:
            json.dump({
                'chat_accounting': chat_accounting,
                'group_operators': group_operators,
                'authorized_groups': list(authorized_groups)
            }, f, ensure_ascii=False)
        logger.info("账单数据已保存到文件")
    except Exception as e:
        logger.error(f"保存数据时出错: {e}", exc_info=True)

def load_data():
    """从文件加载账单数据"""
    global chat_accounting, group_operators, authorized_groups
    try:
        if os.path.exists('bot_data.json'):
            with open('bot_data.json', 'r', encoding='utf-8') as f:
                data = json.load(f)
                chat_accounting = data['chat_accounting']
                group_operators = data['group_operators']
                authorized_groups = set(data['authorized_groups'])
            logger.info("成功从文件加载账单数据")
        else:
            logger.info("未找到数据文件，使用默认空数据")
    except Exception as e:
        logger.error(f"加载数据时出错: {e}", exc_info=True)

class HealthCheckHandler(BaseHTTPRequestHandler):
    """健康检查HTTP处理器，防止Render休眠"""
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'Bot is running')
        
    def log_message(self, format, *args):
        # 禁用HTTP请求日志以减少日志噪音
        return

def start_health_server():
    """启动健康检查HTTP服务器"""
    try:
        port = int(os.environ.get('PORT', 10000))
        server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
        logger.info(f"健康检查服务器启动在端口 {port}")
        server.serve_forever()
    except Exception as e:
        logger.error(f"启动健康检查服务器时出错: {e}", exc_info=True)

def shutdown_handler(signum, frame):
    """处理关闭信号，确保在关闭前保存数据"""
    logger.info(f"收到信号 {signum}，保存数据并关闭...")
    save_data()
    sys.exit(0)

if __name__ == '__main__':
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        print("Bot stopped!")
    except Exception as e:
        logger.critical(f"机器人遇到致命错误: {e}", exc_info=True)
        print(f"Fatal error: {e}") 
