# -*- encoding: UTF-8 -*-

import logging
import time

import settings
from dingtalkchatbot.chatbot import DingtalkChatbot, ActionCard, CardItem


def push(msg):
    if settings.config['push']['enable']:
        # WebHook地址
        webhook = settings.config['push']['webhook']
        secret = settings.config['push']['secret']
        # 初始化机器人小丁
        xiaoding = DingtalkChatbot(webhook, secret=secret)  # 方式二：勾选“加签”选项时使用（v1.5以上新功能）
        # Text消息@所有人
        xiaoding.send_text(msg=msg)
        time.sleep(0.5)  # 暂停0.5秒

    logging.info(msg)


def statistics(msg=None):
    push(msg)


def strategy(msg=None):
    if msg is None or not msg:
        msg = '今日没有符合条件的股票'
    push(msg)
