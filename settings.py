# -*- encoding: UTF-8 -*-
import yaml
import os
import baostock as bs
import akshare as ak  # 注意：baostock不支持龙虎榜数据，这里保留akshare用于龙虎榜数据获取


def init():
    global config
    root_dir = os.path.dirname(os.path.abspath(__file__))  # This is your Project Root
    config_file = os.path.join(root_dir, 'config.yaml')
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)


def config():
    return config
