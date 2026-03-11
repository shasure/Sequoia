# -*- encoding: UTF-8 -*-
import os
from datetime import datetime

import logging

import pandas as pd

import settings
from const import MARKET_A, MARKET_HK


def fetch_a_stock_data(stock_code):
    """根据股票代码获取A股数据"""
    stock_data_dir = settings.config['stock_data_dir']
    
    for prefix in ['sh.', 'sz.']:
        csv_file = f"{stock_data_dir}/{prefix}{stock_code}.csv"
        if os.path.exists(csv_file):
            data = pd.read_csv(csv_file)
            
            if data.empty:
                continue
            
            data['日期'] = pd.to_datetime(data['date'])
            data['开盘'] = pd.to_numeric(data['open'], errors='coerce').fillna(0).astype('float64')
            data['收盘'] = pd.to_numeric(data['close'], errors='coerce').fillna(0).astype('float64')
            data['最高'] = pd.to_numeric(data['high'], errors='coerce').fillna(0).astype('float64')
            data['最低'] = pd.to_numeric(data['low'], errors='coerce').fillna(0).astype('float64')
            data['成交量'] = pd.to_numeric(data['volume'], errors='coerce').fillna(0).astype('float64')
            data['成交额'] = pd.to_numeric(data['amount'], errors='coerce').fillna(0).astype('float64')
            data['换手率'] = pd.to_numeric(data['turn'], errors='coerce').fillna(0).astype('float64')
            data['涨跌幅'] = pd.to_numeric(data['pctChg'], errors='coerce').fillna(0).astype('float64')
            
            data = data[['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '换手率', '涨跌幅']]
            return data
    
    logging.warning(f"股票代码 '{stock_code}' 未找到对应的数据文件")
    return None


def fetch_hk_stock_data(stock_code):
    """获取港股数据（保留原有逻辑）"""
    import akshare as ak
    start_date = settings.config['start_date']
    data = ak.stock_hk_hist(symbol=stock_code, period="daily", start_date=start_date, adjust="qfq")
    return data


def fetch_batch(stocks_batch, a_or_hk):
    """批量获取股票数据"""
    stocks_data = {}
    
    for code_name in stocks_batch:
        stock_code = code_name[0]
        
        if a_or_hk == MARKET_A:
            data = fetch_a_stock_data(stock_code)
        elif a_or_hk == MARKET_HK:
            data = fetch_hk_stock_data(stock_code)
        else:
            logging.warning(f"未知的市场类型: {a_or_hk}")
            continue
        
        if data is not None and not data.empty:
            stocks_data[code_name] = data
        else:
            logging.debug(f"股票 {code_name} 没有数据，略过...")
            print(f"股票 {code_name} 没有数据，略过...")
    
    return stocks_data


def run(stocks, a_or_hk):
    """获取股票数据

    :param a_or_hk: "A" or "HK"
    :param stocks: tuple list, [('代码', '名称')]
    :return: dict, {('代码', '名称'), df}
    """

    stocks_data = {}
    stocks_list = list(stocks)
    
    for code_name in stocks_list:
        stock_code = code_name[0]
        
        if a_or_hk == MARKET_A:
            data = fetch_a_stock_data(stock_code)
        elif a_or_hk == MARKET_HK:
            data = fetch_hk_stock_data(stock_code)
        else:
            logging.warning(f"未知的市场类型: {a_or_hk}")
            continue
        
        if data is not None and not data.empty:
            stocks_data[code_name] = data
        else:
            logging.debug(f"股票 {code_name} 没有数据，略过...")
            print(f"股票 {code_name} 没有数据，略过...")


    return stocks_data
