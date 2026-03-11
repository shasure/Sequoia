# -*- encoding: UTF-8 -*-
import pandas as pd

import data_fetcher
import settings
import strategy.enter as enter
from strategy import turtle_trade, climax_limitdown
from strategy import backtrace_ma250
from strategy import breakthrough_platform
from strategy import parking_apron
from strategy import low_backtrace_increase
from strategy import keep_increasing
from strategy import high_tight_flag
from strategy import my_strategies_a
import push
import logging
import time
import datetime
from const import *


def candidates_from_A():
    import os
    
    stock_list_file = f"{settings.config['stock_data_dir']}/a_share_stock_list.csv"
    stock_list = pd.read_csv(stock_list_file)
    
    stock_list = stock_list[~stock_list['code_name'].str.contains('ST', na=False)]
    
    results = []
    stock_data_dir = settings.config['stock_data_dir']
    
    for idx, row in stock_list.iterrows():
        code = row['code']
        code_name = row['code_name']
        
        csv_file = f"{stock_data_dir}/{code}.csv"
        if not os.path.exists(csv_file):
            continue
        
        try:
            data = pd.read_csv(csv_file)
            if data.empty:
                continue
            
            latest = data.iloc[-1]
            turnover = float(latest['turn']) if pd.notna(latest['turn']) else 0
            amount = float(latest['amount']) if pd.notna(latest['amount']) else 0
            
            if turnover > 0:
                market_cap_approx = amount / turnover * 100
            else:
                market_cap_approx = 0
            
            if market_cap_approx > 30 * 10 ** 8:
                results.append({
                    '代码': code.replace('sh.', '').replace('sz.', ''),
                    '名称': code_name,
                    '总市值': market_cap_approx,
                    '涨跌幅': float(latest['pctChg']) if pd.notna(latest['pctChg']) else 0
                })
        except Exception as e:
            logging.debug(f"处理股票 {code} 失败: {e}")
            continue
    
    result = pd.DataFrame(results)
    
    return result


def prepare_a():
    logging.info("************************ process start ***************************************")
    # 候选股票
    a_data = candidates_from_A()
    logging.info(f"A股候选股票数量：{len(a_data)}")
    subset = a_data[['代码', '名称']]
    stocks = [tuple(x) for x in subset.values]
    statistics(a_data, stocks)

    strategies = {
        '放量上涨': enter.check_volume,
        '均线多头': keep_increasing.check,
        '停机坪': parking_apron.check,
        '回踩年线': backtrace_ma250.check,
        # '突破平台': breakthrough_platform.check,
        '无大幅回撤': low_backtrace_increase.check,
        '海龟交易法则': turtle_trade.check_enter,
        '高而窄的旗形': high_tight_flag.check,
        '放量跌停': climax_limitdown.check,
        # my strategies a
        '放量上涨_v1': my_strategies_a.more_volume,
        '缩量回踩_v1': my_strategies_a.backtrace_lower_volume,
        '缩量回踩_v2': my_strategies_a.lower_volume,
        '均线多头排列': my_strategies_a.ma_increasing_order,  # 单独使用不好
        '均线多头排列+缩量回踩_v2': my_strategies_a.ma_increasing_order_lower_volume,  # 作用不大
        '均线聚合': my_strategies_a.ma_gathering,
        '均线聚合+放量上涨_v1': my_strategies_a.ma_gathering_more_volume,
        '均线聚合+缩量回踩_v2': my_strategies_a.ma_gathering_lower_volume,
        '持续放量': my_strategies_a.continuous_increasing_volume,
    }
    # 只加载配置文件中的策略
    if len(settings.config['strategy_a']) > 0:
        strategies = {name: strategies[name] for name in settings.config['strategy_a'] if name in strategies.keys()}
    else:
        return

    # 主要代码
    process(stocks, strategies,)

    logging.info("************************ process   end ***************************************")


def process(stocks, strategies):
    """主要代码

    :param stocks: df, [('代码', '名称')]
    :param strategies:
    :return:
    """
    stocks_data = data_fetcher.run(stocks, MARKET_A)  # 返回{'代码', df}
    # 策略循环
    for strategy, strategy_func in strategies.items():
        check(stocks_data, strategy, strategy_func, MARKET_A)
        time.sleep(2)


def write2txt(strategy, results, a_or_hk):
    # 将results写入名称为f"{strategy}_{a_or_hk}_{date}.txt"的文件中，目录在settings.config['result_dir']中，写入内容每行为results的key
    date = datetime.datetime.now().strftime('%Y%m%d')
    file_name = f"{strategy}_{a_or_hk}_{date}.txt"
    file_path = f"{settings.config['result_dir']}/{file_name}"
    with open(file_path, 'w', encoding='utf-8') as f:
        for key in results.keys():
            f.write(key[0] + '\n')


def check(stocks_data, strategy, strategy_func, a_or_hk):
    """单个策略

    :param a_or_hk:
    :param stocks_data:
    :param strategy: 名称
    :param strategy_func: 函数
    :return:
    """
    end = settings.config['end_date']
    # 检查股票在end_date前是否有数据
    m_filter = check_enter(end_date=end, strategy_fun=strategy_func)
    results = dict(filter(m_filter, stocks_data.items()))
    if len(results) > 0:
        # 写入txt，方便导入同花顺
        write2txt(strategy, results, a_or_hk)
        push.strategy('**************{}**************\n**************符合条件的有{}个**************\n{}'.format(strategy, len(results), list(results.keys())))


def check_enter(end_date=None, strategy_fun=enter.check_volume):
    def end_date_filter(stock_data):
        # 这里日期是datetime.date类型，end_date是需要转换的，不能直接对比；str to datetime
        end_date_dt = None
        if end_date is not None:
            end_date_dt = datetime.datetime.strptime(end_date, '%Y%m%d').date()
            if end_date_dt < stock_data[1].iloc[0].日期:  # 该股票在end_date时还未上市
                logging.debug("{}在{}时还未上市".format(stock_data[0], end_date))
                return False
        return strategy_fun(stock_data[0], stock_data[1], end_date=end_date_dt)

    return end_date_filter


# 统计数据
def statistics(all_data, stocks):
    limitup = len(all_data.loc[(all_data['涨跌幅'] >= 9.8)])
    limitdown = len(all_data.loc[(all_data['涨跌幅'] <= -9.8)])

    up5 = len(all_data.loc[(all_data['涨跌幅'] >= 5)])
    down5 = len(all_data.loc[(all_data['涨跌幅'] <= -5)])

    msg = "涨停数：{}   跌停数：{}\n涨幅大于5%数：{}  跌幅大于5%数：{}".format(limitup, limitdown, up5, down5)
    push.statistics(msg)