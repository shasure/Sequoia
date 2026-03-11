# -*- encoding: UTF-8 -*-
from operator import truediv

import numpy as np
import talib as tl
import pandas as pd
import logging


def market_maker(rdf, threshold=60):
    """check是不是庄股：简单通过成交量的中位数对应的成交额来判断

    :param threshold:
    :param df: 个股dataframe
    :return:
    """
    MIN_AMOUNT = 2.5 * 10 ** 8  # 最少成交额
    # 减少误判
    if len(rdf) < threshold:
        return False
    df = rdf.copy(deep=True).tail(threshold)
    df = df[["成交量", "收盘", "成交额"]]
    df = df.sort_values('成交量', ascending=False)
    mid_vol_amount = df.iloc[len(df) // 2]["成交额"]
    if mid_vol_amount < MIN_AMOUNT:
        return True
    return False


def more_volume(code_name, data, end_date=None, threshold=60):
    """放量上涨；特殊情况是涨停，会根据代码判断涨幅

    :param code_name:
    :param data:
    :param end_date:
    :param threshold:
    :return:
    """
    # hyper params
    MIN_UP_RATIO = 4  # 最小上涨
    MIN_TOTAL_TRADE_VALUE = 5 * 10 ** 8
    MIN_VOL_RATIO = 2
    MIN_MA5_VOL_RATIO = 1.7

    if len(data) < 250:
        logging.debug("{0}:样本小于250天...\n".format(code_name))
        return False

    data['vol_ma5'] = pd.Series(tl.MA(data['成交量'].values, 5), index=data.index.values)

    if end_date is not None:
        mask = (data['日期'] <= end_date)
        data = data.loc[mask].copy()  # 显式创建副本
    if data.empty:
        return False

    # 涨停是特殊的，直接算放量
    if data.iloc[-1]['涨跌幅'] >= get_limit_percent(code_name):
        return True

    p_change = data.iloc[-1]['涨跌幅']
    # 上涨最少4%
    if p_change < MIN_UP_RATIO:
        return False

    # 最后一天成交量
    last_vol = data.iloc[-1]['成交量']
    # 最后一天成交额
    last_val = data.iloc[-1]['成交额']
    second_last_vol = data.iloc[-2]['成交量']

    # 成交额不低于5亿
    if last_val < MIN_TOTAL_TRADE_VALUE:
        return False

    mean_vol = data.iloc[-2]['vol_ma5']  # -2表示计算平均量能时不算今天

    vol_ratio = last_vol / second_last_vol
    mean_vol_ratio = last_vol / mean_vol

    if vol_ratio >= MIN_VOL_RATIO and not market_maker(data, threshold):  # 2倍
        vol_ratio = "*{0}\n对比上一天放量：{1:.2f}\t涨幅：{2}%\n".format(code_name, vol_ratio, p_change)
        logging.debug(vol_ratio)
        return True
    elif mean_vol_ratio >= MIN_MA5_VOL_RATIO and not market_maker(data, threshold):  # 1.7倍
        msg_vol_ma5 = "*{0}\n对比vol_ma5放量：{1:.2f}\t涨幅：{2}%\n".format(code_name, mean_vol_ratio, p_change)
        logging.debug(msg_vol_ma5)
        return True
    else:
        return False


def lower_volume(code_name, data, end_date=None, threshold=60):
    """简单定义缩量，不考虑放量时间；收盘价不能偏离ma5太多

    :param code_name:
    :param data:
    :param end_date:
    :param threshold:
    :return:
    """
    # hyper params
    MIN_UP_RATIO = 4  # 最小上涨
    MIN_TOTAL_TRADE_VALUE = 3 * 10 ** 8
    MAX_VOL_RATIO = 0.7  # 缩量要求
    MAX_CLOSE_MA_RATIO = 0.03  # 收盘价与MA的最大偏离比例

    if len(data) < 250:
        logging.debug("{0}:样本小于250天...\n".format(code_name))
        return False

    if end_date is not None:
        mask = (data['日期'] <= end_date)
        data = data.loc[mask].copy()  # 显式创建副本
    if data.empty:
        return False

    last_val = data.iloc[-1]['成交额']

    # 成交额不低于3亿
    if last_val < MIN_TOTAL_TRADE_VALUE:
        return False

    data['ma5'] = pd.Series(tl.MA(data['收盘'].values, 5), index=data.index.values)
    data['ma10'] = pd.Series(tl.MA(data['收盘'].values, 10), index=data.index.values)
    data['ma20'] = pd.Series(tl.MA(data['收盘'].values, 20), index=data.index.values)
    data['vol_ma5'] = pd.Series(tl.MA(data['成交量'].values, 5), index=data.index.values)

    # 计算当前成交量与前一日成交量的比值
    vol_ratio_1 = data.iloc[-1]['成交量'] / data.iloc[-2]['成交量']
    vol_ratio_2 = data.iloc[-1]['成交量'] / data.iloc[-2]['vol_ma5']

    if vol_ratio_1 < MAX_VOL_RATIO or vol_ratio_2 < MAX_VOL_RATIO:  # 缩量
        # 判断收盘价是否偏离ma均线太多
        # 检查所有均线是否聚集
        ma_list = [data.iloc[-1]["ma5"], data.iloc[-1]["ma10"], data.iloc[-1]["ma20"]]
        found_match = False
        for idx, ma in enumerate(ma_list):
            if abs((data.iloc[-1]['收盘'] - ma) / ma) < MAX_CLOSE_MA_RATIO:
                found_match = True
                break
        if not found_match:
            return False
    else:
        return False

    if not market_maker(data, threshold):
        return True
    return False


def backtrace_lower_volume(code_name, data, end_date=None, threshold=60):
    # hyper params
    MIN_UP_RATIO = 4  # 最小上涨
    MIN_TOTAL_TRADE_VALUE = 3 * 10 ** 8
    MIN_VOL_RATIO = 2  # 放量要求
    MIN_VOL_BACK_RATIO = 1.6  # 缩量要求

    if len(data) < 250:
        logging.debug("{0}:样本小于250天...\n".format(code_name))
        return False

    if end_date is not None:
        mask = (data['日期'] <= end_date)
        data = data.loc[mask].copy()  # 显式创建副本
    if data.empty:
        return False

    # 最后一天成交额
    last_val = data.iloc[-1]['成交额']

    # 成交额不低于3亿
    if last_val < MIN_TOTAL_TRADE_VALUE:
        return False

    data['vol_ma2'] = pd.Series(tl.MA(data['成交量'].values, 2), index=data.index.values)
    data['vol_ma3'] = pd.Series(tl.MA(data['成交量'].values, 3), index=data.index.values)
    data['vol_ma4'] = pd.Series(tl.MA(data['成交量'].values, 4), index=data.index.values)
    data['vol_ma5'] = pd.Series(tl.MA(data['成交量'].values, 5), index=data.index.values)

    flag = False
    if data.iloc[-6]['涨跌幅'] > MIN_UP_RATIO and (data.iloc[-6]['收盘'] + data.iloc[-6][
        '开盘']) / 2 < data.iloc[-1]['收盘'] and data.iloc[-6]['成交量'] / data.iloc[-7]['vol_ma5'] > MIN_VOL_RATIO and \
            data.iloc[-6]['成交量'] / data.iloc[-1]['vol_ma5'] > MIN_VOL_BACK_RATIO:
        flag = True
    if not flag and data.iloc[-5]['涨跌幅'] > MIN_UP_RATIO and (data.iloc[-5]['收盘'] + data.iloc[-5][
        '开盘']) / 2 < data.iloc[-1]['收盘'] and data.iloc[-5]['成交量'] / data.iloc[-6]['vol_ma5'] > MIN_VOL_RATIO and \
            data.iloc[-5]['成交量'] / data.iloc[-1]['vol_ma4'] > MIN_VOL_BACK_RATIO:
        flag = True
    if not flag and data.iloc[-4]['涨跌幅'] > MIN_UP_RATIO and data.iloc[-4]['开盘'] < data.iloc[-1]['收盘'] and \
            data.iloc[-4]['成交量'] / data.iloc[-5]['vol_ma5'] > MIN_VOL_RATIO and data.iloc[-4]['成交量'] > \
            data.iloc[-1]['vol_ma3']:
        flag = True
    if not flag and data.iloc[-3]['涨跌幅'] > MIN_UP_RATIO and data.iloc[-3]['开盘'] < data.iloc[-1]['收盘'] and \
            data.iloc[-3]['成交量'] / data.iloc[-4]['vol_ma5'] > MIN_VOL_RATIO and data.iloc[-3]['成交量'] > \
            data.iloc[-1]['vol_ma2']:
        flag = True

    if flag and not market_maker(data, threshold):
        return True

    return False


def continuous_increasing_volume(code_name, data, end_date=None, threshold=60):
    """
    判断股票是否持续放量，根据庄票设置阈值

    :param code_name: 股票代码
    :param data: 包含股票数据的DataFrame
    :param end_date: 截止日期，默认为None
    :param threshold: 阈值，默认为60
    :return: 如果满足持续放量条件返回True，否则返回False
    """
    MIN_VOL_RATIO = 2
    MIN_VOL_RATIO_MM = 4

    if len(data) < 250:
        logging.debug("{0}:样本小于250天...\n".format(code_name))
        return False

    if end_date is not None:
        mask = (data['日期'] <= end_date)
        data = data.loc[mask].copy()  # 显式创建副本
    if data.empty:
        return False

    # 计算成交量的5日均线
    data['vol_ma5'] = pd.Series(tl.MA(data['成交量'].values, 5), index=data.index.values)

    # 根据市值判断
    with np.errstate(divide='ignore'):
        if market_maker(data, threshold):
            if data.iloc[-1]["vol_ma5"] / data.iloc[-5]["vol_ma5"] > MIN_VOL_RATIO_MM:
                return True
        else:
            if data.iloc[-1]["vol_ma5"] / data.iloc[-5]["vol_ma5"] > MIN_VOL_RATIO:
                return True

    return False


def ma_increasing_order(code_name, data, end_date=None, threshold=60):
    # hyper params
    MIN_TOTAL_TRADE_VALUE = 5 * 10 ** 8

    if len(data) < 250:
        logging.debug("{0}:样本小于250天...\n".format(code_name))
        return False

    if end_date is not None:
        mask = (data['日期'] <= end_date)
        data = data.loc[mask].copy()  # 显式创建副本
    if data.empty:
        return False

    # 最后一天成交额
    last_val = data.iloc[-1]['成交额']

    # 成交额不低于3亿
    if last_val < MIN_TOTAL_TRADE_VALUE:
        return False

    data['ma5'] = pd.Series(tl.MA(data['收盘'].values, 5), index=data.index.values)
    data['ma10'] = pd.Series(tl.MA(data['收盘'].values, 10), index=data.index.values)
    data['ma20'] = pd.Series(tl.MA(data['收盘'].values, 20), index=data.index.values)
    data['ma30'] = pd.Series(tl.MA(data['收盘'].values, 30), index=data.index.values)
    data['ma60'] = pd.Series(tl.MA(data['收盘'].values, 60), index=data.index.values)
    data['ma120'] = pd.Series(tl.MA(data['收盘'].values, 120), index=data.index.values)

    last_row = data.iloc[-1]
    ma_list = [last_row["ma5"], last_row["ma10"], last_row["ma20"], last_row["ma30"], last_row["ma60"],
               last_row["ma120"]]
    for idx in range(len(ma_list) - 1):
        if ma_list[idx] < ma_list[idx + 1]:
            return False

    if not market_maker(data, threshold):
        return True

    return False


def ma_increasing_order_lower_volume(code_name, data, end_date=None, threshold=60):
    if ma_increasing_order(code_name, data, end_date=end_date, threshold=threshold):
        if lower_volume(code_name, data, end_date=end_date, threshold=threshold):
            return True
    return False


def ma_gathering(code_name, data, end_date=None, threshold=60):
    if len(data) < 250:
        logging.debug("{0}:样本小于250天...\n".format(code_name))
        return False

    if end_date is not None:
        mask = (data['日期'] <= end_date)
        data = data.loc[mask].copy()  # 显式创建副本
    if data.empty:
        return False

    # 换手率，默认单位是百分比
    if data.iloc[-1]['换手率'] < 2:
        return False

    data['ma5'] = pd.Series(tl.MA(data['收盘'].values, 5), index=data.index.values)
    data['ma10'] = pd.Series(tl.MA(data['收盘'].values, 10), index=data.index.values)
    data['ma20'] = pd.Series(tl.MA(data['收盘'].values, 20), index=data.index.values)
    data['ma30'] = pd.Series(tl.MA(data['收盘'].values, 30), index=data.index.values)

    # 检查所有均线是否聚集
    ma_list = [data.iloc[-1]["ma5"], data.iloc[-1]["ma10"], data.iloc[-1]["ma20"], data.iloc[-1]["ma30"]]

    for idx, ma in enumerate(ma_list):
        found_match = False
        gap = 0.03
        if idx == 3:  # ma30放宽要求
            gap = 0.05
        for other_idx, other_ma in enumerate(ma_list):
            if idx != other_idx and abs((ma - other_ma) / other_ma) < gap:
                found_match = True
                break
        if not found_match:
            return False

    if not market_maker(data, threshold):
        return True

    return False


def ma_gathering_more_volume(code_name, data, end_date=None, threshold=60):
    if ma_gathering(code_name, data, end_date=end_date, threshold=threshold):
        if more_volume(code_name, data, end_date=end_date, threshold=threshold):
            return True
    return False


def ma_gathering_lower_volume(code_name, data, end_date=None, threshold=60):
    if ma_gathering(code_name, data, end_date=end_date, threshold=threshold):
        if lower_volume(code_name, data, end_date=end_date, threshold=threshold):
            return True
    return False


def get_limit_percent(code_name):
    """
    根据A股股票代码判断涨跌幅限制（10%、20%、30% 或 ST规则）
    """
    # 清理非数字字符并补零至6位（如"600" -> "000600"）
    code = code_name[0].zfill(6)
    prefix_3 = code[:3]
    prefix_2 = code[:2]
    first_char = code[0]

    # 按优先级判断板块
    if prefix_3 == "688" or prefix_3 == "689":  # 科创板（688开头）
        return 19.8
    elif prefix_2 == "30":  # 创业板（30开头）
        return 19.8
    elif first_char == "8" or prefix_2 == "43":  # 北交所（8或43开头）
        return 29.8
    elif prefix_2 in ["60", "00"]:  # 沪市主板（60开头）或深市主板（00开头）
        return 9.8
    else:
        print("代码判断失败：", code_name)
        return 0
