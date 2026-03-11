import baostock as bs
import pandas as pd
import os
from datetime import datetime, timedelta
from tqdm import tqdm

# --- 数据字段映射说明 ---
# 字段说明 (Daily):
# date: 日期
# code: 股票代码
# open: 开盘价
# high: 最高价
# low: 最低价
# close: 收盘价
# preclose: 前收盘价
# volume: 成交量（股）
# amount: 成交额（元）
# adjustflag: 复权状态 (1:不复权, 2:前复权, 3:后复权)
# turn: 换手率
# tradestatus: 交易状态 (1:正常交易, 0:停牌)
# pctChg: 涨跌幅 (%)
# isST: 是否ST (1:是, 0:否)

# --- 配置参数 ---
START_DATE = "2024-01-01"
END_DATE = datetime.now().strftime('%Y-%m-%d')
FREQUENCY = "d"
ADJUST_FLAG = "3"
OUTPUT_DIR = "ashare_data_csv_d"
THREADS = 1

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


def download_stock_data(code, lg):
    """增量下载单个股票数据"""
    output_path = os.path.join(OUTPUT_DIR, f"{code}.csv")
    current_start = START_DATE
    is_append = False

    if os.path.exists(output_path):
        try:
            existing_last_row = pd.read_csv(output_path, usecols=['date']).tail(1)
            if not existing_last_row.empty:
                last_date_str = existing_last_row['date'].iloc[0]
                last_date = datetime.strptime(last_date_str, '%Y-%m-%d')
                current_start = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
                is_append = True
                if current_start > END_DATE:
                    return f"Skip: {code} (Up to date)"
        except Exception as e:
            pass

    if lg.error_code != '0':
        return f"Login failed: {code}"

    rs = bs.query_history_k_data_plus(
        code,
        'date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST',
        start_date=current_start,
        end_date=END_DATE,
        frequency=FREQUENCY,
        adjustflag=ADJUST_FLAG
    )

    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())

    if data_list:
        df = pd.DataFrame(data_list,
                          columns=['date', 'code', 'open', 'high', 'low', 'close', 'preclose', 'volume', 'amount',
                                   'adjustflag', 'turn', 'tradestatus', 'pctChg', 'isST'])
        cols = [c for c in df.columns if c not in ['date', 'ßtime', 'code', 'adjustflag']]
        df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')

        if is_append:
            df.to_csv(output_path, mode='a', index=False, header=False, encoding='utf-8')
        else:
            df.to_csv(output_path, index=False, encoding='utf-8')
        status = "Success"
    else:
        status = "No new data"

    return f"{status}: {code}"


def get_all_stock_codes():
    """获取股票列表 (自动处理非交易日)"""
    global END_DATE  # 声明使用全局变量
    bs.login()
    stock_list = []
    # 如果 END_DATE 是周末或节假日，query_all_stock 会返回空
    # 我们尝试向前搜索最近 100 天，直到找到一个有数据的交易日
    search_date = datetime.strptime(END_DATE, '%Y-%m-%d')

    print(f"Searching for stock list starting from {END_DATE}...")
    for i in range(100):
        target_day = (search_date - timedelta(days=i)).strftime('%Y-%m-%d')
        END_DATE = target_day
        rs = bs.query_all_stock(day=target_day)
        while (rs.error_code == '0') & rs.next():
            stock_list.append(rs.get_row_data())

        if stock_list:
            print(f"Successfully retrieved ticker list from: {target_day}")
            break

    bs.logout()
    if not stock_list:
        return []

    df = pd.DataFrame(stock_list, columns=rs.fields)

    # 首先保存完整的股票列表到CSV，用于调试
    full_csv_path = os.path.join(OUTPUT_DIR, 'full_stock_list.csv')
    df.to_csv(full_csv_path, index=False, encoding='utf-8-sig')
    print(f"Full stock list saved to: {full_csv_path}")
    print(f"Total stocks in full list: {len(df)}")

    # 过滤条件：只保留沪深个股股票，排除指数
    df_filtered = df[~df['code'].str.match(r'^sh\.000|^sh\.5|^sz\.159|^sz\.399')]

    # 保存过滤后的股票列表到CSV
    csv_path = os.path.join(OUTPUT_DIR, 'a_share_stock_list.csv')
    df_filtered.to_csv(csv_path, index=False, encoding='utf-8-sig')
    print(f"Filtered A-share stock list saved to: {csv_path}")
    print(f"Total A-share stocks: {len(df_filtered)}")

    # 检查被过滤掉的股票代码，看是否有遗漏
    filtered_out = df[~df.index.isin(df_filtered.index)]
    print(f"\nFiltered out {len(filtered_out)} stocks. Sample of filtered out codes:")
    # print(filtered_out['code'].head(20).tolist())

    # 返回过滤后的股票代码列表
    return df_filtered['code'].tolist()


if __name__ == "__main__":
    print(f"[{datetime.now()}] Starting A-Share Sync...")

    all_codes = get_all_stock_codes()
    if not all_codes:
        print("Error: Could not retrieve stock list. Please check your internet or try again later.")
    else:
        print(f"Found {len(all_codes)} tickers to sync.")

        # 使用简单循环处理每个股票，每100次重新登录
        lg = bs.login()
        if lg.error_code != '0':
            print(f"Login failed: {lg.error_msg}")
        else:
            count = 0
            # for code in tqdm(all_codes, desc="Syncing"):
            for code in all_codes:
                result = download_stock_data(code, lg)
                count += 1
                if count % 100 == 0:
                    bs.logout()
                    lg = bs.login()
                    if lg.error_code != '0':
                        print(f"Re-login failed: {lg.error_msg}")
                        break

            # 全部操作结束后退出
            bs.logout()

    print(f"Done. Data saved in {OUTPUT_DIR}")
