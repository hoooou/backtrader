import pandas as pd
from xtquant import xtdata
import pandas as pd
def dictToDataFrame(data):
    # print(f"原始数据:{data}")
    mm=pd.DataFrame()
    if(data["close"].empty):
        return mm
    for item in data:
        mm[item]=data[item].T
    mm.index=pd.to_datetime(arg=mm.index)
    # print(f"转换后{mm.index}")
    return mm
from pandas import DataFrame
import pandas as pd

def coverBatchStockToDataFrame(fromData):
    data={}
    for item in fromData["close"].T.columns:
        result = pd.concat([
            fromData["open"].loc[item], 
            fromData["high"].loc[item],
            fromData["close"].loc[item],
            fromData["volume"].loc[item],
            fromData["amount"].loc[item],
            fromData["preClose"].loc[item]], axis=1,keys=["open", "high", "low", "close", "volume", "amount", "preClose"])
        data[item]=result
    return data

def getBatchStock(stock_list, period, start_time, end_time, fill_data=False,dividend_type=""):
    # 必须下载后才能调用,能快很多
    existing_data = xtdata.get_market_data(
                    field_list=["open", "high", "low", "close", "volume", "amount", "preClose"],stock_list=stock_list,
                    period=period,start_time=start_time,end_time=end_time,fill_data=fill_data,dividend_type="none"
                )
    return coverBatchStockToDataFrame(existing_data)

def getStockDataAndDownload(stock_list, period, start_time, end_time, fill_data=False,forceDownLoad=False,dividend_type=""):
    """
    获取股票数据，按时间周期进行分批下载：
        - 1m 和 5m 数据按 1 年（365 天）为单位分批下载。
        - 1d 数据按 5 年（1825 天）为单位分批下载。

    :param stock_list: 股票代码列表
    :param period: 时间周期（1m, 5m, 1d）
    :param start_time: 开始时间，格式为 'YYYYMMDD'
    :param end_time: 结束时间，格式为 'YYYYMMDD'
    :return: dict，包含每只股票数据的字典 {stock_code: DataFrame}
    :param dividend_type: 除权类型"none" "front" "back" "front_ratio" "back_ratio"

    """
    # 将时间格式转换为 pandas.Timestamp
    start = pd.to_datetime(start_time, format='%Y%m%d')
    if end_time=="":
        end=pd.to_datetime(arg="now")
    else:
        end = pd.to_datetime(end_time, format='%Y%m%d')

    # 根据周期确定每批次的时间跨度
    max_days = 365 if period in ["1m", "5m"] else 365*5  # 1 年 or 5 年
    all_stock_data = {}

    # 遍历所有股票
    for index,stock in enumerate(stock_list):
        current_start = start
        stock_data = pd.DataFrame()  # 存储当前股票的所有数据
        print(f"下载进度{index}/{len(stock_list)}")
        # 遍历时间段
        while current_start < end:
            current_end = min(current_start + pd.Timedelta(days=max_days), end)

            # 检查数据是否已存在
            existing_data = xtdata.get_market_data(
                    field_list=["open", "high", "low", "close", "volume", "amount", "preClose"],
                    stock_list=[stock],
                    period=period,
                    dividend_type=dividend_type,
                    start_time=current_start.strftime('%Y%m%d'),
                    end_time=current_end.strftime('%Y%m%d'),
                    fill_data=fill_data
                )

            if not forceDownLoad and existing_data and not existing_data["close"].empty:
                print(f"{stock}: {current_start.strftime('%Y%m%d')} 到 {current_end.strftime('%Y%m%d')} 数据已存在，无需下载。")
                # 合并已有数据
                stock_data = pd.concat([stock_data, dictToDataFrame(existing_data)])
            else:
                print(f"下载缺失数据 {stock}: {current_start.strftime('%Y%m%d')} 到 {current_end.strftime('%Y%m%d')}")
                xtdata.download_history_data(
                    stock_code=stock,
                    period=period,
                    start_time=current_start.strftime('%Y%m%d'),
                    end_time=current_end.strftime('%Y%m%d'),
                    incrementally=True
                )
                # 下载后再次获取数据
                downloaded_data = xtdata.get_market_data(
                    field_list=["open", "high", "low", "close", "volume", "amount", "preClose"],
                    stock_list=[stock],
                    period=period,
                    dividend_type=dividend_type,
                    start_time=current_start.strftime('%Y%m%d'),
                    end_time=current_end.strftime('%Y%m%d'),
                    fill_data=fill_data
                )

                if downloaded_data and not downloaded_data["close"].empty:
                    stock_data = pd.concat([stock_data, dictToDataFrame(downloaded_data)])

            # 更新当前开始时间
            current_start = current_end + pd.Timedelta(days=1)

        # 存储该股票的所有数据
        all_stock_data[stock] = stock_data

    return all_stock_data
