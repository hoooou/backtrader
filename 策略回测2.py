from pandas import DataFrame
import pandas as pd
def coverBatchStockToDataFrame(existing_data:DataFrame):
    data={}
    for item in existing_data["close"].T.columns:
        result = pd.concat([
            existing_data["open"].loc[item], 
            existing_data["high"].loc[item],
            existing_data["low"].loc[item],
            existing_data["close"].loc[item],
            existing_data["volume"].loc[item],
            existing_data["amount"].loc[item],
            existing_data["preClose"].loc[item]], axis=1,keys=['open', 'high', 'low', 'close', 'volume', 'amount', 'preClose'])
        result["openinterest"]=0
        result.set_index(result.index,inplace=True)
        
        data[item]=result
    return data
import backtrader as bt
class CustomPandasData(bt.feeds.PandasData):
    line=("amount",)
    params=(
        ("datatime",None),
        ("open","open"),
        ("close","close"),
        ("low","low"),
        ("high","high"),
        ("volume","volume"),
        ("openinterest",-1),
        )

import backtrader as bt
import datetime
import pandas as pd


class StockSelectStrategy(bt.Strategy):
    params = dict(
        selnum=30,  # 设置持仓股数在总的股票池中的占比，如买入表现最好的前30只股票
        rperiod=1,  # 计算收益率的周期
        vperiod=6,  # 计算波动率的周期，过去6个月的波动率
        mperiod=2,  # 计算动量的周期，如过去2个月的收益
        reserve=0.05  # 5% 为了避免出现资金不足的情况，每次调仓都预留 5% 的资金不用于交易
    )
    def log(self, arg):
        print("进入log")
        print('{} {}'.format(self.datetime.date(), arg))

    def __init__(self):
        print("进入init")
        # 计算持仓权重，等权
        # self.perctarget = (1.0 - self.p.reserve) / self.p.selnum
        # # 循环计算每只股票的收益波动率因子
        # self.rs = {d:bt.ind.PctChange(d, period=self.p.rperiod) for d in self.datas}
        # self.vs = {d:1/(bt.ind.StdDev(ret, period=self.p.vperiod)+0.000001) for d,ret in self.rs.items()}
        # # 循环计算每只股票的动量因子
        # self.ms = {d:bt.ind.ROC(d, period=self.p.mperiod) for d in self.datas}
        # # 将 ep 和 roe 因子进行匹配
        # self.EP = {d:d.lines.EP for d in self.datas}
        # self.ROE = {d:d.lines.ROE for d in self.datas}
        # self.all_factors = [self.rs, self.vs, self.ms, self.EP, self.ROE]
    def stop(self):
        print(f"结束:{pd.to_datetime('now')}")
    def next(self):
        pass
        print("进入next")
        # print(f"当前数据{self.data}")

if __name__=='__main__':
    import os
    os.environ["NUMEXPR_MAX_THREADS"] = "32"
    os.environ["JULIA_NUM_THREADS"] = "32"
    # 实例化 cerebro
    print(f"开始{pd.to_datetime('now')}")
    from xtquant import xtdata
    stocks = xtdata.get_stock_list_in_sector('沪深A股')  
    existing_data = xtdata.get_market_data(
                        field_list=["open", "high", "low", "close", "volume", "amount", "preClose"],stock_list=stocks,
                        period="1d",start_time="20190101",end_time="20220101",fill_data=False,dividend_type="back"
                    )
    all_stock=coverBatchStockToDataFrame(existing_data)
    cerebro = bt.Cerebro()
    # 按股票代码，依次循环传入数据
    print(f"添加数据{pd.to_datetime('now')}")
    for index,stock in enumerate(all_stock):
        # 日期对齐
        data = pd.DataFrame(index=pd.to_datetime(existing_data["close"].columns.unique())) # 获取回测区间内所有交易日
        data_ = pd.merge(left=data, right=all_stock[stock], left_index=True, right_index=True, how='left')
        # 缺失值处理：日期对齐时会使得有些交易日的数据为空，所以需要对缺失数据进行填充
        data_.loc[:,['volume','openinterest']] = data_.loc[:,['volume','openinterest']].fillna(0)
        data_.loc[:,['open','high','low','close']] = data_.loc[:,['open','high','low','close']].fillna(method='pad')
        data_.loc[:,['open','high','low','close']] = data_.loc[:,['open','high','low','close']].fillna(0.0000001)
        # 导入数据
        datafeed = CustomPandasData(dataname=data_, 
                                fromdate=datetime.datetime(2019,1,31), 
                                todate=datetime.datetime(2021,8,31),
                                timeframe=bt.TimeFrame.Days) # 将数据的时间周期设置为月度
        cerebro.adddata(datafeed, name=stock) # 通过 name 实现数据集与股票的一一对应
    # 初始资金 100,000,000    
    cerebro.broker.setcash(100000000.0) 
    # 佣金，双边各 0.0003
    cerebro.broker.setcommission(commission=0.0003) 
    # 滑点：双边各 0.0001
    cerebro.broker.set_slippage_perc(perc=0.0001) 
    # 将编写的策略添加给大脑，别忘了 ！
    cerebro.addstrategy(StockSelectStrategy)
    # 返回收益率时序
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='_TimeReturn')
    result = cerebro.run(maxcpus=32)
    # 得到收益率时序
    ret = pd.Series(result[0].analyzers._TimeReturn.get_analysis())
    print(f"结束:{pd.to_datetime('now')}")
    #########  注意  #########
    # PyFolio 分析器返回的收益也是月度收益，但是绘制的各种收益分析图形会有问题，有些图绘制不出来
