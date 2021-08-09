import datetime
import pandas as pd


class TICK_SOURCE():
    """数据来源类型
    """
    EQ_SINA = 1
    AK_A_EM = 2


def get_formater_easy_ticks(code_list) -> list:
    """
    下载处理 easyquotation ticks 数据
    """
    import easyquotation
    quotation = easyquotation.use('sina')
    l1_ticks = quotation.stocks(code_list)
    tick_list = []
    for code, tick in l1_ticks.items():
        dic = {'datetime': '{} {}'.format(tick['date'], tick['time']), 'code': code, 'date': tick['date'],
               'time': tick['time'],
               'open': tick['open'], 'high': tick['high'], 'close': tick['close'], 'low': tick['low'],
               'price': tick['now'],
               'vol': tick['volume'], 'amount': 0,
               'year': int(tick['date'][:4]), 'month': int(tick['date'][5:7]), 'day': int(tick['date'][-2:]),
               'hour': int(tick['time'][:2]), 'minute': int(tick['time'][3:5])
               }
        tick_list.append(dic)
    return tick_list


def get_formater_akshare_ticks() -> pd.DataFrame:
    """
    下载处理 akshare ticks 数据
    """
    import akshare as ak
    # l1_ticks = ak.stock_zh_a_spot()
    l1_ticks = ak.stock_zh_a_spot_em()
    df_ticks = pd.DataFrame()
    df_ticks['code'] = l1_ticks['代码']
    df_ticks['name'] = l1_ticks['名称']
    df_ticks['price'] = l1_ticks['最新价']
    df_ticks['high'] = l1_ticks['最高']
    df_ticks['low'] = l1_ticks['最低']
    df_ticks['open'] = l1_ticks['今开']
    df_ticks['close'] = l1_ticks['昨收']
    df_ticks['vol'] = l1_ticks['成交量']
    cur_time = datetime.datetime.now()
    df_ticks['datetime'] = cur_time.strftime("%Y-%m-%d %H:%M:%S")
    return df_ticks
