#
import datetime
import os
import logging
import click
import pandas as pd
from QARealtimeCollector.collectors.stockbarcollector import QARTCStockBar

logger = logging.getLogger(__name__)

# 输出结果整行显示
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class TICK_SOURCE():
    """数据来源类型
    """
    EQ_SINA = 1
    AK_A_EM = 2


class QARTC_StockTick(QARTCStockBar):

    def __init__(self, delay=0, date: datetime.datetime = None, log_dir='./log', debug=False, tick_source=TICK_SOURCE.EQ_SINA):
        if delay == 0:
            if tick_source == TICK_SOURCE.EQ_SINA:
                delay = 20
            elif tick_source == TICK_SOURCE.AK_A_EM:
                delay = 60
        super().__init__(delay=delay, date=date, log_dir=log_dir, debug=debug)
        self.tick_source = tick_source

    def get_formater_easy_ticks(self) -> pd.DataFrame:
        """
        下载处理 easyquotation ticks 数据
        """
        import easyquotation
        quotation = easyquotation.use('sina')
        l1_ticks = quotation.stocks(self.code_list)
        logger.info("原始数据获取成功. count %s" % len(l1_ticks))
        tick_list = []
        for code, tick in l1_ticks.items():
            dic = {'code': code, 'datetime': '{} {}'.format(tick['date'], tick['time']), 'price': tick['now'],
                   'open': tick['open'], 'close': tick['close'], 'high': tick['high'], 'low': tick['low'],
                   'volume': tick['volume'], 'name': tick['name']}
            tick_list.append(dic)
        df_ticks = pd.DataFrame(tick_list)
        return df_ticks

    def get_formater_akshare_ticks(self) -> pd.DataFrame:
        """
        处理 akshare ticks 数据
        """
        import akshare as ak
        l1_ticks = ak.stock_zh_a_spot()
        # l1_ticks = ak.stock_zh_a_spot_em()
        logger.info("原始数据获取成功. count %s" % len(l1_ticks))
        df_ticks = pd.DataFrame()
        df_ticks['code'] = l1_ticks['代码']
        df_ticks['name'] = l1_ticks['名称']
        df_ticks['price'] = l1_ticks['最新价']
        df_ticks['high'] = l1_ticks['最高']
        df_ticks['low'] = l1_ticks['最低']
        df_ticks['open'] = l1_ticks['今开']
        df_ticks['close'] = l1_ticks['昨收']
        df_ticks['volume'] = l1_ticks['成交量']
        cur_time = datetime.datetime.now()
        df_ticks['datetime'] = cur_time.strftime("%Y-%m-%d %H:%M:%S")
        return df_ticks

    def get_data_from_source(self):
        if self.tick_source == TICK_SOURCE.EQ_SINA:
            l1_ticks_data = self.get_formater_easy_ticks()
        elif self.tick_source == TICK_SOURCE.AK_A_EM:
            l1_ticks_data = self.get_formater_akshare_ticks()
        else:
            l1_ticks_data = pd.DataFrame()
        return l1_ticks_data

    def get_security_bar_concurrent(self, code_list, _type, lens):
        try:
            context = self.get_data_from_source()
            return [context]
        except:
            raise Exception


@click.command()
# @click.argument()
@click.option('-t', '--delay', default=0, help="fetch data interval, float", type=click.FLOAT)
@click.option('-log', '--logfile', help="log file path", type=click.Path(exists=False))
@click.option('-log_dir', '--log_dir', help="log path", type=click.Path(exists=False))
def main(delay: float = 0, logfile: str = None, log_dir: str = None):
    try:
        from QARealtimeCollector.utils.logconf import update_log_file_config
        logfile = 'stock.collector.log' if logfile is None else logfile
        log_dir = '' if log_dir is None else log_dir
        logging.config.dictConfig(update_log_file_config(logfile))
    except Exception as e:
        print(e.__str__())
    QARTC_StockTick(delay=delay, log_dir=log_dir.replace('~', os.path.expanduser('~')), debug=True).start()


if __name__ == "__main__":
    main()
