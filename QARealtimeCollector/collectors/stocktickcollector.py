#
import datetime
import os
import logging
import click
import pandas as pd
from QARealtimeCollector.collectors.stockbarcollector import QARTCStockBar
from QARealtimeCollector.connector.crawl import get_formater_easy_ticks, get_formater_akshare_ticks, TICK_SOURCE

logger = logging.getLogger(__name__)


class QARTC_StockTick(QARTCStockBar):

    def __init__(self, delay=0.0, date: datetime.datetime = None, log_dir='./log', debug=False, tick_source=TICK_SOURCE.EQ_SINA):
        if delay == 0:
            if tick_source == TICK_SOURCE.EQ_SINA:
                delay = 20
            elif tick_source == TICK_SOURCE.AK_A_EM:
                delay = 60
        super().__init__(delay=delay, date=date, log_dir=log_dir, debug=debug)
        self.tick_source = tick_source

    def get_data_from_source(self):
        if self.tick_source == TICK_SOURCE.EQ_SINA:
            l1_ticks_data = get_formater_easy_ticks(self.code_list)
            l1_ticks_data = pd.DataFrame(l1_ticks_data)
        elif self.tick_source == TICK_SOURCE.AK_A_EM:
            l1_ticks_data = get_formater_akshare_ticks()
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
    # 输出结果整行显示
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    main()
