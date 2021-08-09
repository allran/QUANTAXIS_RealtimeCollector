##关于股票行情数据的使用
> 标准化订阅股票行情数据流程:
- 1.安装库
```sybase
$ git clone https://github.com/allran/QUANTAXIS_RealtimeCollector.git
$ cd QUANTAXIS_RealtimeCollector
$ pip uninstall qarealtime-collector
$ pip install -e .
$ pip install easyquotation
$ pip install akshare
```
注意：此方法为本地源码安装, 由于改了天神的库代码，所以不能直接pip安装。

- 2.获取实时行情bar数据
```python
from QARealtimeCollector.collectors.stocktickcollector import QARTC_StockTick
import threading
log_dir = './logs'  #默认log数据目录
second = 60  #60秒取一次
isDebug = True  #正式环境为false
s = QARTC_StockTick(log_dir=log_dir, debug=isDebug)
threading.Thread(target=s.start).start()
```

- 3.订阅行情
```python
import json
from QAPUBSUB.producer import publisher_routing
code = '000039' #需要订阅行情的股票code
action = 'subscribe' #订阅行情(unsubscribe为取消订阅行情)
publisher_routing(exchange='QARealtime_Market', routing_key='stock').pub(json.dumps({
    'topic': 'subscribe',
    'code': code
}), routing_key='stock')
```
注意：先运行第2步后，然后订阅行情，才有bar数据输出
- 4.监听行情实时数据
```python
import json
from QAPUBSUB import consumer
import threading

def on_data(a, b, c, data):
    parsed = json.loads(data)
    ss = json.dumps(parsed, indent=4)
    print('监听到bar数据：', ss)

c = consumer.subscriber(exchange='realtime_stock_min')
c.callback = on_data
threading.Thread(target=c.start).start()
```

- 5.重采样对应分钟线数据
```python
from QARealtimeCollector.datahandler.stock_resampler import QARTCStockBarResampler
import threading
log_dir = './logs'  # logs数据目录
frequency = '1min'  # 频率
isDebug = True
s = QARTCStockBarResampler(frequency=frequency, log_dir=log_dir)
threading.Thread(target=s.start).start()
```

#### 注意：通过代码方式能更好的让人理解流程逻辑。但以上2345步骤均可使用命令行操作，具体详查QA文档。
