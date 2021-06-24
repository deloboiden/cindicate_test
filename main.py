import websocket
import json
import traceback
from datetime import datetime

DATA_STREAM_ENDPOINT = 'wss://stream.binance.com:9443/ws'
BTC_USDT_KLINE = "/btcusdt@kline_1m"
ETH_USDT_KLINE = "/ethusdt@kline_1m"
BNB_BTC_KLINE = "/bnbbtc@kline_1m"
LOGGING_RATE_CODES = {'BTCUSDT': [], 'ETHUSDT': [], 'BNBBTC': []}
FILTER_WINDOW_WIDTH = 7  # must be > 0
LOG_FILE_PATH = './logfile.csv'
ERROR_LOG_FILE_PATH = './error.log'

def on_message(ws, message):
    def calculate(data_dict, data_list):
        data_list.append(float(data_dict['k']['c']))
        if len(data_list) > FILTER_WINDOW_WIDTH:
            data_list.pop(0)
        return sum(data_list) / len(data_list)

    try:
        messageDict = json.loads(message)
        if messageDict['k']['x']:
            rate_code = messageDict['s']
            if rate_code in LOGGING_RATE_CODES:
                result = calculate(messageDict, LOGGING_RATE_CODES[rate_code])
                with open(LOG_FILE_PATH, 'a') as file:
                    file.write("{}; {}; {}\n".format(rate_code,
                                                     datetime.utcfromtimestamp(messageDict['k']['T'] / 1000).isoformat(),
                                                     result))
    except Exception as e:
        with open(ERROR_LOG_FILE_PATH, 'a') as file:
            traceback.print_exc(file=file)


def on_error(ws, error):
    with open(ERROR_LOG_FILE_PATH, 'a') as file:
        file.write(error)


def on_close(ws):
    print("closed")


def on_open(ws):
    with open(LOG_FILE_PATH, 'w') as file:
        file.write("rate_code; datetime; cost\n")
    with open(ERROR_LOG_FILE_PATH, 'w'):
        print("connected")


if __name__ == "__main__":
    ws = websocket.WebSocketApp(DATA_STREAM_ENDPOINT + BTC_USDT_KLINE + ETH_USDT_KLINE + BNB_BTC_KLINE,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.run_forever()
