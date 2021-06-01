import pytest
from main import *

test_dicts_1 = [
'{"e": "kline", "E": 1622536620071, "s": "ETHUSDT", "k": {"t": 1622536560000, "T": 1622536619999, "s": "ETHUSDT", "i": "1m", "f": 465978618, "L": 465980749, "o": "2573.49000000", "c": "2581.15000000", "h": "2581.99000000", "l": "2571.75000000", "v": "1801.53120000", "n": 2132, "x": true, "q": "4642076.33345860", "V": "966.47819000", "Q": "2489754.61224490", "B": "0"}}',
'{"e": "kline", "E": 1622536680076, "s": "ETHUSDT", "k": {"t": 1622536620000, "T": 1622536679999, "s": "ETHUSDT", "i": "1m", "f": 465980750, "L": 465982024, "o": "2580.35000000", "c": "2577.24000000", "h": "2580.92000000", "l": "2574.65000000", "v": "665.91042000", "n": 1275, "x": true, "q": "1716628.71039570", "V": "274.83507000", "Q": "708421.70136850", "B": "0"}}',
'{"e": "kline", "E": 1622536740065, "s": "ETHUSDT", "k": {"t": 1622536680000, "T": 1622536739999, "s": "ETHUSDT", "i": "1m", "f": 465982025, "L": 465983280, "o": "2577.31000000", "c": "2574.28000000", "h": "2577.71000000", "l": "2571.92000000", "v": "484.37835000", "n": 1256, "x": true, "q": "1247010.69461570", "V": "202.76727000", "Q": "521956.72274290", "B": "0"}}',
'{"e": "kline", "E": 1622536800093, "s": "ETHUSDT", "k": {"t": 1622536740000, "T": 1622536799999, "s": "ETHUSDT", "i": "1m", "f": 465983281, "L": 465984625, "o": "2574.29000000", "c": "2578.30000000", "h": "2580.34000000", "l": "2573.19000000", "v": "580.94193000", "n": 1345, "x": true, "q": "1497714.53775290", "V": "367.01762000", "Q": "946260.06877200", "B": "0"}}',
'{"e": "kline", "E": 1622536860103, "s": "ETHUSDT", "k": {"t": 1622536800000, "T": 1622536859999, "s": "ETHUSDT", "i": "1m", "f": 465984626, "L": 465985981, "o": "2578.63000000", "c": "2580.55000000", "h": "2583.88000000", "l": "2578.15000000", "v": "1234.38327000", "n": 1356, "x": true, "q": "3185583.92572040", "V": "581.09675000", "Q": "1499395.59707100", "B": "0"}}',
'{"e": "kline", "E": 1622537040008, "s": "ETHUSDT", "k": {"t": 1622536980000, "T": 1622537039999, "s": "ETHUSDT", "i": "1m", "f": 465988680, "L": 465992924, "o": "2576.32000000", "c": "2558.84000000", "h": "2576.33000000", "l": "2556.27000000", "v": "2597.76810000", "n": 4245, "x": true, "q": "6665601.43707320", "V": "528.89918000", "Q": "1357818.38857110", "B": "0"}}'
]

output_1 = """rate_code; datetime; cost
ETHUSDT; 2021-06-01T08:36:59.999000; 2581.15
ETHUSDT; 2021-06-01T08:37:59.999000; 2579.1949999999997
ETHUSDT; 2021-06-01T08:38:59.999000; 2577.556666666667
ETHUSDT; 2021-06-01T08:39:59.999000; 2577.7425000000003
ETHUSDT; 2021-06-01T08:40:59.999000; 2578.304
ETHUSDT; 2021-06-01T08:43:59.999000; 2575.06
"""

test_dicts_2 = [
'{"e": "kline", "E": 1622536740465, "s": "BTCUS", "k": {"t": 1622536680000, "T": 1622536739999, "s": "BTCUSDT", "i": "1m", "f": 885315807, "L": 885317261, "o": "36140.74000000", "c": "36134.36000000", "h": "36142.52000000", "l": "36088.32000000", "v": "54.51976000", "n": 1455, "x": true, "q": "1969233.53905531", "V": "29.75382500", "Q": "1074750.70372171", "B": "0"}}',
'{"e": "kline", "E": 1622536800088, "s": "BTCUSDT", "k": {"t": 1622536740000, "T": 1622536799999, "s": "BTCUSDT", "i": "1m", "f": 885317262, "L": 885318938, "o": "36130.12000000", "c": "36180.27000000", "h": "36190.00000000", "l": "36120.22000000", "v": "31.09581400", "n": 1677, "x": true, "q": "1124740.44679234", "V": "13.26877500", "Q": "479943.17778058", "B": "0"}}',
'{"e": "kline", "E": 1622536860077, "s": "CUSDT", "k": {"t": 1622536800000, "T": 1622536859999, "s": "BTCUSDT", "i": "1m", "f": 885318939, "L": 885320202, "o": "36177.80000000", "c": "36196.63000000", "h": "36212.34000000", "l": "36173.00000000", "v": "56.97039000", "n": 1264, "x": true, "q": "2061942.81227405", "V": "34.44230100", "Q": "1246513.19668876", "B": "0"}}',
'{"e": "kline", "E": 1622537040053, "s": "BTCUSDT", "k": {"t": 1622536980000, "T": 1622537039999, "s": "BTCUSDT", "i": "1m", "f": 885323519, "L": 885326096, "o": "36124.50000000", "c": "36029.60000000", "h": "36127.44000000", "l": "36026.27000000", "v": "126.74777500", "n": 2578, "x": true, "q": "4572233.23511828", "V": "54.12827800", "Q": "1952337.79857981", "B": "0"}}'
]

output_2 = """rate_code; datetime; cost
BTCUSDT; 2021-06-01T08:39:59.999000; 7375.8042857142855
BTCUSDT; 2021-06-01T08:43:59.999000; 12154.154285714283
"""

test_dicts_3 = [  # error while parsing in first line
'{"e" "kline", "E": 1622536740465, "s": "BTCUSDT", "k": {"t": 1622536680000, "T": 1622536739999, "s": "BTCUSDT", "i": "1m", "f": 885315807, "L": 885317261, "o": "36140.74000000", "c": "36134.36000000", "h": "36142.52000000", "l": "36088.32000000", "v": "54.51976000", "n": 1455, "x": true, "q": "1969233.53905531", "V": "29.75382500", "Q": "1074750.70372171", "B": "0"}}',
'{"e": "kline", "E": 1622536800088, "s": "BTCUSDT", "k": {"t": 1622536740000, "T": 1622536799999, "s": "BTCUSDT", "i": "1m", "f": 885317262, "L": 885318938, "o": "36130.12000000", "c": "36180.27000000", "h": "36190.00000000", "l": "36120.22000000", "v": "31.09581400", "n": 1677, "x": true, "q": "1124740.44679234", "V": "13.26877500", "Q": "479943.17778058", "B": "0"}}',
'{"e": "kline", "E": 1622536860077, "s": "BTCUSDT", "k": {"t": 1622536800000, "T": 1622536859999, "s": "BTCUSDT", "i": "1m", "f": 885318939, "L": 885320202, "o": "36177.80000000", "c": "36196.63000000", "h": "36212.34000000", "l": "36173.00000000", "v": "56.97039000", "n": 1264, "x": true, "q": "2061942.81227405", "V": "34.44230100", "Q": "1246513.19668876", "B": "0"}}',
'{"e": "kline", "E": 1622537040053, "s": "BTCUSDT", "k": {"t": 1622536980000, "T": 1622537039999, "s": "BTCUSDT", "i": "1m", "f": 885323519, "L": 885326096, "o": "36124.50000000", "c": "36029.60000000", "h": "36127.44000000", "l": "36026.27000000", "v": "126.74777500", "n": 2578, "x": true, "q": "4572233.23511828", "V": "54.12827800", "Q": "1952337.79857981", "B": "0"}}'
]

output_3 = """rate_code; datetime; cost
BTCUSDT; 2021-06-01T08:39:59.999000; 16954.58714285714
BTCUSDT; 2021-06-01T08:40:59.999000; 21757.78
BTCUSDT; 2021-06-01T08:43:59.999000; 26536.537142857145
"""

def clear_error_log():
    with open(ERROR_LOG_FILE_PATH, "w"):
        pass

def error_log_not_empty():
    with open(ERROR_LOG_FILE_PATH, "r") as file:
        return bool(len(file.read()))

def read_output():
    with open(LOG_FILE_PATH, "r") as file:
        return file.read()


def setup_function():
    on_open(None)

@pytest.mark.parametrize("test_input, expected_output, expected_error",
                         [(test_dicts_1, output_1, False),
                          (test_dicts_2, output_2, False),
                          (test_dicts_3, output_3, True)])
def test_on_message(test_input, expected_output, expected_error):
    clear_error_log()
    for data_dict in test_input:
        on_message(None, data_dict)
    assert read_output() == expected_output
    assert error_log_not_empty() == expected_error
