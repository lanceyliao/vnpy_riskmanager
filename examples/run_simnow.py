import multiprocessing
import sys
from datetime import time, datetime
from logging import INFO
from time import sleep

from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
# 订阅行情
from vnpy.trader.setting import SETTINGS
from vnpy.trader.utility import load_json, save_json
from vnpy_ctastrategy import CtaStrategyApp
from vnpy_ctastrategy.base import EVENT_CTA_LOG
from vnpy_riskmanager import RiskManagerApp

# from tdays_setting import save_tdays_json_wind
from tdays_setting import save_tdays_json_rq
# from main_ticker import get_main_ticker_wind
from main_ticker import get_main_ticker_rq
import re
SETTINGS["log.active"] = True
SETTINGS["log.level"] = INFO
SETTINGS["log.console"] = True

import vnpy

version = vnpy.__version__
if version < '3.0.0':  # vnpy 2.*版本
    # 录制IB行情数据
    from vnpy.gateway.ctp import CtpGateway
else:  # vnpy 3.*版本
    from vnpy_ctp import CtpGateway

# 提前15分钟开启行情录制
# US_DAY_START = time(9, 10)  # Regular trading hours start at 9:30 AM ET
# US_DAY_END = time(16, 15)  # Regular trading hours end at 4:00 PM ET
# trading_hours = [(time(8, 45), time(11, 45)), (time(12, 45), time(15, 15)), (time(20, 45), time.max),
#                  (time.min, time(2, 45))]
# trading_hours = [(time.min, time(15, 15)), (time(20, 45), time.max)]
trading_hours = [(time(8, 45), time(15, 15)), (time(20, 45), time.max), (time.min, time(2, 45))]

# connect_filename = 'connect_ctp_gs.json'
connect_filename = 'connect_ctp_simnow.json'


# connect_filename = 'connect_ctp盘后.json'


def check_trading_period():
    """
    Check if it's trading period.
    """
    trading = False
    current_time = datetime.now().time()
    # if US_DAY_START <= current_time <= US_DAY_END:
    for start, end in trading_hours:
        if start <= current_time <= end:
            trading = True  # trading = True
        # simnow全天站点测试 lance
    return trading


from roll_over import RolloverTool


def check_change_ticker(cta_engine, main_engine, change_ticker):
    roll_tool = RolloverTool(cta_engine, main_engine)
    # 记录换月成功的合约
    success_ticker = []
    for old_symbol, new_symbol in change_ticker:
        flag = roll_tool.roll_all(old_symbol, new_symbol)
        if flag:
            change_ticker.remove((old_symbol, new_symbol))
            success_ticker.append((old_symbol, new_symbol))
            main_engine.write_log(f"合约换月完成：{old_symbol} -> {new_symbol}")
            # main_engine.get_engine("email").send_email("合约换月完成", f"合约换月完成：{old_symbol} -> {new_symbol}")
        else:
            main_engine.write_log(f"合约换月失败：{old_symbol} -> {new_symbol}")
    # 根据换月成功的合约，更新'main_ticker.json'
    if len(success_ticker):
        main_ticker = load_json('main_ticker.json')
        for old_symbol, new_symbol in success_ticker:
            symbol_pre = re.findall(r'[a-zA-Z]+', old_symbol)[0]
            main_ticker[symbol_pre] = new_symbol
        # new_main_ticker = {re.findall(r'[a-zA-Z]+', old_symbol)[0]: new_symbol for old_symbol, new_symbol in
        #                    success_ticker}
        # main_ticker |= new_main_ticker # python3.9
        save_json('main_ticker.json', main_ticker)
    main_engine.get_engine("email").send_email("主力合约变更", f"换月成功：{success_ticker}，失败：{list(set(change_ticker) - set(success_ticker))}")
    return change_ticker

def run_child():
    """
    Running in the child process.
    """
    SETTINGS["log.file"] = True

    # 创建事件引擎
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)

    # 添加交易接口
    main_engine.add_gateway(CtpGateway)
    main_engine.write_log("接口添加成功")

    # 添加风控引擎
    rm_engine = main_engine.add_app(RiskManagerApp)
    main_engine.write_log("风控引擎添加成功")

    cta_engine = main_engine.add_app(CtaStrategyApp)
    main_engine.write_log("主引擎创建成功")

    log_engine = main_engine.get_engine("log")
    event_engine.register(EVENT_CTA_LOG, log_engine.process_log_event)
    main_engine.write_log("注册日志事件监听")

    email_engine = main_engine.get_engine("email")

    # 获取数据接口配置
    setting = load_json(connect_filename)
    main_engine.write_log("数据接口配置加载成功")

    # 连接数据接口
    main_engine.connect(setting, "CTP")
    main_engine.write_log("连接CTP接口")

    sleep(10)
    main_engine.write_log("***查询资金和持仓***")
    # 查询资金 - 自动
    main_engine.write_log(main_engine.get_all_accounts())
    # 查询持仓
    main_engine.write_log(main_engine.get_all_positions())

    cta_engine.init_engine()
    main_engine.write_log("CTA策略初始化完成")

    # 给engine载入双均线策略，实盘运行
    main_engine.write_log("***从数据库读取准备数据, 实盘运行***")
    num_strategy = 0
    # strategy_symbol = load_json('sharpe_ge_1_2.json')
    # strategy_symbol = load_json('sharpe_ge_1_2_copy.json')# 6658310（开始权益） 2023.11.17 9:05:29
    # strategy_symbol = load_json('sharpe_ge_1_2_copy_10.json')
    # strategy_symbol = load_json('sharpe_ge_1_2_new.json')
    strategy_symbol = load_json('sharpe_ge_1_2_copy_j.json')

    # change_ticker = get_main_ticker_wind()
    change_ticker = get_main_ticker_rq()
    main_engine.write_log("保存主力合约完成")
    if len(change_ticker) > 0:
        # 发送邮件
        email_engine.send_email("主力合约变更", f"主力合约变更：{change_ticker}")
        main_engine.write_log(f"主力合约变更：{change_ticker}")

    # save_tdays_json_wind()
    save_tdays_json_rq()
    main_engine.write_log("保存交易日完成")
    sleep(5)

    main_ticker = load_json('main_ticker.json')
    for strategy_name, symbol_list in strategy_symbol.items():
        for symbol in symbol_list:
            vt_symbol = main_ticker.get(symbol, None)
            strategy_class_name = strategy_name.split("_ly")[0]
            strategy_class = strategy_class_name

            # 普通策略+plus策略
            if 'plus_' in strategy_name:
                strategy_class = strategy_class_name.strip('plus_')
            cta_engine.add_strategy(strategy_class, f'{strategy_class_name}_{symbol}', vt_symbol, {})
            num_strategy += 1

            # 只添加plus策略
            # if 'plus_' in strategy_name:
            #     strategy_class = strategy_class_name.strip('plus_')
            #     cta_engine.add_strategy(strategy_class, f'{strategy_class_name}_{symbol}', vt_symbol, {})
            #     num_strategy += 1

            # 只添加普通策略
            # if 'plus_' not in strategy_name:
            #     cta_engine.add_strategy(strategy_class, f'{strategy_class_name}_{symbol}', vt_symbol, {})
            #     num_strategy += 1

    sleep(5)  # Leave enough time to complete strategy initialization

    cta_engine.init_all_strategies()
    sleep(0.8 * num_strategy)  # Leave enough time to complete strategy initialization
    main_engine.write_log("CTA策略全部初始化")

    cta_engine.start_all_strategies()
    main_engine.write_log("CTA策略全部启动")
    # 发送邮件（包含时间信息）
    email_engine.send_email("CTA策略全部启动", f"{datetime.now()} CTA策略全部启动")

    count = 0
    while True:
        sleep(10)
        count += 1

        trading = check_trading_period()
        if not trading:
            print("关闭子进程")
            main_engine.close()
            sys.exit(0)
        elif len(change_ticker) and count % 3 == 0:# 每30秒检查一次
            # 限制在上午9：30到10：00之间换月
            # if time(9, 30) <= datetime.now().time() <= time(10, 0):
            main_engine.write_log(f"合约换月：{change_ticker}")
            change_ticker = check_change_ticker(cta_engine, main_engine, change_ticker)


def run_parent():
    """
    Running in the parent process.
    """
    print("启动CTA策略守护父进程")

    child_process = None

    while True:
        trading = check_trading_period()

        # Start child process in trading period
        if trading and child_process is None:
            print("启动子进程")
            child_process = multiprocessing.Process(target=run_child)
            child_process.start()
            print("子进程启动成功")

        # 非记录时间则退出子进程
        if not trading and child_process is not None:
            if not child_process.is_alive():
                child_process = None
                print("子进程关闭成功")

        sleep(5)


if __name__ == "__main__":
    run_parent()
