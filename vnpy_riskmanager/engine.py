from collections import defaultdict
from typing import Callable, Dict, Optional, List
from types import MethodType
from vnpy.event import Event, EventEngine
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import OrderData, OrderRequest, LogData, TradeData, PositionData, AccountData
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.event import EVENT_TRADE, EVENT_ORDER, EVENT_LOG, EVENT_TIMER
from vnpy.trader.constant import Direction, Status
from vnpy.trader.utility import load_json, save_json

from sqlalchemy import create_engine, text
from urllib.parse import quote_plus as urlquote
from datetime import datetime

from vnpy_ctp.gateway.ctp_gateway import CtpTdApi

APP_NAME = "RiskManager"


class RiskEngine(BaseEngine):
    """风控引擎"""

    setting_filename: str = "risk_manager_setting.json"

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.active: bool = False

        self.order_flow_count: int = 0
        self.order_flow_limit: int = 50# 委托流控上限（笔）

        self.order_flow_clear: int = 1# 委托流控清空（秒）
        self.order_flow_timer: int = 0

        # self.position_timer: int = 0
        # self.position_flash: int = 120# 持仓刷新（秒）
        #
        # userName = 'root'
        # password = 'p0o9i8u7'
        # dbHost = 'localhost'
        # dbPort = 3306
        # dbName = 'scout'
        # DB_CONNECT = f'mysql+pymysql://{userName}:{urlquote(password)}@{dbHost}:{dbPort}/{dbName}?charset=utf8'
        # self.mysql_engine = create_engine(
        #     DB_CONNECT,
        #     max_overflow=50,  # 超过连接池大小外最多创建的连接
        #     pool_size=50,  # 连接池大小
        #     pool_timeout=5,  # 池中没有线程最多等待的时间，否则报错
        #     pool_recycle=-1,  # 多久之后对线程池中的线程进行一次连接的回收（重置）
        #     # encoding='utf-8',
        #     echo=False
        # )

        self.plugin_count = 0
        self.init_plugin_count = 0
        self.load_check_risk_plugin()
        # 执行init_plugin方法（1、2、...）
        if self.init_plugin_count > 0:
            for i in range(1, self.init_plugin_count + 1):
                getattr(self, f'init_plugin_{i}')()

        self.order_size_limit: int = 100# 单笔委托上限（数量）

        self.trade_count: int = 0
        self.trade_limit: int = 1000# 总成交上限（笔）

        self.order_cancel_limit: int = 500# 合约撤单上限（笔）
        self.order_cancel_counts: Dict[str, int] = defaultdict(int)

        self.active_order_limit: int = 50# 活动委托上限（笔）

        self.active_order_books: Dict[str, ActiveOrderBook] = {}

        self.load_setting()
        self.register_event()
        self.patch_send_order()

    def patch_send_order(self) -> None:
        """
        Patch send order function of MainEngine.
        """
        self._send_order: Callable[[OrderRequest, str], str] = self.main_engine.send_order
        self.main_engine.send_order = self.send_order

    def send_order(self, req: OrderRequest, gateway_name: str) -> str:
        """"""
        result: bool = self.check_risk(req, gateway_name)
        if not result:
            return ""

        # 执行风控插件（1、2、...）
        if self.plugin_count > 0:
            for i in range(1, self.plugin_count + 1):
                result: bool = getattr(self, f'check_risk_{i}')(req, gateway_name)
                if not result:
                    self.write_log(f"风控插件{i}拦截此笔委托")
                    return ""

        return self._send_order(req, gateway_name)

    def load_check_risk_plugin(self):
        import inspect, os, importlib
        from glob import glob
        # 导入plugin目录下的所有.py文件中，RiskEngine的子类的check_risk方法并按顺序赋予别名
        for f in glob(os.path.join(os.path.dirname(__file__), 'plugin', '*.py')):
            module_name = os.path.basename(f)[:-3]
            module = importlib.import_module(f'vnpy_riskmanager.plugin.{module_name}')
            for name, obj in inspect.getmembers(module):
                if inspect.isclass(obj) and issubclass(obj, RiskEngine) and obj is not RiskEngine:
                    self.plugin_count += 1
                    # 如果RiskEngine的子类的init_plugin方法存在，赋予别名
                    if hasattr(obj, 'init_plugin'):
                        self.init_plugin_count += 1
                        setattr(self, f'init_plugin_{self.init_plugin_count}', MethodType(obj.init_plugin, self))
                    if hasattr(obj, 'process_trade_event_'):
                        setattr(self, f'process_trade_event_{self.plugin_count}', MethodType(obj.process_trade_event_, self))
                        self.event_engine.register(EVENT_TRADE, getattr(self, f'process_trade_event_{self.plugin_count}'))
                    setattr(self, f'check_risk_{self.plugin_count}', MethodType(obj.check_risk, self))
                    self.write_log(f"风控插件{name}加载成功")
        self.write_log(f"风控插件加载成功，共加载{self.plugin_count}个插件")

    def update_setting(self, setting: dict) -> None:
        """"""
        self.active = setting["active"]
        self.order_flow_limit = setting["order_flow_limit"]
        self.order_flow_clear = setting["order_flow_clear"]
        self.order_size_limit = setting["order_size_limit"]
        self.trade_limit = setting["trade_limit"]
        self.active_order_limit = setting["active_order_limit"]
        self.order_cancel_limit = setting["order_cancel_limit"]

        if self.active:
            self.write_log("交易风控功能启动")
        else:
            self.write_log("交易风控功能停止")

    def get_setting(self) -> dict:
        """"""
        setting: dict = {
            "active": self.active,# 风控运行状态
            "order_flow_limit": self.order_flow_limit,# 委托流控上限（笔）
            "order_flow_clear": self.order_flow_clear,# 委托流控清空（秒）
            "order_size_limit": self.order_size_limit,# 单笔委托上限（数量）
            "trade_limit": self.trade_limit,# 总成交上限（笔）
            "active_order_limit": self.active_order_limit,# 活动委托上限（笔）
            "order_cancel_limit": self.order_cancel_limit,# 合约撤单上限（笔）
        }
        return setting

    def load_setting(self) -> None:
        """"""
        setting: dict = load_json(self.setting_filename)
        if not setting:
            return

        self.update_setting(setting)

    def save_setting(self) -> None:
        """"""
        setting: dict = self.get_setting()
        save_json(self.setting_filename, setting)

    def register_event(self) -> None:
        """"""
        self.event_engine.register(EVENT_TRADE, self.process_trade_event)
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        self.event_engine.register(EVENT_ORDER, self.process_order_event)

    def process_order_event(self, event: Event) -> None:
        """"""
        order: OrderData = event.data

        order_book: ActiveOrderBook = self.get_order_book(order.vt_symbol)
        order_book.update_order(order)

        if order.status != Status.CANCELLED:
            return
        self.order_cancel_counts[order.vt_symbol] += 1

    def process_trade_event(self, event: Event) -> None:
        """"""
        trade: TradeData = event.data
        self.trade_count += trade.volume

    def process_timer_event(self, event: Event) -> None:
        """"""
        self.order_flow_timer += 1

        if self.order_flow_timer >= self.order_flow_clear:
            self.order_flow_count = 0
            self.order_flow_timer = 0

        # self.position_timer += 1
        # if self.position_timer >= self.position_flash:
        #     self.position_timer = 0
        #     self.save_positions()

    def get_balance(self) -> float:
        # 法1
        # self.accs: Dict[str, AccountData] = self.main_engine.get_engine('oms').accounts


        # 法2
        # accs_value: List[AccountData] = self.main_engine.get_all_accounts()
        # # 如果账户数量为0，返回0
        # if len(accs_value) == 0:
        #     return 0
        # # 如果账户数量不为0，返回账户权益
        # return accs_value[0].balance


        # # 法3
        gateway_name: str = 'CTP'
        # accountid = '123456'
        accountid: str = self.get_accountid()
        vt_accountid: str = f"{gateway_name}.{accountid}"
        self.acc: Optional[AccountData] = self.main_engine.get_account(vt_accountid)
        if not self.acc:
            return 0
        return self.acc.balance

    def get_accountid(self) -> str:
        gateway_name: str = 'CTP'
        gateway: BaseGateway = self.main_engine.get_gateway(gateway_name)
        td_api: "CtpTdApi" = gateway.td_api
        accountid: str = td_api.userid
        return accountid

    def save_positions(self) -> None:
        self.positions: List[PositionData] = self.main_engine.get_all_positions()
        self.save_mysql(self.positions)

    # 根据vt_symbol获取对应的持仓列表
    def get_symbol_positions(self, vt_symbol) -> List[PositionData]:
        self.positions: List[PositionData] = self.main_engine.get_all_positions()
        return [position for position in self.positions if position.vt_symbol == vt_symbol and position.volume != 0]

    def save_mysql(self, positions) -> None:
        # 1.删除
        sql = f"delete from user_position where user='zhongxin'"
        with self.mysql_engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()
            self.write_log(f"持仓数据删除成功：{sql}")
        # 2.检查position数量，如果为0，直接返回；如果不为0，批量插入
        if len(positions) == 0:
            return
        # 3.批量插入
        sql = f"insert into user_position(user,symbol,direction,volume,remarks,create_date,price,yd_volume,frozen) values"
        create_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for position in positions:
            # 如果持仓不为0，插入
            if position.volume != 0:
                sql += f"('zhongxin','{position.vt_symbol}','{position.direction.value}','{position.volume}',null,'{create_date}','{position.price}','{position.yd_volume}','0'),"
        sql = sql[:-1]
        with self.mysql_engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()
            self.write_log(f"持仓数据保存成功：{sql}")

    def write_log(self, msg: str) -> None:
        """"""
        log: LogData = LogData(msg=msg, gateway_name="RiskManager")
        event: Event = Event(type=EVENT_LOG, data=log)
        self.event_engine.put(event)

    def check_risk(self, req: OrderRequest, gateway_name: str) -> bool:
        """"""
        if not self.active:
            return True

        # Check order volume
        if req.volume <= 0:
            self.write_log("委托数量必须大于0")
            return False

        if req.volume > self.order_size_limit:
            self.write_log(
                f"单笔委托数量{req.volume}，超过限制{self.order_size_limit}")
            return False

        # Check trade volume
        if self.trade_count >= self.trade_limit:
            self.write_log(
                f"今日总成交合约数量{self.trade_count}，超过限制{self.trade_limit}")
            return False

        # Check flow count
        if self.order_flow_count >= self.order_flow_limit:
            self.write_log(
                f"委托流数量{self.order_flow_count}，超过限制每{self.order_flow_clear}秒{self.order_flow_limit}次")
            return False

        # Check all active orders
        active_order_count: int = len(self.main_engine.get_all_active_orders())
        if active_order_count >= self.active_order_limit:
            self.write_log(
                f"当前活动委托次数{active_order_count}，超过限制{self.active_order_limit}")
            return False

        # Check order cancel counts
        order_cancel_count: int = self.order_cancel_counts.get(req.vt_symbol, 0)
        if order_cancel_count >= self.order_cancel_limit:
            self.write_log(f"当日{req.vt_symbol}撤单次数{order_cancel_count}，超过限制{self.order_cancel_limit}")
            return False

        # Check order self trade
        order_book: ActiveOrderBook = self.get_order_book(req.vt_symbol)
        if req.direction == Direction.LONG:
            best_ask: float = order_book.get_best_ask()
            if best_ask and req.price >= best_ask:
                self.write_log(f"买入价格{req.price}大于等于已挂最低卖价{best_ask}，可能导致自成交")
                return False
        else:
            best_bid: float = order_book.get_best_bid()
            if best_bid and req.price <= best_bid:
                self.write_log(f"卖出价格{req.price}小于等于已挂最低买价{best_bid}，可能导致自成交")
                return False

        # Add flow count if pass all checks
        self.order_flow_count += 1
        return True

    def get_order_book(self, vt_symbol: str) -> "ActiveOrderBook":
        """"""
        order_book: Optional[ActiveOrderBook] = self.active_order_books.get(vt_symbol, None)
        if not order_book:
            order_book = ActiveOrderBook(vt_symbol)
            self.active_order_books[vt_symbol] = order_book
        return order_book


class ActiveOrderBook:
    """活动委托簿"""

    def __init__(self, vt_symbol: str) -> None:
        """"""
        self.vt_symbol: str = vt_symbol

        self.bid_prices: Dict[str, float] = {}
        self.ask_prices: Dict[str, float] = {}

    def update_order(self, order: OrderData) -> None:
        """更新委托数据"""
        if order.is_active():
            if order.direction == Direction.LONG:
                self.bid_prices[order.vt_orderid] = order.price
            else:
                self.ask_prices[order.vt_orderid] = order.price
        else:
            if order.vt_orderid in self.bid_prices:
                self.bid_prices.pop(order.vt_orderid)
            elif order.vt_orderid in self.ask_prices:
                self.ask_prices.pop(order.vt_orderid)

    def get_best_bid(self) -> float:
        """获取最高买价"""
        if not self.bid_prices:
            return 0
        return max(self.bid_prices.values())

    def get_best_ask(self) -> float:
        """获取最低卖价"""
        if not self.ask_prices:
            return 0
        return min(self.ask_prices.values())
