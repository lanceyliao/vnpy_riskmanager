from typing import Optional

from vnpy.trader.constant import Direction
from vnpy.trader.object import OrderRequest, TradeData, OrderData, PositionData
from vnpy_riskmanager import RiskEngine
from vnpy.utils.symbol_info import all_sizes, extract_symbol_pre

class SymbolFrozen(RiskEngine):
    def check_risk(self, req: OrderRequest, gateway_name: str) -> bool:
        # 父类check_risk方法：
        # if not self.active:
        #     return True
        #
        # # Check order volume
        # if req.volume <= 0:
        #     self.write_log("委托数量必须大于0")
        #     return False
        #
        # if req.volume > self.order_size_limit:
        #     self.write_log(f"单笔委托数量{req.volume}，超过限制{self.order_size_limit}")
        #     return False
        #
        # # Check trade volume
        # if self.trade_count >= self.trade_limit:
        #     self.write_log(f"今日总成交合约数量{self.trade_count}，超过限制{self.trade_limit}")
        #     return False
        #
        # # Check flow count
        # if self.order_flow_count >= self.order_flow_limit:
        #     self.write_log(
        #         f"委托流数量{self.order_flow_count}，超过限制每{self.order_flow_clear}秒{self.order_flow_limit}次")
        #     return False
        #
        # # Check all active orders
        # active_order_count: int = len(self.main_engine.get_all_active_orders())
        # if active_order_count >= self.active_order_limit:
        #     self.write_log(f"当前活动委托次数{active_order_count}，超过限制{self.active_order_limit}")
        #     return False
        #
        # # Check order cancel counts
        # order_cancel_count: int = self.order_cancel_counts.get(req.vt_symbol, 0)
        # if order_cancel_count >= self.order_cancel_limit:
        #     self.write_log(f"当日{req.vt_symbol}撤单次数{order_cancel_count}，超过限制{self.order_cancel_limit}")
        #     return False
        #
        # # Check order self trade
        # order_book: ActiveOrderBook = self.get_order_book(req.vt_symbol)
        # if req.direction == Direction.LONG:
        #     best_ask: float = order_book.get_best_ask()
        #     if best_ask and req.price >= best_ask:
        #         self.write_log(f"买入价格{req.price}大于等于已挂最低卖价{best_ask}，可能导致自成交")
        #         return False
        # else:
        #     best_bid: float = order_book.get_best_bid()
        #     if best_bid and req.price <= best_bid:
        #         self.write_log(f"卖出价格{req.price}小于等于已挂最低买价{best_bid}，可能导致自成交")
        #         return False
        #
        # # Add flow count if pass all checks
        # self.order_flow_count += 1
        # return True

        # SymbolFrozen的check_risk方法：当单品种持仓占用资金超过5%时，限制开仓。
        # 1.获取当前持仓
        vt_symbol: str = req.vt_symbol
        symbol_pre, symbol_num, exchange = extract_symbol_pre(vt_symbol)
        positions: List[PositionData] = self.get_symbol_positions(vt_symbol)
        # 2.获取当前持仓占用资金（各条持仓信息的：持仓数量*持仓方向*持仓均价*合约乘数 加总）
        frozen: float = 0
        for position in positions:
            if position.direction == Direction.LONG:
                direction = 1
            elif position.direction == Direction.SHORT:
                direction = -1
            else:
                direction = 0
            frozen += position.volume * direction * position.price
        frozen = abs(frozen * all_sizes[symbol_pre])
        # 3.获取当前持仓占用资金占总资金比例
        total: float = self.get_balance()
        if total == 0:
            return False
        ratio: float = frozen / total
        # 4.判断是否超过5%
        if ratio > 0.05:
            self.write_log(f"品种{vt_symbol}持仓占用资金{frozen}，超过总资金{total}的5%")
            return False
        else:
            return True
        # 5.获取当前委托品种的tick中的价格、开平仓方向
        tick: TickData = self.main_engine.get_tick(vt_symbol)
        price: float = tick.last_price
        direction: Direction = req.direction
        offset: Offset = req.offset
        # 6.计算当前委托对该品种持仓占用资金的增减
        # todo: 注：此处计算方式待定。其他计算方式如，按 volume 的绝对值 计算，不考虑 锁仓的price。
        # 7.判断如果执行该委托后，持仓占用资金占总资金比例是否超过5%
