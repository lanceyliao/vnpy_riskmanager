from typing import Dict

from vnpy.event import Event
from vnpy.trader.constant import Offset
from vnpy.trader.object import OrderRequest, TradeData
from vnpy.trader.utility import load_json, save_json

from vnpy_riskmanager import RiskEngine


class SymbolIntradayMaxOpen(RiskEngine):
    def init_plugin(self):
        self.symbol_max_open: Dict[str, int] = load_json("symbol_max_open.json")
        self.contract_max_open: Dict[str, int] = load_json("contract_max_open.json")

    def check_risk(self, req: OrderRequest, gateway_name: str) -> bool:
        # SymbolIntradayMaxOpen的check_risk方法：风控模块4：“XX品种当日开仓量超过XX”
        symbol = req.vt_symbol.rsplit(".", 1)[0].upper()
        for i, s in enumerate(symbol):
            if s.isdigit():
                pre = symbol[:i]
                break
        if pre in self.symbol_max_open and self.symbol_max_open[pre] <= 0:
            self.write_log(f"{pre}当日开仓量已达上限")
            return False
        if symbol in self.contract_max_open and self.contract_max_open[symbol] <= 0:
            self.write_log(f"{symbol}当日开仓量已达上限")
            return False

    def process_trade_event_(self, event: Event) -> None:
        """"""
        trade: TradeData = event.data
        if trade.offset != Offset.OPEN:
            return
        symbol = trade.symbol.upper()
        for i, s in enumerate(symbol):
            if s.isdigit():
                pre = symbol[:i]
                break
        if pre in self.symbol_max_open and self.symbol_max_open[pre] > 0:
            self.symbol_max_open[pre] -= trade.volume

        if symbol in self.contract_max_open and self.contract_max_open[symbol] > 0:
            self.contract_max_open[symbol] -= trade.volume


if __name__ == "__main__":
    default_symbol_max_open = {'RB': 32000, 'FU': 16000, 'AG': 13000, 'HC': 10000, 'SP': 8000, 'RU': 6000, 'AL': 4000,
                               'ZN': 3000, 'AU': 2800, 'CU': 2000, 'SC': 3200, 'EC': 1000, 'LH': 1000, 'I': 2000,
                               'J': 50, 'JM': 1000, 'P': 10000, 'PG': 10000, 'M': 20000, 'V': 18000, 'Y': 15000,
                               'PP': 10000, 'C': 8000, 'L': 8000, 'SI': 3000, 'LC': 10000, 'SA': 10000, 'TA': 30000,
                               'MA': 25000, 'FG': 25000, 'OI': 10000, 'SR': 10000, 'CF': 10000, 'RM': 15000,
                               'SM': 10000, 'UR': 2000, 'IF': 500, 'IC': 500, 'IH': 500, 'IM': 500, 'IO': 100,
                               'HO': 100, 'MO': 100}
    default_contract_max_open = {'EC2406': 100, 'EC2408': 100, 'EC2410': 100, 'EC2412': 100, 'EC2502': 100,
                                 'EC2504': 100, 'BB2501': 1, 'BB2502': 1, 'BB2503': 1, 'BB2504': 1, 'I2409': 500,
                                 'LC2407': 3000, 'ZC406': 20, 'ZC407': 20, 'ZC408': 20, 'ZC409': 20, 'ZC410': 20,
                                 'ZC411': 20, 'ZC412': 20, 'ZC501': 20, 'ZC502': 20, 'ZC503': 20, 'ZC504': 20,
                                 'ZC505': 20, 'SA406': 1000, 'SA407': 1000, 'SA408': 1000, 'FG406': 10000,
                                 'FG407': 10000, 'FG408': 10000, 'SM406': 2000, 'SM407': 2000, 'SM408': 2000,
                                 'CJ406': 100, 'CJ407': 100, 'CJ408': 100, 'CJ409': 100, 'UR406': 2000, 'UR407': 2000,
                                 'UR408': 2000, 'UR409': 2000}
    save_json("symbol_max_open.json", default_symbol_max_open)
    save_json("contract_max_open.json", default_contract_max_open)
