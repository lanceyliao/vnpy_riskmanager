from vnpy.trader.object import OrderRequest
from vnpy.trader.utility import load_json, save_json
from vnpy_riskmanager import RiskEngine


class SymbolRestriction(RiskEngine):
    def init_plugin(self):
        # 0. 默认使用白名单模块
        self.restrict_by_white_list = True
        # 1. 读取配置文件，获取白名单、黑名单
        if self.restrict_by_white_list:
            self.restriction_list = load_json("white_list.json")['white_list']
            self.write_log(f"使用白名单模块，白名单：{self.restriction_list}")
        else:
            self.restriction_list = load_json("black_list.json")['black_list']
            self.write_log(f"使用黑名单模块，黑名单：{self.restriction_list}")

    def check_risk(self, req: OrderRequest, gateway_name: str) -> bool:
        # SymbolRestriction的check_risk方法：风控模块3：“涉及XX品种，直接drop掉”
        #   ===> 风控模块增加两个配置文件，white_list/black_list。
        #             实现白名单、黑名单两种场景。
        #             代码中控制启动白名单or黑名单模块。

        # 2. 判断是否在白名单/黑名单中，如果不在白名单，直接drop掉（或者如果在黑名单，直接drop掉）
        symbol = req.vt_symbol.split(".")[0]
        if self.restrict_by_white_list and symbol not in self.restriction_list:
            self.write_log(f"{symbol}不在白名单中，直接drop掉")
            return False
        elif not self.restrict_by_white_list and symbol in self.restriction_list:
            self.write_log(f"{symbol}在黑名单中，直接drop掉")
            return False
        return True


if __name__ == "__main__":
    # 将'rb2405'加入到白名单中，并保存到white_list.json文件中
    white_list = {'white_list': ['rb2405']}
    save_json("white_list.json", white_list)
