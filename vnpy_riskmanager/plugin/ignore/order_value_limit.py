from typing import Dict, Optional

from vnpy.trader.object import OrderRequest, ContractData
from vnpy.trader.utility import load_json, save_json
from vnpy_riskmanager import RiskEngine


class OrderValueLimit(RiskEngine):
    """订单价值量限制"""
    
    def init_plugin(self):
        """初始化插件配置"""
        # 加载配置文件
        config = load_json("order_value_limit.json")
        
        # 设置默认值
        self.base_account_value = config.get("base_account_value", 1000000)  # 基准账户资金，默认100万
        self.base_max_order_value = config.get("base_max_order_value", 8000)  # 基准最大订单价值量，默认8000
        
        self.write_log(f"订单价值量限制插件加载成功，基准账户资金:{self.base_account_value}，基准最大订单价值量:{self.base_max_order_value}")

    def check_risk(self, req: OrderRequest, gateway_name: str) -> bool:
        """检查风控"""
        # 1. 获取当前账户资金
        account_value = self.get_balance(gateway_name)
        if not account_value:
            self.write_log(f"无法获取账户资金[{gateway_name}]，拒绝订单")
            return False
            
        # 2. 计算当前订单的价值量
        # 获取合约信息
        contract: Optional[ContractData] = self.main_engine.get_contract(req.vt_symbol)
        if not contract:
            self.write_log(f"找不到合约信息{req.vt_symbol}，拒绝订单")
            return False
            
        # 使用合约的size属性(币安为1,CTP为真实合约乘数)
        order_value = req.volume * req.price * contract.size
            
        # 3. 计算允许的最大订单价值量
        # 按照账户资金比例计算，例如:
        # 如果基准账户100万可下8000，那么5万的账户可下400
        max_order_value = (account_value / self.base_account_value) * self.base_max_order_value
        
        # 4. 检查是否超过限制
        if order_value > max_order_value:
            self.write_log(f"订单价值量{order_value:.2f}超过账户限制{max_order_value:.2f}")
            return False
            
        return True


if __name__ == "__main__":
    # 创建默认配置文件
    default_config = {
        "base_account_value": 1000000,  # 基准账户资金100万
        "base_max_order_value": 8000,   # 基准最大订单价值量8000
    }
    save_json("order_value_limit.json", default_config)