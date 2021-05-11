""""""
import hashlib
import hmac
import time
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Callable
from copy import copy
import pytz
from pytz import timezone
from tzlocal import get_localzone
from simplejson.errors import JSONDecodeError
from requests import Response

from vnpy.trader.constant import (
    Exchange,
    Interval,
    OrderType,
    Product,
    Status,
    Direction
)
from vnpy.trader.object import (
    AccountData,
    BarData,
    TickData,
    OrderData,
    TradeData,
    ContractData,
    PositionData,
    HistoryRequest,
    SubscribeRequest,
    CancelRequest,
    OrderRequest
)
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.gateway import BaseGateway
from vnpy.event.engine import EventEngine

from requests import ConnectionError
from vnpy_websocket import WebsocketClient
from vnpy_rest import RestClient, Request


# 本地时区
LOCAL_TZ: timezone = get_localzone()

# UTC时区
UTC_TZ = pytz.utc

# 实盘REST API地址
REST_HOST = "https://api.bybit.com"

# 实盘Websocket API地址
INVERSE_WEBSOCKET_HOST = "wss://stream.bybit.com/realtime"
PUBLIC_WEBSOCKET_HOST = "wss://stream.bybit.com/realtime_public"
PRIVATE_WEBSOCKET_HOST = "wss://stream.bybit.com/realtime_private"

# 模拟盘REST API地址
TESTNET_REST_HOST = "https://api-testnet.bybit.com"

# 模拟盘Websocket API地址
TESTNET_INVERSE_WEBSOCKET_HOST = "wss://stream-testnet.bybit.com/realtime"
TESTNET_PUBLIC_WEBSOCKET_HOST = "wss://stream-testnet.bybit.com/realtime_public"
TESTNET_PRIVATE_WEBSOCKET_HOST = "wss://stream-testnet.bybit.com/realtime_private"

# 委托状态映射
STATUS_BYBIT2VT: Dict[str, Status] = {
    "Created": Status.NOTTRADED,
    "New": Status.NOTTRADED,
    "PartiallyFilled": Status.PARTTRADED,
    "Filled": Status.ALLTRADED,
    "Cancelled": Status.CANCELLED,
    "Rejected": Status.REJECTED,
}

# 委托类型映射
ORDER_TYPE_VT2BYBIT: Dict[OrderType, str] = {
    OrderType.LIMIT: "Limit",
    OrderType.MARKET: "Market",
}
ORDER_TYPE_BYBIT2VT: Dict[str, OrderType] = {v: k for k, v in ORDER_TYPE_VT2BYBIT.items()}

# 买卖方向映射
DIRECTION_VT2BYBIT: Dict[Direction, str] = {Direction.LONG: "Buy", Direction.SHORT: "Sell"}
DIRECTION_BYBIT2VT: Dict[str, Direction] = {v: k for k, v in DIRECTION_VT2BYBIT.items()}

OPPOSITE_DIRECTION: Dict[Direction, Direction] = {
    Direction.LONG: Direction.SHORT,
    Direction.SHORT: Direction.LONG,
}

# 数据频率映射
INTERVAL_VT2BYBIT: Dict[Interval, str] = {
    Interval.MINUTE: "1",
    Interval.HOUR: "60",
    Interval.DAILY: "D",
    Interval.WEEKLY: "W",
}
TIMEDELTA_MAP: Dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
    Interval.WEEKLY: timedelta(days=7),
}

# 正向合约类型列表
symbols_usdt: List[str] = ["BTCUSDT"]

# 反向合约类型列表
symbols_inverse: List[str] = ["BTCUSD", "ETHUSD", "EOSUSD", "XRPUSD"]


class BybitGateway(BaseGateway):
    """
    vn.py用于对接Bybit交易所的交易接口。
    """

    default_setting: Dict[str, str] = {
        "ID": "",
        "Secret": "",
        "服务器": ["REAL", "TESTNET"],
        "合约模式": ["反向", "正向"],
        "代理地址": "",
        "代理端口": "",
    }

    exchanges: List[Exchange] = [Exchange.BYBIT]

    def __init__(self, event_engine: EventEngine, gateway_name: str = "BYBIT") -> None:
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.rest_api: "BybitRestApi" = BybitRestApi(self)
        self.private_ws_api: "BybitPrivateWebsocketApi" = BybitPrivateWebsocketApi(self)
        self.public_ws_api: "BybitPublicWebsocketApi" = BybitPublicWebsocketApi(self)

    def connect(self, setting: dict) -> None:
        """连接交易接口"""
        key: str = setting["ID"]
        secret: str = setting["Secret"]
        server: str = setting["服务器"]
        proxy_host: str = setting["代理地址"]
        proxy_port: str = setting["代理端口"]

        if setting["合约模式"] == "正向":
            usdt_base = True
        else:
            usdt_base = False

        if proxy_port.isdigit():
            proxy_port = int(proxy_port)
        else:
            proxy_port = 0

        self.rest_api.connect(
            usdt_base,
            key,
            secret,
            server,
            proxy_host,
            proxy_port
        )
        self.private_ws_api.connect(
            usdt_base,
            key, secret,
            server,
            proxy_host,
            proxy_port
        )
        self.public_ws_api.connect(
            usdt_base,
            server,
            proxy_host,
            proxy_port
        )

        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.public_ws_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        return self.rest_api.send_order(req)

    def cancel_order(self, req: CancelRequest):
        """委托撤单"""
        self.rest_api.cancel_order(req)

    def query_account(self) -> None:
        """查询资金"""
        pass

    def query_position(self) -> None:
        """查询持仓"""
        return
        self.rest_api.query_position()

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """查询历史数据"""
        return self.rest_api.query_history(req)

    def close(self) -> None:
        """关闭连接"""
        self.rest_api.stop()
        self.private_ws_api.stop()
        self.public_ws_api.stop()

    def process_timer_event(self, event):
        """处理定时事件"""
        self.query_position()


class BybitRestApi(RestClient):
    """"""

    def __init__(self, gateway: BybitGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: BybitGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.usdt_base: bool = False
        self.key: str = ""
        self.secret: bytes = b""

        self.order_count: int = 0
        self.contract_codes: set = set()

    def sign(self, request: Request) -> Request:
        """ 生成ByBit签名"""
        request.headers = {"Referer": "vn.py"}

        if request.method == "GET":
            api_params = request.params
            if api_params is None:
                api_params = request.params = {}
        else:
            api_params = request.data
            if api_params is None:
                api_params = request.data = {}

        api_params["api_key"] = self.key
        api_params["recv_window"] = 30 * 1000
        api_params["timestamp"] = generate_timestamp(-5)

        data2sign = "&".join(
            [f"{k}={v}" for k, v in sorted(api_params.items())])
        signature: str = sign(self.secret, data2sign.encode())
        api_params["sign"] = signature

        return request

    def new_orderid(self) -> str:
        """生成本地委托号"""
        prefix: str = datetime.now().strftime("%Y%m%d-%H%M%S-")

        self.order_count += 1
        suffix: str = str(self.order_count).rjust(8, "0")

        orderid: str = prefix + suffix
        return orderid

    def connect(
        self,
        usdt_base: bool,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int,
    ) -> None:
        """连接REST服务器"""
        self.usdt_base = usdt_base
        self.key = key
        self.secret = secret.encode()

        if server == "REAL":
            self.init(REST_HOST, proxy_host, proxy_port)
        else:
            self.init(TESTNET_REST_HOST, proxy_host, proxy_port)

        self.start(3)
        self.gateway.write_log("REST API启动成功")

        self.query_contract()
        self.query_order()

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        orderid: str = self.new_orderid()
        order: OrderData = req.create_order_data(orderid, self.gateway_name)

        data: dict = {
            "symbol": req.symbol,
            "side": DIRECTION_VT2BYBIT[req.direction],
            "qty": float(req.volume),
            "order_link_id": orderid,
            "time_in_force": "GoodTillCancel",
            "reduce_only": False,
            "close_on_trigger": False
        }

        data["order_type"] = ORDER_TYPE_VT2BYBIT[req.type]
        data["price"] = req.price

        if self.usdt_base:
            path: str = "/private/linear/order/create"
        else:
            path: str = "/v2/private/order/create"

        self.add_request(
            "POST",
            path,
            callback=self.on_send_order,
            data=data,
            extra=order,
            on_failed=self.on_send_order_failed,
            on_error=self.on_send_order_error,
        )

        self.gateway.on_order(order)
        return order.vt_orderid

    def on_send_order_failed(
        self,
        status_code: int,
        request: Request
    ) -> None:
        """委托下单失败服务器报错回报"""
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        data: dict = request.response.json()
        error_msg: str = data["ret_msg"]
        error_code: int = data["ret_code"]
        msg = f"委托失败，错误代码:{error_code},  错误信息：{error_msg}"
        self.gateway.write_log(msg)

    def on_send_order_error(
        self,
        exception_type: type,
        exception_value: Exception,
        tb,
        request: Request
    ) -> None:
        """委托下单回报函数报错回报"""
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)

    def on_send_order(self, data: dict, request: Request) -> None:
        """委托下单回报"""
        if self.check_error("委托下单", data):
            order: OrderData = request.extra
            order.status = Status.REJECTED
            self.gateway.on_order(order)
            return

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        data: dict = {
            "symbol": req.symbol,
            "order_link_id": req.orderid
        }

        if self.usdt_base:
            path: str = "/private/linear/order/cancel"
        else:
            path: str = "/v2/private/order/cancel"

        self.add_request(
            "POST",
            path,
            data=data,
            callback=self.on_cancel_order
        )

    def on_cancel_order_error(
        self,
        exception_type: type,
        exception_value: Exception,
        tb,
        request: Request
    ) -> None:
        """委托撤单回报函数报错回报"""
        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)

    def on_cancel_order(self, data: dict, request: Request) -> None:
        """委托撤单回报"""
        if self.check_error("委托撤单", data):
            return

    def on_failed(self, status_code: int, request: Request) -> None:
        """处理请求失败回报"""
        try:
            data: dict = request.response.json()
            error_msg: str = data["ret_msg"]
            error_code: int = data["ret_code"]
            msg = f"请求失败，状态码：{request.status}，错误代码：{error_code}, 信息：{error_msg}"
        except JSONDecodeError:
            text = request.response.text
            msg = f"请求失败，信息：{text}"

        self.gateway.write_log(msg)

    def on_error(
        self,
        exception_type: type,
        exception_value: Exception,
        tb,
        request: Request
    ) -> None:
        """触发异常回报"""
        msg = f"触发异常，状态码：{exception_type}，信息：{exception_value}"
        self.gateway.write_log(msg)

        sys.stderr.write(
            self.exception_detail(exception_type, exception_value, tb, request)
        )

    def on_query_position(self, data: dict, request: Request) -> None:
        """持仓查询回报"""
        if self.check_error("查询持仓", data):
            return

        for d in data["result"]:
            if d["side"] == "Buy":
                volume = d["size"]
            else:
                volume = -d["size"]

            position: PositionData = PositionData(
                symbol=d["symbol"],
                exchange=Exchange.BYBIT,
                direction=Direction.NET,
                volume=volume,
                price=d["entry_price"],
                gateway_name=self.gateway_name
            )
            self.gateway.on_position(position)

            if not self.usdt_base:
                account: AccountData = AccountData(
                    accountid=d["symbol"].replace("USD", ""),
                    balance=d["wallet_balance"],
                    frozen=d["order_margin"],
                    gateway_name=self.gateway_name,
                )
                self.gateway.on_account(account)

    def on_query_contract(self, data: dict, request: Request) -> None:
        """合约查询回报"""
        if self.check_error("查询合约", data):
            return

        for d in data["result"]:
            self.contract_codes.add(d["name"])

            contract: ContractData = ContractData(
                symbol=d["name"],
                exchange=Exchange.BYBIT,
                name=d["name"],
                product=Product.FUTURES,
                size=1,
                pricetick=float(d["price_filter"]["tick_size"]),
                min_volume=d["lot_size_filter"]["min_trading_qty"],
                net_position=True,
                history_data=True,
                gateway_name=self.gateway_name
            )

            if self.usdt_base and "USDT" in contract.symbol:
                self.gateway.on_contract(contract)
            elif not self.usdt_base and "USDT" not in contract.symbol:
                self.gateway.on_contract(contract)

        self.gateway.write_log("合约信息查询成功")
        self.query_position()
        self.query_account()

    def on_query_account(self, data: dict, request: Request) -> None:
        """资金查询回报"""
        if self.check_error("查询账号", data):
            return

        for key, value in data["result"].items():
            account: AccountData = AccountData(
                accountid=key,
                balance=value["wallet_balance"],
                frozen=value["order_margin"],
                gateway_name=self.gateway_name,
            )
            self.gateway.on_account(account)

    def on_query_order(self, data: dict, request: Request):
        """未成交委托查询回报"""
        if self.check_error("查询委托", data):
            return

        params: dict = request.params
        symbol: str = params["symbol"]

        result: dict = data["result"]
        if not result:
            self.gateway.write_log(f"{symbol}委托信息查询成功")
            return

        if not result["data"]:
            self.gateway.write_log(f"{symbol}委托信息查询成功")
            return

        for d in result["data"]:
            orderid: str = d["order_link_id"]
            if not orderid:     # Ignore order not placed by vn.py
                continue

            if self.usdt_base:
                dt: datetime = generate_datetime(d["created_time"])
            else:
                dt: datetime = generate_datetime(d["created_at"])

            order: OrderData = OrderData(
                symbol=d["symbol"],
                exchange=Exchange.BYBIT,
                orderid=orderid,
                type=ORDER_TYPE_BYBIT2VT[d["order_type"]],
                direction=DIRECTION_BYBIT2VT[d["side"]],
                price=d["price"],
                volume=d["qty"],
                traded=d["cum_exec_qty"],
                status=STATUS_BYBIT2VT[d["order_status"]],
                datetime=dt,
                gateway_name=self.gateway_name
            )
            self.gateway.on_order(order)

        if (
            "last_page" in result
            and result["current_page"] != result["last_page"]
        ):
            self.query_order(result["current_page"] + 1)
        else:
            self.gateway.write_log(f"{symbol}委托信息查询成功")

    def query_contract(self) -> None:
        """查询合约信息"""
        self.add_request(
            "GET",
            "/v2/public/symbols",
            self.on_query_contract
        )

    def check_error(self, name: str, data: dict) -> bool:
        """回报状态检查"""
        if data["ret_code"]:
            error_code: int = data["ret_code"]
            error_msg: str = data["ret_msg"]
            msg = f"{name}失败，错误代码：{error_code}，信息：{error_msg}"
            self.gateway.write_log(msg)
            return True

        return False

    def query_account(self) -> None:
        """查询资金"""
        params: dict = {"coin": "USDT"}
        self.add_request(
            "GET",
            "/v2/private/wallet/balance",
            self.on_query_account,
            params
        )

    def query_position(self) -> None:
        """查询持仓"""
        if self.usdt_base:
            path: str = "/private/linear/position/list"
            symbols: list = symbols_usdt
        else:
            path: str = "/position/list"
            symbols: list = symbols_inverse

        for symbol in symbols:
            params: dict = {"symbol": symbol}

            self.add_request(
                "GET",
                path,
                self.on_query_position,
                params
            )

    def query_order(self, page: int = 1) -> None:
        """查询未成交委托"""
        if self.usdt_base:
            path: str = "/private/linear/order/list"
            symbols: list = symbols_usdt
        else:
            path: str = "/open-api/order/list"
            symbols: list = symbols_inverse

        for symbol in symbols:
            params: dict = {
                "symbol": symbol,
                "limit": 50,
                "page": page,
                "order_status": "New,PartiallyFilled"
            }

            self.add_request(
                "GET",
                path,
                callback=self.on_query_order,
                params=params
            )

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """查询历史数据"""
        history: list = []
        count: int = 200
        start_time: int = int(req.start.timestamp())

        if self.usdt_base:
            path: str = "/public/linear/kline"
        else:
            path: str = "/v2/public/kline/list"

        while True:
            # Create query params
            params: dict = {
                "symbol": req.symbol,
                "interval": INTERVAL_VT2BYBIT[req.interval],
                "from": start_time,
                "limit": count
            }

            # Get response from server
            resp: Response = self.request(
                "GET",
                path,
                params=params
            )

            # Break if request failed with other status code
            if resp.status_code // 100 != 2:
                msg = f"获取历史数据失败，状态码：{resp.status_code}，信息：{resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                data: dict = resp.json()

                ret_code: int = data["ret_code"]
                if ret_code:
                    ret_msg: str = data["ret_msg"]
                    msg = f"获取历史数据出错，错误信息：{ret_msg}"
                    self.gateway.write_log(msg)
                    break

                if not data["result"]:
                    msg = f"获取历史数据为空，开始时间：{start_time}，数量：{count}"
                    self.gateway.write_log(msg)
                    break

                buf: list = []
                for d in data["result"]:
                    dt: datetime = datetime.fromtimestamp(d["open_time"])
                    dt: datetime = LOCAL_TZ.localize(dt)

                    bar: BarData = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=dt,
                        interval=req.interval,
                        volume=float(d["volume"]),
                        open_price=float(d["open"]),
                        high_price=float(d["high"]),
                        low_price=float(d["low"]),
                        close_price=float(d["close"]),
                        gateway_name=self.gateway_name
                    )
                    buf.append(bar)

                history.extend(buf)

                begin: datetime = buf[0].datetime
                end: datetime = buf[-1].datetime
                msg = f"获取历史数据成功，{req.symbol} - {req.interval.value}，{begin} - {end}"
                self.gateway.write_log(msg)

                # Break if last data collected
                if len(buf) < count:
                    break

                # Update start time
                start_time: int = int((bar.datetime + TIMEDELTA_MAP[req.interval]).timestamp())

        return history


class BybitPublicWebsocketApi(WebsocketClient):
    """"""

    def __init__(self, gateway: BybitGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: BybitGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.usdt_base: bool = False

        self.callbacks: Dict[str, Callable] = {}
        self.ticks: Dict[str, TickData] = {}
        self.subscribed: Dict[str, SubscribeRequest] = {}

        self.symbol_bids: Dict[str, dict] = {}
        self.symbol_asks: Dict[str, dict] = {}

    def connect(
        self,
        usdt_base: bool,
        server: str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """连接Websocket公共频道"""
        self.usdt_base = usdt_base
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.server = server

        if self.server == "REAL":
            if usdt_base:
                url = PUBLIC_WEBSOCKET_HOST
            else:
                url = INVERSE_WEBSOCKET_HOST
        else:
            if usdt_base:
                url = TESTNET_PUBLIC_WEBSOCKET_HOST
            else:
                url = TESTNET_INVERSE_WEBSOCKET_HOST

        self.init(url, self.proxy_host, self.proxy_port)
        self.start()

    def on_connected(self) -> None:
        """连接成功回报"""
        self.gateway.write_log("行情Websocket API连接成功")

        if self.subscribed:
            for req in self.subscribed.values():
                self.subscribe(req)

    def on_disconnected(self) -> None:
        """连接断开回报"""
        self.gateway.write_log("行情Websocket API连接断开")

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.subscribed[req.symbol] = req

        tick: TickData = TickData(
            symbol=req.symbol,
            exchange=req.exchange,
            datetime=datetime.now(UTC_TZ),
            name=req.symbol,
            gateway_name=self.gateway_name
        )
        self.ticks[req.symbol] = tick

        self.subscribe_topic(f"instrument_info.100ms.{req.symbol}", self.on_tick)
        self.subscribe_topic(f"orderBookL2_25.{req.symbol}", self.on_depth)

    def subscribe_topic(
        self,
        topic: str,
        callback: Callable[[str, dict], Any]
    ) -> None:
        """订阅公共频道推送"""
        self.callbacks[topic] = callback

        req: dict = {
            "op": "subscribe",
            "args": [topic],
        }
        self.send_packet(req)

    def on_packet(self, packet: dict) -> None:
        """推送数据回报"""
        if "topic" not in packet:
            op: str = packet["request"]["op"]
            if op == "auth":
                self.on_login(packet)
        else:
            channel: str = packet["topic"]
            callback: callable = self.callbacks[channel]
            callback(packet)

    def on_error(
        self,
        exception_type: type,
        exception_value: Exception,
        tb
    ) -> None:
        """触发异常回报"""
        msg = f"触发异常，状态码：{exception_type}，信息：{exception_value}"
        self.gateway.write_log(msg)

        sys.stderr.write(self.exception_detail(
            exception_type, exception_value, tb))

    def on_tick(self, packet: dict) -> None:
        """行情推送回报"""
        topic: str = packet["topic"]
        type_: str = packet["type"]
        data: dict = packet["data"]

        symbol: str = topic.replace("instrument_info.100ms.", "")
        tick: TickData = self.ticks[symbol]

        if type_ == "snapshot":
            if not data["last_price_e4"]:           # Filter last price with 0 value
                return

            tick.last_price = int(data["last_price_e4"]) / 10000

            if self.usdt_base:
                tick.volume = int(data["volume_24h_e8"]) / 100000000
            else:
                tick.volume = int(data["volume_24h"])

            tick.datetime = generate_datetime(data["updated_at"])
        else:
            update: dict = data["update"][0]

            if "last_price_e4" in update:
                if not update["last_price_e4"]:     # Filter last price with 0 value
                    return
                tick.last_price = int(update["last_price_e4"]) / 10000

            if "volume_24h_e8" in update:
                tick.volume = int(update["volume_24h_e8"]) / 100000000
            elif "volume_24h" in update:
                tick.volume = int(update["volume_24h"])

            tick.datetime = generate_datetime(update["updated_at"])

        self.gateway.on_tick(copy(tick))

    def on_depth(self, packet: dict) -> None:
        """盘口推送回报"""
        topic: str = packet["topic"]
        type_: str = packet["type"]
        data: dict = packet["data"]

        # Update depth data into dict buf
        symbol: str = topic.replace("orderBookL2_25.", "")
        tick: TickData = self.ticks[symbol]
        bids: dict = self.symbol_bids.setdefault(symbol, {})
        asks: dict = self.symbol_asks.setdefault(symbol, {})

        if type_ == "snapshot":
            if self.usdt_base:
                buf: list = data["order_book"]
            else:
                buf: list = data

            for d in buf:
                price: float = float(d["price"])

                if d["side"] == "Buy":
                    bids[price] = d
                else:
                    asks[price] = d
        else:
            for d in data["delete"]:
                price: float = float(d["price"])
                if d["side"] == "Buy":
                    bids.pop(price)
                else:
                    asks.pop(price)

            for d in (data["update"] + data["insert"]):
                price: float = float(d["price"])
                if d["side"] == "Buy":
                    bids[price] = d
                else:
                    asks[price] = d

        # Calculate 1-5 bid/ask depth
        bid_keys: list = list(bids.keys())
        bid_keys.sort(reverse=True)

        ask_keys: list = list(asks.keys())
        ask_keys.sort()

        for i in range(5):
            n = i + 1

            bid_price = bid_keys[i]
            bid_data = bids[bid_price]
            ask_price = ask_keys[i]
            ask_data = asks[ask_price]

            setattr(tick, f"bid_price_{n}", bid_price)
            setattr(tick, f"bid_volume_{n}", bid_data["size"])
            setattr(tick, f"ask_price_{n}", ask_price)
            setattr(tick, f"ask_volume_{n}", ask_data["size"])

        if tick.datetime:
            self.gateway.on_tick(copy(tick))


class BybitPrivateWebsocketApi(WebsocketClient):
    """"""

    def __init__(self, gateway: BybitGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: BybitGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: bytes = b""
        self.server: str = ""  # REAL or TESTNET
        self.usdt_base: bool = False

        self.callbacks: Dict[str, Callable] = {}
        self.ticks: Dict[str, TickData] = {}
        self.subscribed: Dict[str, SubscribeRequest] = {}

        self.symbol_bids: Dict[str, dict] = {}
        self.symbol_asks: Dict[str, dict] = {}

    def connect(
        self,
        usdt_base: bool,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """连接Websocket私有频道"""
        self.usdt_base = usdt_base
        self.key = key
        self.secret = secret.encode()
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.server = server

        if self.server == "REAL":
            if usdt_base:
                url = PRIVATE_WEBSOCKET_HOST
            else:
                url = INVERSE_WEBSOCKET_HOST
        else:
            if usdt_base:
                url = TESTNET_PRIVATE_WEBSOCKET_HOST
            else:
                url = TESTNET_INVERSE_WEBSOCKET_HOST

        self.init(url, self.proxy_host, self.proxy_port)
        self.start()

    def login(self) -> None:
        """用户登录"""
        expires: int = generate_timestamp(30)
        msg = f"GET/realtime{int(expires)}"
        signature: str = sign(self.secret, msg.encode())

        req: dict = {
            "op": "auth",
            "args": [self.key, expires, signature]
        }
        self.send_packet(req)

    def subscribe_topic(
        self,
        topic: str,
        callback: Callable[[str, dict], Any]
    ) -> None:
        """订阅私有频道"""
        self.callbacks[topic] = callback

        req: dict = {
            "op": "subscribe",
            "args": [topic],
        }
        self.send_packet(req)

    def on_connected(self) -> None:
        """连接成功回报"""
        self.gateway.write_log("交易Websocket API连接成功")
        self.login()

    def on_disconnected(self) -> None:
        """连接断开回报"""
        self.gateway.write_log("交易Websocket API连接断开")

    def on_packet(self, packet: dict) -> None:
        """推送数据回报"""
        if "topic" not in packet:
            op: str = packet["request"]["op"]
            if op == "auth":
                self.on_login(packet)
        else:
            channel: str = packet["topic"]
            callback: callable = self.callbacks[channel]
            callback(packet)

    def on_error(
        self,
        exception_type: type,
        exception_value: Exception,
        tb
    ) -> None:
        """触发异常回报"""
        msg = f"触发异常，状态码：{exception_type}，信息：{exception_value}"
        self.gateway.write_log(msg)

        sys.stderr.write(self.exception_detail(
            exception_type, exception_value, tb))

    def on_login(self, packet: dict):
        """用户登录请求回报"""
        success: bool = packet.get("success", False)
        if success:
            self.gateway.write_log("交易Websocket API登录成功")

            self.subscribe_topic("order", self.on_order)
            self.subscribe_topic("execution", self.on_trade)
            self.subscribe_topic("position", self.on_position)

            if self.usdt_base:
                self.subscribe_topic("wallet", self.on_account)
        else:
            self.gateway.write_log("交易Websocket API登录失败")

    def on_account(self, packet: dict) -> None:
        """资金更新推送"""
        for d in packet["data"]:
            account: AccountData = AccountData(
                accountid="USDT",
                balance=d["wallet_balance"],
                frozen=d["wallet_balance"] - d["available_balance"],
                gateway_name=self.gateway_name,
            )
            self.gateway.on_account(account)

    def on_trade(self, packet: dict) -> None:
        """成交更新推送"""
        for d in packet["data"]:
            orderid: str = d["order_link_id"]
            if not orderid:
                orderid: str = d["order_id"]

            trade: TradeData = TradeData(
                symbol=d["symbol"],
                exchange=Exchange.BYBIT,
                orderid=orderid,
                tradeid=d["exec_id"],
                direction=DIRECTION_BYBIT2VT[d["side"]],
                price=float(d["price"]),
                volume=d["exec_qty"],
                datetime=generate_datetime(d["trade_time"]),
                gateway_name=self.gateway_name,
            )

            self.gateway.on_trade(trade)

    def on_order(self, packet: dict) -> None:
        """委托更新推送"""
        for d in packet["data"]:
            if self.usdt_base:
                dt: datetime = generate_datetime(d["create_time"])
            else:
                dt: datetime = generate_datetime(d["timestamp"])

            order: OrderData = OrderData(
                symbol=d["symbol"],
                exchange=Exchange.BYBIT,
                orderid=d["order_link_id"],
                type=ORDER_TYPE_BYBIT2VT[d["order_type"]],
                direction=DIRECTION_BYBIT2VT[d["side"]],
                price=float(d["price"]),
                volume=d["qty"],
                traded=d["cum_exec_qty"],
                status=STATUS_BYBIT2VT[d["order_status"]],
                datetime=dt,
                gateway_name=self.gateway_name
            )

            self.gateway.on_order(order)

    def on_position(self, packet: dict) -> None:
        """持仓更新推送"""
        for d in packet["data"]:
            if d["side"] == "Buy":
                volume = d["size"]
            else:
                volume = -d["size"]

            position: PositionData = PositionData(
                symbol=d["symbol"],
                exchange=Exchange.BYBIT,
                direction=Direction.NET,
                volume=volume,
                price=float(d["entry_price"]),
                gateway_name=self.gateway_name
            )
            self.gateway.on_position(position)


def generate_timestamp(expire_after: float = 30) -> int:
    """生成时间戳"""
    return int(time.time() * 1000 + expire_after * 1000)


def sign(secret: bytes, data: bytes) -> str:
    """生成签名"""
    return hmac.new(
        secret, data, digestmod=hashlib.sha256
    ).hexdigest()


def generate_datetime(timestamp: str) -> datetime:
    """生成时间"""
    if "." in timestamp:
        part1, part2 = timestamp.split(".")
        if len(part2) > 7:
            part2 = part2[:6] + "Z"
            timestamp = ".".join([part1, part2])

        dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    else:
        dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    dt = UTC_TZ.localize(dt)
    return dt
