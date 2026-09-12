from numpy.random import f
import pandas as pd
import numpy as np
import time
import random
import re
import akshare as ak
import os
import warnings
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header
from typing import Dict, List, Optional, Any, Tuple
from concurrent.futures import ThreadPoolExecutor
import datetime

try:
    from czsc import CZSC, Freq, RawBar
    from czsc.signals.cxt import (
        cxt_bi_trend_V230824,
        cxt_bi_base_V230228,
        cxt_five_bi_V230619,
        cxt_first_buy_V221126
    )

    CZSC_AVAILABLE = True
except ImportError:
    print("[WARN] czsc 未安装，趋势状态层将使用简化计算")
    CZSC_AVAILABLE = False

warnings.filterwarnings("ignore")

# ==================== 配置区 ====================
_API_KEY1 = "tk_81a9c96173cd4a1c889595fdc2822520"
_API_KEY2 = "tk_112266c24f1446e69f26748f7c24decd"
_API_KEY3 = "tk_bf02d28504ea4e0582361055c6634a17"
_API_KEY4 = "tk_6b3960e6a0fc4cd5a6ea044d1a4114b7"

_tf1 = None
_tf2 = None
_tf3 = None
_tf4 = None
try:
    from tickflow import TickFlow

    _tf1 = TickFlow(api_key=_API_KEY1)
    _tf2 = TickFlow(api_key=_API_KEY2)
    _tf3 = TickFlow(api_key=_API_KEY3)
    _tf4 = TickFlow(api_key=_API_KEY4)
except ImportError:
    print("[WARN] TickFlow 未安装，ETF 分析将跳过")

SMTP_SERVER = 'smtp.qq.com'
SMTP_PORT = 465
EMAIL_USER = '778988525@qq.com'
EMAIL_PASS = 'plhjfrrygodkbeed'
EMAIL_TO_LIST = ['2032018426@qq.com']
# EMAIL_TO_LIST = ['2032018426@qq.com', '778988525@qq.com', '543276389@qq.com', '1462139311@qq.com', 'lqn0609@126.com',
#                  '124024760@163.com', '916029848@qq.com', '616816004@qq.com', '522800656@qq.com', '851078218@qq.com',
#                  '404767971@qq.com', '287577117@qq.com', '542063354@qq.com', '657311884@qq.com', '398912563@qq.com',
#                  '295086367@qq.com', '442106482@qq.com', '561688991@qq.com', '454869448@qq.com', '657280091@qq.com',
#                  '564561227@qq.com', '8524602@qq.com', '1650685058@qq.com', '418561464@qq.com', '104126261@qq.com',
#                  '183477660@qq.com', '326667158@qq.com', '649556111@qq.com', 'chenbaihang28@163.com',
#                  '147434754@qq.com','812977495@qq.com']


# ==================== 数据层：股票数据获取 ====================

class DataLayer:
    """数据层：负责获取和预处理股票数据"""

    STOCK_POOL = {
        # === 银行板块：区分稳健型与进攻型 ===
        # 四大行：波动极小，很难触碰月线下轨，以 PB 分位控制安全边际
        "601988.SH": {"name": "中国银行", "type": "stock", "calc_dy": True, "pb_threshold": 25, "month_zone": "mid"},
        "601288.SH": {"name": "农业银行", "type": "stock", "calc_dy": True, "pb_threshold": 25, "month_zone": "mid"},
        "601939.SH": {"name": "建设银行", "type": "stock", "calc_dy": True, "pb_threshold": 25, "month_zone": "mid"},
        "601398.SH": {"name": "工商银行", "type": "stock", "calc_dy": True, "pb_threshold": 25, "month_zone": "mid"},
        # 优质城商/股份行：波动较大，必须守在下轨，防范估值中枢下移
        "600036.SH": {"name": "招商银行", "type": "stock", "calc_dy": True, "pb_threshold": 30, "month_zone": "lower"},
        "600919.SH": {"name": "江苏银行", "type": "stock", "calc_dy": True, "pb_threshold": 30, "month_zone": "lower"},
        "601166.SH": {"name": "兴业银行", "type": "stock", "calc_dy": True, "pb_threshold": 30, "month_zone": "lower"},
        "002142.SZ": {"name": "宁波银行", "type": "stock", "calc_dy": True, "pb_threshold": 20, "month_zone": "lower"},
        "601838.SH": {"name": "成都银行", "type": "stock", "calc_dy": True, "pb_threshold": 30, "month_zone": "lower"},

        # === 保险：强beta属性，坚守下轨 ===
        "601318.SH": {"name": "中国平安", "type": "stock", "calc_dy": True, "pb_threshold": 20, "month_zone": "lower"},

        # === 公用事业/电信：红利资产的锚，防范高位回撤 ===
        # 长电这类股票，只有下轨才具备真正的“安全买点”
        "600900.SH": {"name": "长江电力", "type": "stock", "calc_dy": True, "pb_threshold": 20, "month_zone": "mid"},
        "600886.SH": {"name": "国投电力", "type": "stock", "calc_dy": True, "pb_threshold": 15, "month_zone": "lower"},
        "600795.SH": {"name": "国电电力", "type": "stock", "calc_dy": True, "pb_threshold": 15, "month_zone": "lower"},
        "601985.SH": {"name": "中国核电", "type": "stock", "calc_dy": True, "pb_threshold": 20, "month_zone": "lower"},
        "003816.SZ": {"name": "中国广核", "type": "stock", "calc_dy": True, "pb_threshold": 15, "month_zone": "lower"},
        "600941.SH": {"name": "中国移动", "type": "stock", "calc_dy": True, "pb_threshold": 20, "month_zone": "lower"},
        "601728.SH": {"name": "中国电信", "type": "stock", "calc_dy": True, "pb_threshold": 20, "month_zone": "lower"},

        # === 能源：周期性极强，绝不追高 ===
        "600028.SH": {"name": "中国石化", "type": "stock", "calc_dy": True, "pb_threshold": 20, "month_zone": "lower"},
        "601857.SH": {"name": "中国石油", "type": "stock", "calc_dy": True, "pb_threshold": 20, "month_zone": "lower"},
        "600938.SH": {"name": "中国海油", "type": "stock", "calc_dy": True, "pb_threshold": 20, "month_zone": "lower"},
        "601088.SH": {"name": "中国神华", "type": "stock", "calc_dy": True, "pb_threshold": 15, "month_zone": "lower"},
        "601225.SH": {"name": "陕西煤业", "type": "stock", "calc_dy": True, "pb_threshold": 15, "month_zone": "lower"},
        "600011.SH": {"name": "华能国际", "type": "stock", "calc_dy": True, "pb_threshold": 20, "month_zone": "lower"},

        # === 消费/白马：逻辑修复中，宁可错过不可做错 ===
        "600519.SH": {"name": "贵州茅台", "type": "stock", "calc_dy": True, "pb_threshold": 20, "month_zone": "lower"},
        "000858.SZ": {"name": "五粮液", "type": "stock", "calc_dy": True, "pb_threshold": 15, "month_zone": "lower"},
        "000333.SZ": {"name": "美的集团", "type": "stock", "calc_dy": True, "pb_threshold": 25, "month_zone": "lower"},
        "000651.SZ": {"name": "格力电器", "type": "stock", "calc_dy": True, "pb_threshold": 25, "month_zone": "lower"},
        "600690.SH": {"name": "海尔智家", "type": "stock", "calc_dy": True, "pb_threshold": 20, "month_zone": "lower"},
        "600887.SH": {"name": "伊利股份", "type": "stock", "calc_dy": True, "pb_threshold": 20, "month_zone": "lower"},
        "000895.SZ": {"name": "双汇发展", "type": "stock", "calc_dy": True, "pb_threshold": 20, "month_zone": "lower"},
        "002714.SZ": {"name": "牧原股份", "type": "stock", "calc_dy": True, "pb_threshold": 25, "month_zone": "lower"},

        # === 制造与周期：高波动品种，严格执行 lower ===
        "601919.SH": {"name": "中远海控", "type": "stock", "calc_dy": True, "pb_threshold": None,
                      "month_zone": "lower"},
        "002594.SZ": {"name": "比亚迪", "type": "stock", "calc_dy": True, "pb_threshold": 15, "month_zone": "lower"},

        # === 医药与医药红利 ===
        "000538.SZ": {"name": "云南白药", "type": "stock", "calc_dy": True, "pb_threshold": 15, "month_zone": "lower"},
        "600750.SH": {"name": "华润江中", "type": "stock", "calc_dy": True, "pb_threshold": 15, "month_zone": "lower"},
        "000423.SZ": {"name": "东阿阿胶", "type": "stock", "calc_dy": True, "pb_threshold": 20, "month_zone": "lower"},
    }

    ETF_POOL = {
        "513530.SH": {"name": "港股红利ETF", "type": "etf", "calc_dy": True, "pb_threshold": None, "month_zone": "mid"},
        "159941.SZ": {"name": "纳指ETF", "type": "etf", "calc_dy": False, "pb_threshold": None, "month_zone": "mid"},
    }

    @staticmethod
    def get_price_data(symbol: str, info: dict) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """获取日线、周线、月线数据"""
        df_d, df_w, df_m = None, None, None

        tf = info.get('tf_instance', _tf1)
        if tf is None:
            print(f"[WARN] TickFlow 未初始化，跳过 ETF: {symbol}")
            return None, None, None
        try:
            end = int(datetime.datetime.now().timestamp() * 1000)
            df_d_raw = tf.klines.get(symbol, period="1d", count=3000, end_time=end, adjust="forward_additive",
                                     as_dataframe=True)
            df_w_raw = tf.klines.get(symbol, period="1w", count=600, end_time=end, adjust="forward_additive",
                                     as_dataframe=True)
            df_m_raw = tf.klines.get(symbol, period="1M", count=240, end_time=end, adjust="forward_additive",
                                     as_dataframe=True)
            for rdf in [df_d_raw, df_w_raw, df_m_raw]:
                if rdf is not None and not rdf.empty:
                    rdf.columns = [c.lower() for c in rdf.columns]
                    if 'trade_date' not in rdf.columns:
                        if 'datetime' in rdf.columns:
                            rdf['trade_date'] = pd.to_datetime(rdf['datetime'])
                        elif 'date' in rdf.columns:
                            rdf['trade_date'] = pd.to_datetime(rdf['date'])
                        else:
                            rdf['trade_date'] = pd.date_range(end=pd.Timestamp.now(), periods=len(rdf), freq='D')
                    else:
                        rdf['trade_date'] = pd.to_datetime(rdf['trade_date'])
            df_d, df_w, df_m = df_d_raw, df_w_raw, df_m_raw
        except Exception as e:
            print(f"[WARN] ETF数据获取失败 {symbol}: {e}")
            return None, None, None

        if df_d is not None and not df_d.empty:
            df_d = df_d.sort_values('trade_date').reset_index(drop=True)
        if df_w is not None and not df_w.empty:
            df_w = df_w.sort_values('trade_date').reset_index(drop=True)
        if df_m is not None and not df_m.empty:
            df_m = df_m.sort_values('trade_date').reset_index(drop=True)

        return df_d, df_w, df_m

    @staticmethod
    def _parse_dividend_per_share(plan: str) -> float:
        if not isinstance(plan, str):
            return 0.0
        match = re.search(r"10派([\d\.]+)元", plan)
        return float(match.group(1)) / 10.0 if match else 0.0

    @staticmethod
    def _parse_report_period(report_str: str) -> Optional[str]:
        if not isinstance(report_str, str) or report_str.strip() == "":
            return None
        s = report_str.strip()
        if "年报" in s:
            return f"{s[:4]}Q4"
        if "中报" in s or "半年报" in s:
            return f"{s[:4]}Q2"
        if "一季报" in s:
            return f"{s[:4]}Q1"
        if "三季报" in s:
            return f"{s[:4]}Q3"
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
        if m:
            dt = pd.Timestamp(s)
            if dt.month <= 4:
                return f"{dt.year - 1}Q4"
            elif dt.month <= 8:
                return f"{dt.year}Q2"
            else:
                return f"{dt.year}Q3"
        return None

    @staticmethod
    def get_stock_dividend_yield_ttm(stock_code: str, current_price: float) -> float:
        """获取股票TTM股息率"""
        try:
            clean_code = stock_code.split('.')[0]
            df = ak.stock_fhps_detail_ths(symbol=clean_code)
            if df.empty:
                return 0.0
            df["每股分红"] = df["分红方案说明"].apply(DataLayer._parse_dividend_per_share)
            valid_mask = (df["每股分红"] > 0) & (df["方案进度"].str.contains("实施|预案", na=False))
            df["归属期"] = df.apply(
                lambda r: DataLayer._parse_report_period(str(r.get("报告期", ""))) if valid_mask.loc[r.name] else None,
                axis=1)
            valid = df[valid_mask & df["归属期"].notna()].sort_values("董事会日期", ascending=False)
            if valid.empty:
                return 0.0
            lp = valid.iloc[0]["归属期"]
            year, q = int(lp[:4]), int(lp[5])
            periods = []
            for _ in range(4):
                periods.append(f"{year}Q{q}")
                q -= 1
                if q == 0:
                    year -= 1
                    q = 4
            total_div = 0.0
            for _, row in df.iterrows():
                if "实施" not in str(row.get("方案进度", "")) and "预案" not in str(row.get("方案进度", "")):
                    continue
                period = DataLayer._parse_report_period(str(row.get("报告期", "")))
                if period in periods:
                    total_div += row["每股分红"]
            return round(total_div / current_price * 100, 2) if current_price > 0 else 0.0
        except:
            return 0.0

    @staticmethod
    def get_recent_dividend(stock_code: str) -> float:
        """获取最近每股分红金额"""
        try:
            clean_code = stock_code.split('.')[0]
            df = ak.stock_fhps_detail_ths(symbol=clean_code)
            if df.empty:
                return 0.0
            df["每股分红"] = df["分红方案说明"].apply(DataLayer._parse_dividend_per_share)
            valid_mask = (df["每股分红"] > 0) & (df["方案进度"].str.contains("实施|预案", na=False))
            df_valid = df[valid_mask].sort_values("董事会日期", ascending=False)
            if df_valid.empty:
                return 0.0

            total_div = 0.0
            lp = DataLayer._parse_report_period(str(df_valid.iloc[0].get("报告期", "")))
            if lp:
                year, q = int(lp[:4]), int(lp[5])
                periods = []
                for _ in range(4):
                    periods.append(f"{year}Q{q}")
                    q -= 1
                    if q == 0:
                        year -= 1
                        q = 4
                for _, row in df_valid.iterrows():
                    period = DataLayer._parse_report_period(str(row.get("报告期", "")))
                    if period in periods:
                        total_div += row["每股分红"]

            if total_div > 0:
                return round(total_div, 4)
            return round(df_valid.iloc[0]["每股分红"], 4)
        except:
            return 0.0

    @staticmethod
    def calculate_etf_dividend(symbol: str) -> float:
        """计算ETF股息率"""
        try:
            clean_symbol = symbol.split('.')[0]
            df_net = ak.fund_open_fund_info_em(symbol=clean_symbol, indicator="单位净值走势")
            latest_nv = df_net.tail(1)['单位净值'].values[0]
            df_div = ak.fund_open_fund_info_em(symbol=clean_symbol, indicator="分红送配详情")
            total_div = df_div.head(12)["每份分红"].str.extract(r'(\d+\.?\d*)').astype(float).sum()[0]
            return round((total_div / latest_nv) * 100, 4)
        except:
            return 0.0

    @staticmethod
    def get_pb_analysis(symbol: str) -> Optional[dict]:
        """获取PB分析数据"""
        try:
            clean_code = "".join(filter(str.isdigit, symbol))
            df = ak.stock_value_em(symbol=clean_code)
            if df.empty:
                return None
            pb_col = next((col for col in ['市净率', 'PB', 'PB_最新'] if col in df.columns), None)
            if pb_col is None:
                return None
            df[pb_col] = pd.to_numeric(df[pb_col], errors='coerce')
            df = df.dropna(subset=[pb_col])
            pb_series = df[pb_col]
            current_pb = pb_series.iloc[-1]
            percentile = (pb_series < current_pb).mean() * 100
            status = "低估" if percentile < 20 else ("合理" if percentile < 70 else "高估")
            return {
                "current_pb": round(current_pb, 3),
                "percentile": round(percentile, 2),
                "pb_min": round(pb_series.min(), 3),
                "pb_median": round(pb_series.median(), 3),
                "status": status
            }
        except Exception as e:
            print(f"[ERROR] PB分析失败 {symbol}: {e}")
            return None


# ==================== 指标层：技术指标计算 ====================

class IndicatorLayer:
    """指标层：负责计算各类技术指标"""

    @staticmethod
    def calc_rsi(close: np.array, period: int = 14) -> float:
        """计算RSI指标"""
        if len(close) < period + 1:
            return 50.0

        delta = np.diff(close)
        gains = np.where(delta > 0, delta, 0)
        losses = np.where(delta < 0, -delta, 0)

        avg_gain = np.zeros_like(gains)
        avg_loss = np.zeros_like(losses)

        avg_gain[period - 1] = np.mean(gains[:period])
        avg_loss[period - 1] = np.mean(losses[:period])

        alpha = 1.0 / period
        for i in range(period, len(gains)):
            avg_gain[i] = alpha * gains[i] + (1 - alpha) * avg_gain[i - 1]
            avg_loss[i] = alpha * losses[i] + (1 - alpha) * avg_loss[i - 1]

        rs = avg_gain / (avg_loss + 1e-9)
        rsi = 100 - (100 / (1 + rs))

        return round(float(rsi[-1]), 2)

    @staticmethod
    def calc_macd(close: np.array, fast: int = 12, slow: int = 26, signal: int = 9) -> dict:
        """计算MACD指标"""
        ema_fast = pd.Series(close).ewm(span=fast, adjust=False).mean().values
        ema_slow = pd.Series(close).ewm(span=slow, adjust=False).mean().values
        diff = ema_fast - ema_slow
        dea = pd.Series(diff).ewm(span=signal, adjust=False).mean().values
        hist = (diff - dea) * 2
        return {
            'dif': float(diff[-1]),
            'dea': float(dea[-1]),
            'hist': float(hist[-1]),
            'prev_hist': float(hist[-2]) if len(hist) > 1 else 0
        }

    @staticmethod
    def calc_boll(close: np.array, period: int = 20, std_dev: int = 2) -> dict:
        """计算布林带指标"""
        if len(close) < period:
            return {'mid': float(close[-1]), 'lower': float(close[-1]), 'upper': float(close[-1])}
        mid = pd.Series(close).rolling(period).mean().values
        std = pd.Series(close).rolling(period).std().values
        upper = mid + std_dev * std
        lower = mid - std_dev * std
        return {
            'mid': float(mid[-1]),
            'lower': float(lower[-1]),
            'upper': float(upper[-1])
        }

    @staticmethod
    def calc_ma(close: np.array, periods: list = [5, 20, 60, 120, 250]) -> dict:
        """计算均线指标"""
        result = {}
        for p in periods:
            if len(close) >= p:
                result[f'ma{p}'] = float(pd.Series(close).rolling(p).mean().values[-1])
            else:
                result[f'ma{p}'] = float(close[-1])
        return result

    @staticmethod
    def get_ma_status(close: float, ma: float) -> str:
        """获取均线状态"""
        if ma <= 0:
            return "○(无效)"
        ratio = close / ma
        if ratio <= 0.98:
            return "✔(跌破)"
        elif ratio >= 1.02:
            return "✗(远离)"
        else:
            return "○(附近)"

    @staticmethod
    def get_boll_status(close: float, mid: float, lower: float) -> str:
        """获取布林带状态"""
        if mid <= lower:
            return "○(无效)"
        if close <= lower:
            return "✔(下轨)"
        elif close <= (mid + lower) / 2:
            return "○(接近下轨)"
        elif close <= mid:
            return "○(中轨下)"
        else:
            return "✗(中轨上)"

    @staticmethod
    def get_rsi_status(rsi: float) -> str:
        """获取RSI状态"""
        if rsi <= 20:
            return "✔(极度超卖)"
        if rsi <= 30:
            return "✔(超卖)"
        if rsi <= 35:
            return "✔(偏弱)"
        elif rsi >= 80:
            return "✗(超买)"
        else:
            return "○(正常)"

    @staticmethod
    def get_box_range(close: np.array, period: int = 120) -> dict:
        """识别箱体区间 - 近 period 根日线的分位数箱体

        用 10%/90% 分位数而非最高/最低价，避免单根插针把箱体撑开；
        取近 period 根而非全部历史，保证箱体反映的是当前的震荡区间。
        """
        if close is None or len(close) < 10:
            last = float(close[-1]) if close is not None and len(close) else 0.0
            print(f"[ERROR] 箱体区间计算失败: 日线数据不足({0 if close is None else len(close)}根)")
            return {"top": last, "bottom": last, "width": 0.0}

        window = np.asarray(close[-period:], dtype=float)
        window = window[np.isfinite(window)]
        if len(window) < 10:
            last = float(close[-1])
            print(f"[ERROR] 箱体区间计算失败: 有效日线不足({len(window)}根)")
            return {"top": last, "bottom": last, "width": 0.0}

        bottom = round(float(np.percentile(window, 10)), 3)
        top = round(float(np.percentile(window, 90)), 3)

        if top <= bottom:
            # 极端横盘（几乎无波动），退化为窗口最高/最低价
            bottom, top = round(float(window.min()), 3), round(float(window.max()), 3)

        width = round((top - bottom) / bottom * 100, 2) if bottom > 0 else 0.0

        return {
            "top": top,
            "bottom": bottom,
            "width": width,
            "period": int(len(window))
        }


# ==================== 评分层：五层评分体系 ====================
class ScoringLayer:
    """评分层：五层评分体系核心逻辑"""

    # ========== 第一层：价值层 (30分) ==========
    class ValueScorer:
        """价值层评分器"""

        @staticmethod
        def calc_dividend_score(dy_val: float, dy_target: float) -> Tuple[float, str]:
            """动态目标股息率评分 (20分)"""
            if dy_val <= 0 or dy_target <= 0:
                return 0, f"0/20 (无数据)"

            ratio = dy_val / dy_target
            if ratio >= 1.0:
                score = 20
            elif ratio >= 0.95:
                score = 18
            elif ratio >= 0.90:
                score = 16
            elif ratio >= 0.85:
                score = 14
            elif ratio >= 0.80:
                score = 12
            elif ratio >= 0.70:
                score = 8
            elif ratio >= 0.60:
                score = 4
            else:
                score = 0

            return score, f"{score}/20 ({dy_val:.2f}%/{dy_target:.2f}%)"

        @staticmethod
        def calc_pb_score(symbol: str, pb_threshold: int) -> Tuple[float, str]:
            """PB分位评分 (10分)"""
            if pb_threshold is None:
                return 0, "0/10 (N/A)"

            pb_data = DataLayer.get_pb_analysis(symbol)
            if pb_data is None:
                return 0, "0/10 (数据缺失)"

            percentile = pb_data["percentile"]
            if percentile <= pb_threshold:
                score = 10
            elif percentile >= 70:
                score = 0
            else:
                score = round(10 * (70 - percentile) / (70 - pb_threshold), 1)

            return score, f"{score}/10 ({percentile:.1f}% [{pb_data['status']}])"

        @classmethod
        def total_score(cls, symbol: str, info: dict, dy_val: float, dy_target: float = 0.0) -> dict:
            """价值层总分"""
            dy_score, dy_detail = cls.calc_dividend_score(dy_val, dy_target)
            pb_score, pb_detail = cls.calc_pb_score(symbol, info.get("pb_threshold"))

            # 获取PB分位数据（仅对股票有效）
            pb_percentile = '-'
            pb_threshold = info.get("pb_threshold")
            if pb_threshold is not None:
                try:
                    pb_data = DataLayer.get_pb_analysis(symbol)
                    if pb_data is not None:
                        pb_percentile = f"{pb_data['percentile']:.1f}% [{pb_data['status']}]"
                except Exception as e:
                    print(f"[ERROR] PB分析失败 {symbol}: {e}")

            return {
                'score': dy_score + pb_score,
                'max_score': 30,
                'dividend': {'score': dy_score, 'detail': dy_detail},
                'pb': {'score': pb_score, 'detail': pb_detail},
                'info': {'pb_percentile': pb_percentile}
            }

    # ========== 第二层：位置层 (20分) ==========
    class PositionScorer:
        """位置层评分器"""

        @staticmethod
        def calc_box_position_score(close: float, box_bottom: float, box_top: float) -> Tuple[float, str]:
            """箱体位置评分 (7分) - 箱内线性打分，箱底满分、箱顶0分"""
            if box_top <= box_bottom:
                return 0, "0/7 (箱体无效)"

            position = (close - box_bottom) / (box_top - box_bottom) * 100
            clamped = min(max(position, 0.0), 100.0)  # 破箱底封顶满分，冲出箱顶封底0分
            score = round(7 * (1 - clamped / 100), 1)

            if position <= 0:
                desc = "跌破箱底"
            elif position <= 25:
                desc = "箱体低位"
            elif position <= 50:
                desc = "箱体中下"
            elif position <= 75:
                desc = "箱体中上"
            elif position <= 100:
                desc = "箱体高位"
            else:
                desc = "突破箱顶"

            return score, f"{score}/7 ({desc} {position:.1f}%)"

        @staticmethod
        def calc_bias250_score(close: float, ma250: float) -> Tuple[float, str]:
            """MA250偏离率Bias评分 (5分) - 长期偏离"""
            if ma250 <= 0:
                return 0, "0/5 (MA250无效)", {}

            bias = (close - ma250) / ma250 * 100

            if bias <= -35:
                score = 5
                desc = "极端低估"
            elif bias <= -25:
                score = 4
                desc = "深价值区"
            elif bias <= -15:
                score = 3
                desc = "偏低"
            elif bias <= -5:
                score = 2
                desc = "正常偏低"
            elif bias <= 5:
                score = 1
                desc = "均值附近"
            else:
                score = 0
                desc = "明显高位"

            return score, f"{score}/5 ({desc} {bias:.1f}%)", {'close': close, 'ma250': ma250, 'bias': bias}

        @classmethod
        def calc_weekly_zhongshu_score(cls, df_weekly: pd.DataFrame) -> Tuple[float, str]:
            """CZSC中枢位置评分 (8分) - 周线级别160根K线"""
            if not CZSC_AVAILABLE or df_weekly is None:
                return 0, "0/8 (czsc不可用)"

            min_bars = 160
            if len(df_weekly) < min_bars:
                return 0, f"0/8 (周线K线不足: {len(df_weekly)} < {min_bars})"

            try:
                from czsc.signals.cxt import get_zs_seq

                df = df_weekly.copy()
                if 'dt' not in df.columns:
                    df['dt'] = pd.to_datetime(df['trade_date'])

                bars = []
                for i, row in df.iterrows():
                    bar = RawBar(
                        symbol=row.get('ts_code', row.get('symbol', 'unknown')),
                        id=i,
                        dt=row['dt'],
                        freq=Freq.W,
                        open=float(row['open']),
                        close=float(row['close']),
                        high=float(row['high']),
                        low=float(row['low']),
                        vol=float(row.get('vol', 0)),
                        amount=float(row.get('amount', 0))
                    )
                    bars.append(bar)

                c = CZSC(bars)

                if not hasattr(c, 'bi_list') or not c.bi_list:
                    print(f"[ERROR] 周线中枢计算失败: CZSC对象没有bi_list属性或bi_list为空")
                    return 0, "0/8 (无笔数据)"

                zs_list = get_zs_seq(c.bi_list)

                if not zs_list:
                    print(f"[ERROR] 周线中枢计算失败: get_zs_seq返回空列表")
                    return 0, "0/8 (无中枢)"

                zs = zs_list[-1]
                zd = zs.zd
                zg = zs.zg
                zz = zs.zz
                close = float(df_weekly.iloc[-1]['close'])

                if zg <= zd:
                    print(f"[ERROR] 周线中枢计算失败: zd={zd:.2f}, zg={zg:.2f}, 中枢无效")
                    return 0, "0/8 (中枢无效)"

                pos = (close - zd) / (zg - zd) * 100

                if pos <= 10:
                    score = 8
                    desc = "中枢下沿"
                elif pos <= 20:
                    score = 7
                    desc = "靠近下沿"
                elif pos <= 30:
                    score = 6
                    desc = "偏低"
                elif pos <= 40:
                    score = 5
                    desc = "中下"
                elif pos <= 50:
                    score = 4
                    desc = "中位"
                elif pos <= 60:
                    score = 3
                    desc = "中上"
                elif pos <= 70:
                    score = 2
                    desc = "偏高"
                elif pos <= 80:
                    score = 1
                    desc = "靠近上沿"
                else:
                    score = 0
                    desc = "中枢上沿"

                return score, f"{score}/8 ({desc} {pos:.1f}%)", {'zd': zd, 'zg': zg, 'zz': zz, 'close': close,
                                                                 'pos': pos, 'zs_count': len(zs_list)}

            except Exception as e:
                print(f"[ERROR] 周线中枢计算失败: {type(e).__name__}: {e}")
                return 0, "0/8 (计算失败)", {}

        @classmethod
        def total_score(cls, close: float, box_bottom: float, box_top: float,
                        ma250: float, df_weekly: pd.DataFrame = None) -> dict:
            """位置层总分 (20分)"""
            box_score, box_detail = cls.calc_box_position_score(close, box_bottom, box_top)
            zhongshu_result = cls.calc_weekly_zhongshu_score(df_weekly)
            bias250_result = cls.calc_bias250_score(close, ma250)

            zhongshu_score, zhongshu_detail, zhongshu_info = zhongshu_result if len(zhongshu_result) == 3 else (
                zhongshu_result[0], zhongshu_result[1], {})
            bias250_score, bias250_detail, bias250_info = bias250_result if len(bias250_result) == 3 else (
                bias250_result[0], bias250_result[1], {})

            return {
                'score': box_score + zhongshu_score + bias250_score,
                'max_score': 20,
                'box': {'score': box_score, 'detail': box_detail, 'close': close, 'bottom': box_bottom, 'top': box_top,
                        'max_score': 7},
                'weekly_zhongshu': {'score': zhongshu_score, 'detail': zhongshu_detail, 'info': zhongshu_info},
                'bias250': {'score': bias250_score, 'detail': bias250_detail, 'info': bias250_info}
            }

    # ========== 第三层：动能层 (25分) ==========
    class MomentumScorer:
        """动能层评分器"""

        @staticmethod
        def _boll_linear_score(close: float, boll_mid: float, boll_lower: float, max_score: float,
                               prefix: str = "") -> Tuple[float, str, float]:
            """BOLL带内线性打分：上轨0分 -> 中轨半分 -> 下轨满分（跌破下轨封顶满分）

            以 %B 位置线性插值，避免"必须跌破下轨才能拿满分"的阶跃式打分。
            """
            boll_upper = 2 * boll_mid - boll_lower
            band = boll_upper - boll_lower
            pct_b = (close - boll_lower) / band  # 下轨=0, 中轨=0.5, 上轨=1
            score = round(max_score * (1 - min(max(pct_b, 0.0), 1.0)), 1)
            return score, ScoringLayer.MomentumScorer._boll_zone_desc(pct_b, prefix), pct_b

        @staticmethod
        def _boll_zone_desc(pct_b: float, prefix: str = "") -> str:
            """根据 %B 位置给出文字描述"""
            if pct_b <= 0:
                return f"跌破{prefix}下轨"
            if pct_b <= 0.25:
                return f"贴近{prefix}下轨"
            if pct_b <= 0.5:
                return f"{prefix}中轨下方"
            if pct_b <= 0.75:
                return f"{prefix}中轨上方"
            return f"{prefix}高位区域"

        @staticmethod
        def calc_monthly_boll_score(close: float, boll_mid: float, boll_lower: float, month_zone: str) -> Tuple[
            float, str, dict]:
            """月线BOLL评分 (10分) - 长周期波动，带内线性打分"""
            if boll_mid <= boll_lower:
                return 0, "0/10 (BOLL无效)", {}

            if month_zone == "mid":
                # 震荡中枢型标的：以中轨为基准，mid*1.05 得0分，mid*0.9 得满分
                high_ref, low_ref = boll_mid * 1.05, boll_mid * 0.9
                ratio = (high_ref - close) / (high_ref - low_ref)
                score = round(10 * min(max(ratio, 0.0), 1.0), 1)
                pct_b = (close - boll_lower) / (2 * (boll_mid - boll_lower))
                desc = ScoringLayer.MomentumScorer._boll_zone_desc(pct_b, "月")
            else:
                score, desc, pct_b = ScoringLayer.MomentumScorer._boll_linear_score(close, boll_mid, boll_lower, 10,
                                                                                   "月")

            return score, f"{score}/10 ({desc} %B={pct_b * 100:.0f}%)", {
                'close': close, 'boll_mid': boll_mid, 'boll_lower': boll_lower,
                'month_zone': month_zone, 'pct_b': round(pct_b, 4)}

        @staticmethod
        def calc_daily_boll_score(close: float, boll_mid: float, boll_lower: float) -> Tuple[float, str]:
            """日线BOLL评分 (5分) - 带内线性打分"""
            if boll_mid <= boll_lower:
                return 0, "0/5 (BOLL无效)"

            score, desc, pct_b = ScoringLayer.MomentumScorer._boll_linear_score(close, boll_mid, boll_lower, 5, "日")
            return score, f"{score}/5 ({desc} %B={pct_b * 100:.0f}%)"

        @staticmethod
        def calc_weekly_boll_score(close: float, boll_mid: float, boll_lower: float) -> Tuple[float, str]:
            """周线BOLL评分 (10分) - 带内线性打分"""
            if boll_mid <= boll_lower:
                return 0, "0/10 (BOLL无效)"

            score, desc, pct_b = ScoringLayer.MomentumScorer._boll_linear_score(close, boll_mid, boll_lower, 10,
                                                                                "周")
            return score, f"{score}/10 ({desc} %B={pct_b * 100:.0f}%)"

        @classmethod
        def total_score(cls, monthly_close: float, monthly_boll_mid: float, monthly_boll_lower: float,
                        month_zone: str,
                        weekly_close: float, weekly_boll_mid: float, weekly_boll_lower: float,
                        daily_close: float, daily_boll_mid: float, daily_boll_lower: float) -> dict:
            """动能层总分 (25分)"""
            monthly_boll_score, monthly_boll_detail, monthly_boll_info = cls.calc_monthly_boll_score(
                monthly_close, monthly_boll_mid, monthly_boll_lower, month_zone)
            weekly_boll_score, weekly_boll_detail = cls.calc_weekly_boll_score(weekly_close, weekly_boll_mid,
                                                                               weekly_boll_lower)
            daily_boll_score, daily_boll_detail = cls.calc_daily_boll_score(daily_close, daily_boll_mid,
                                                                            daily_boll_lower)

            return {
                'score': monthly_boll_score + weekly_boll_score + daily_boll_score,
                'max_score': 25,
                'monthly_boll': {'score': monthly_boll_score, 'detail': monthly_boll_detail,
                                 'info': monthly_boll_info, 'max_score': 10},
                'weekly_boll': {'score': weekly_boll_score, 'detail': weekly_boll_detail, 'max_score': 10},
                'daily_boll': {'score': daily_boll_score, 'detail': daily_boll_detail, 'max_score': 5}
            }

    # ========== 第四层：结构层 (15分) - 使用 czsc ==========
    class CycleStructureScorer:
        """结构层评分器 - 使用czsc缠论分析月线笔结构和周线背驰"""

        @staticmethod
        def _create_czsc_object(df: pd.DataFrame, freq_str: str, stock_name: str = "") -> Optional[CZSC]:
            """将DataFrame转换为CZSC对象"""
            if not CZSC_AVAILABLE or df is None:
                return None

            # 不同周期最小K线要求
            min_bars_map = {
                'D': 250,
                'W': 160,
                'M': 36  # 月线最低门槛调整为36根（约3年）
            }

            min_bars = min_bars_map.get(freq_str, 100)

            if len(df) < min_bars:
                print(f"[WARN] {stock_name}-结构层-周期K线不足: {freq_str} {len(df)} < {min_bars}")
                return None

            try:
                freq_map = {'D': Freq.D, 'W': Freq.W, 'M': Freq.M}
                freq_enum = freq_map.get(freq_str, Freq.D)

                df = df.copy()
                if 'dt' not in df.columns:
                    df['dt'] = pd.to_datetime(df['trade_date'])

                bars = []
                for i, row in df.iterrows():
                    bar = RawBar(
                        symbol=row.get('ts_code', row.get('symbol', 'unknown')),
                        id=i,
                        dt=row['dt'],
                        freq=freq_enum,
                        open=float(row['open']),
                        close=float(row['close']),
                        high=float(row['high']),
                        low=float(row['low']),
                        vol=float(row.get('vol', 0)),
                        amount=float(row.get('amount', 0))
                    )
                    bars.append(bar)

                return CZSC(bars)
            except Exception as e:
                print(f"[ERROR] CZSC对象创建失败: {e}")
                return None

        @classmethod
        def calc_monthly_bi_structure_score(cls, df_monthly: pd.DataFrame, stock_name: str = "") -> Tuple[float, str]:
            """月线结构评分 (8分) - 大级别方向"""
            if not CZSC_AVAILABLE:
                return 0, "0/8 (czsc不可用)", {}

            monthly_count = len(df_monthly) if df_monthly is not None else 0
            c = cls._create_czsc_object(df_monthly, 'M', stock_name)
            if c is None:
                return 0, "0/8 (数据不足)", {}

            details = []
            score_breakdown = {}
            signal_type = None

            # ========== 第一部分：长期环境分（5分）==========
            env_score = 0
            if hasattr(c, 'bi_list') and c.bi_list:
                try:
                    bi_state = cxt_bi_base_V230228(c)
                    if bi_state:
                        bi_value = str(list(bi_state.values())[0])
                        details.append(f"笔状态:{bi_value}")
                        parts = bi_value.split('_')
                        direction = parts[0] if parts else ''
                        state = parts[1] if len(parts) > 1 else ''

                        if direction == '向下' and state == '中继':
                            env_score = 5
                            signal_type = '向下中继'
                        elif direction == '向下' and state == '转折':
                            env_score = 4
                            signal_type = '向下转折'
                        elif direction == '横向' or state == '横盘':
                            env_score = 3
                            signal_type = '横盘'
                        elif direction == '向上' and state == '转折':
                            env_score = 2
                            signal_type = '向上转折'
                        elif direction == '向上' and state == '中继':
                            env_score = 0
                            signal_type = '向上中继'
                    score_breakdown['长期环境分'] = env_score
                except:
                    pass

            # ========== 第二部分：结构强化分（0 ~ +3分）==========
            reinforce_score = 0
            try:
                five_bi_sig = cxt_five_bi_V230619(c)
                if five_bi_sig:
                    for v in five_bi_sig.values():
                        vs = str(v)
                        details.append(f"五笔:{vs}")

                        # 只保留加分信号
                        if 'aAb式底背驰' in vs:
                            reinforce_score = max(reinforce_score, 3)
                            score_breakdown['aAb式底背驰'] = 3
                            signal_type = '月线aAb式底背驰'
                        elif '类趋势底背驰' in vs:
                            reinforce_score = max(reinforce_score, 2)
                            score_breakdown['类趋势底背驰'] = 2
                            if not signal_type:
                                signal_type = '月线类趋势底背驰'
                        elif '底背驰' in vs:
                            reinforce_score = max(reinforce_score, 1)
                            score_breakdown['底背驰'] = 1
                            if not signal_type:
                                signal_type = '月线底背驰'
            except:
                pass

            # 结构强化分限制在 0~3 之间
            reinforce_score = max(min(reinforce_score, 3), 0)

            # ========== 原始分数计算 ==========
            raw_score = env_score + reinforce_score
            raw_score = max(min(raw_score, 8), 0)

            # ========== 可信度计算（仅月线数量 >= 36时应用）==========
            if monthly_count >= 60:
                confidence = 1.0
            elif monthly_count >= 48:
                confidence = 0.9
            elif monthly_count >= 36:
                confidence = 0.8
            else:
                confidence = 1.0  # < 36 时不应用可信度，保持原始分数

            # 应用可信度
            score = round(raw_score * confidence, 1)
            score_breakdown['可信度'] = confidence
            score_breakdown['原始分数'] = raw_score
            details.append(f"可信度:{confidence}")

            reason = f"{signal_type or '无明确信号'}"

            return score, f"{score}/8 ({','.join(details) if details else '无明确结构'})", {'score': score,
                                                                                            'max_score': 8,
                                                                                            'details': details,
                                                                                            'reason': reason,
                                                                                            'score_breakdown': score_breakdown,
                                                                                            'signal_type': signal_type,
                                                                                            'env_score': env_score,
                                                                                            'reinforce_score': reinforce_score,
                                                                                            'confidence': confidence,
                                                                                            'raw_score': raw_score}

        @classmethod
        def calc_weekly_structure_score(cls, df_weekly: pd.DataFrame, stock_name: str = "") -> Tuple[float, str]:
            """周线结构评分 (7分) - 动态拐点"""
            if not CZSC_AVAILABLE:
                return 0, "0/7 (czsc不可用)", {}

            c = cls._create_czsc_object(df_weekly, 'W', stock_name)
            if c is None:
                return 0, "0/7 (数据不足)", {}

            details = []
            score_breakdown = {}
            signal_type = None

            # ========== 第一部分：环境基础分（3分）==========
            env_score = 0
            if hasattr(c, 'bi_list') and c.bi_list:
                try:
                    bi_state = cxt_bi_base_V230228(c)
                    if bi_state:
                        bi_value = str(list(bi_state.values())[0])
                        details.append(f"笔状态:{bi_value}")
                        parts = bi_value.split('_')
                        direction = parts[0] if parts else ''
                        state = parts[1] if len(parts) > 1 else ''

                        if direction == '向下' and state == '转折':
                            env_score = 3
                            signal_type = '向下转折'
                        elif direction == '向下' and state == '中继':
                            env_score = 2
                            signal_type = '向下中继'
                        elif direction == '向上' and state == '转折':
                            env_score = 1
                            signal_type = '向上转折'
                        elif direction == '向上' and state == '中继':
                            env_score = 0
                            signal_type = '向上中继'
                    score_breakdown['环境基础分'] = env_score
                except:
                    pass

            # ========== 第二部分：反转结构强化分（0 ~ +4分）==========
            reversal_score = 0

            # 1. 一买信号（+4分）
            try:
                first_buy_sig = cxt_first_buy_V221126(c)
                if first_buy_sig:
                    reversal_score += 4
                    details.append('一买')
                    score_breakdown['一买'] = 4
                    signal_type = '周线一买'
            except:
                pass

            # 2. 五笔信号（可正可负）
            try:
                five_bi_sig = cxt_five_bi_V230619(c)
                if five_bi_sig:
                    for v in five_bi_sig.values():
                        vs = str(v)
                        details.append(f"五笔:{vs}")

                        # 加分信号
                        if '类三买' in vs:
                            reversal_score += 3
                            score_breakdown['类三买'] = 3
                            if not signal_type:
                                signal_type = '周线类三买'
                        elif 'aAb式底背驰' in vs:
                            reversal_score += 3
                            score_breakdown['aAb式底背驰'] = 3
                            if not signal_type:
                                signal_type = '周线aAb式底背驰'
                        elif '类趋势底背驰' in vs or '底背驰' in vs:
                            reversal_score += 2
                            score_breakdown['底背驰'] = 2
                            if not signal_type:
                                signal_type = '周线底背驰'
                        elif '上颈线突破' in vs:
                            reversal_score += 2
                            score_breakdown['上颈线突破'] = 2
                            if not signal_type:
                                signal_type = '周线上颈线突破'

                        # 减分信号
                        elif '类三卖' in vs:
                            reversal_score -= 3
                            score_breakdown['类三卖'] = -3
                            if not signal_type:
                                signal_type = '周线类三卖'
                        elif 'aAb式顶背驰' in vs:
                            reversal_score -= 3
                            score_breakdown['aAb式顶背驰'] = -3
                            if not signal_type:
                                signal_type = '周线aAb式顶背驰'
                        elif '类趋势顶背驰' in vs or '顶背驰' in vs:
                            reversal_score -= 2
                            score_breakdown['顶背驰'] = -2
                            if not signal_type:
                                signal_type = '周线顶背驰'
                        elif '下颈线突破' in vs:
                            reversal_score -= 2
                            score_breakdown['下颈线突破'] = -2
                            if not signal_type:
                                signal_type = '周线下颈线突破'
            except:
                pass

            # 3. 趋势辅助（最多+1分）
            try:
                trend_sig = cxt_bi_trend_V230824(c)
                if trend_sig:
                    trend_value = str(list(trend_sig.values())[0])
                    details.append(f"趋势:{trend_value}")
                    if '向下转折' in trend_value:
                        reversal_score += 1
                        score_breakdown['趋势向下转折'] = 1
                    elif '横盘' in trend_value:
                        reversal_score += 0.5
                        score_breakdown['趋势横盘'] = 0.5
            except:
                pass

            # 限制反转结构强化分范围在 0 ~ +4 之间
            reversal_score = max(min(reversal_score, 4), 0)

            # ========== 总分计算 ==========
            score = env_score + reversal_score
            score = max(min(score, 7), 0)

            reason = f"{signal_type or '无明确信号'}"

            return score, f"{score}/7 ({','.join(details) if details else '无明确结构'})", {'score': score,
                                                                                            'max_score': 7,
                                                                                            'details': details,
                                                                                            'reason': reason,
                                                                                            'score_breakdown': score_breakdown,
                                                                                            'signal_type': signal_type,
                                                                                            'env_score': env_score,
                                                                                            'reversal_score': reversal_score}

        @classmethod
        def generate_monthly_html(cls, df_monthly: pd.DataFrame, stock_name: str = "") -> str:
            """生成月线缠论HTML图表（包含五笔、趋势、笔状态信号标注）"""
            if not CZSC_AVAILABLE or df_monthly is None:
                return "<p>CZSC不可用</p>"

            c = cls._create_czsc_object(df_monthly, 'M', stock_name)
            if c is None:
                return "<p>数据不足，无法生成图表</p>"

            try:
                import plotly.graph_objects as go
                from plotly.subplots import make_subplots

                df = c.bars_raw_df
                fig = make_subplots(rows=3, cols=1, shared_xaxes=True,
                                    vertical_spacing=0.05, row_heights=[0.5, 0.25, 0.25],
                                    subplot_titles=('月线 K线 + 笔 + 信号', '五笔结构', '趋势状态'))

                fig.add_trace(go.Candlestick(
                    x=df['dt'], open=df['open'], high=df['high'],
                    low=df['low'], close=df['close'], name='K线',
                    increasing_line_color='red', decreasing_line_color='green'
                ), row=1, col=1)

                bi_count = 0
                if hasattr(c, 'bi_list') and c.bi_list:
                    for bi in c.bi_list:
                        bi_count += 1
                        color = 'red' if str(bi.direction) == '向上' else 'green'
                        fig.add_trace(go.Scatter(
                            x=[bi.raw_bars[0].dt, bi.raw_bars[-1].dt],
                            y=[bi.raw_bars[0].close, bi.raw_bars[-1].close],
                            mode='lines', line=dict(color=color, width=4),
                            name=f'笔 {bi.direction}'
                        ), row=1, col=1)

                    for i, bi in enumerate(c.bi_list[-10:]):
                        mid_x = bi.raw_bars[len(bi.raw_bars) // 2].dt
                        mid_price = (bi.raw_bars[0].close + bi.raw_bars[-1].close) / 2
                        color = '#FF6B6B' if str(bi.direction) == '向上' else '#4ECDC4'
                        fig.add_annotation(
                            x=mid_x, y=mid_price,
                            text=f"<b>{bi.direction.value[:1]}</b>",
                            showarrow=False,
                            font=dict(size=10, color=color),
                            row=1, col=1
                        )

                try:
                    five_bi_sig = cxt_five_bi_V230619(c)
                    sig_text = "其他"
                    if five_bi_sig:
                        for v in five_bi_sig.values():
                            vs = str(v)
                            sig_text = vs.split('_')[-2] if '_' in vs else vs
                    fig.add_annotation(
                        x=df['dt'].iloc[-1], y=df['high'].max() * 1.02,
                        text=f"<b>五笔: {sig_text}</b>",
                        showarrow=False,
                        font=dict(size=12, color='blue'),
                        row=2, col=1
                    )
                except:
                    pass

                try:
                    trend_sig = cxt_bi_trend_V230824(c)
                    trend_text = "其他"
                    if trend_sig:
                        for v in trend_sig.values():
                            vs = str(v)
                            trend_text = vs.split('_')[-2] if '_' in vs else vs
                    fig.add_annotation(
                        x=df['dt'].iloc[-1], y=1.02,
                        text=f"<b>趋势: {trend_text}</b>",
                        showarrow=False,
                        font=dict(size=12, color='purple'),
                        row=3, col=1
                    )
                except:
                    pass

                fig.update_layout(
                    height=800,
                    showlegend=True,
                    title_text=f"月线缠论图表 - 笔数:{bi_count}",
                    xaxis_rangeslider_visible=False
                )

                return fig.to_html(full_html=True, include_plotlyjs=True)
            except Exception as e:
                return f"<p>图表生成失败: {str(e)}</p>"

        @classmethod
        def generate_weekly_html(cls, df_weekly: pd.DataFrame, stock_name: str = "") -> str:
            """生成周线缠论HTML图表"""
            if not CZSC_AVAILABLE or df_weekly is None:
                return "<p>CZSC不可用</p>"

            c = cls._create_czsc_object(df_weekly, 'W', stock_name)
            if c is None:
                return "<p>数据不足，无法生成图表</p>"

            try:
                import plotly.graph_objects as go
                from plotly.subplots import make_subplots

                df = c.bars_raw_df
                fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                                    vertical_spacing=0.03, row_heights=[0.7, 0.3],
                                    subplot_titles=('周线 K线 + 笔', '成交量'))

                fig.add_trace(go.Candlestick(
                    x=df['dt'], open=df['open'], high=df['high'],
                    low=df['low'], close=df['close'], name='K线'
                ), row=1, col=1)

                if hasattr(c, 'bi_list') and c.bi_list:
                    for bi in c.bi_list:
                        color = 'red' if str(bi.direction) == '向上' else 'green'
                        fig.add_trace(go.Scatter(
                            x=[bi.raw_bars[0].dt, bi.raw_bars[-1].dt],
                            y=[bi.raw_bars[0].close, bi.raw_bars[-1].close],
                            mode='lines', line=dict(color=color, width=3),
                            name=f'笔 {bi.direction}'
                        ), row=1, col=1)

                fig.add_trace(go.Bar(x=df['dt'], y=df['vol'], name='成交量', marker_color='blue'), row=2, col=1)
                fig.update_layout(height=600, showlegend=True, title_text="周线缠论图表",
                                  xaxis_rangeslider_visible=False)

                return fig.to_html(full_html=True, include_plotlyjs=True)
            except Exception as e:
                return f"<p>图表生成失败: {str(e)}</p>"

        @classmethod
        def total_score(cls, df_monthly: pd.DataFrame, df_weekly: pd.DataFrame, stock_name: str = "") -> dict:
            """结构层总分 (15分)"""
            monthly_result = cls.calc_monthly_bi_structure_score(df_monthly, stock_name)
            weekly_result = cls.calc_weekly_structure_score(df_weekly, stock_name)
            monthly_html = cls.generate_monthly_html(df_monthly, stock_name)
            weekly_html = cls.generate_weekly_html(df_weekly, stock_name)

            monthly_score, monthly_detail, monthly_info = monthly_result if len(monthly_result) == 3 else (
                monthly_result[0], monthly_result[1], {})
            weekly_score, weekly_detail, weekly_info = weekly_result if len(weekly_result) == 3 else (weekly_result[0],
                                                                                                      weekly_result[1],
                                                                                                      {})

            return {
                'score': monthly_score + weekly_score,
                'max_score': 15,
                'monthly_structure': {'score': monthly_score, 'detail': monthly_detail, 'info': monthly_info},
                'weekly_structure': {'score': weekly_score, 'detail': weekly_detail, 'info': weekly_info},
                'monthly_html': monthly_html,
                'weekly_html': weekly_html
            }

    # ========== 第五层：共振层 (10分) ==========
    class CycleResonanceScorer:
        """周期共振层：多周期共振识别"""

        @classmethod
        def calc_resonance_score(cls, position_result: dict, momentum_result: dict, cycle_result: dict) -> dict:
            """周期共振评分 (10分) - 不重新计算信号，只读取2/3/4层结果"""
            score_breakdown = {}
            details = []

            # ========== 第一部分：位置共振（3分）==========
            position_conditions = 0
            position_resonance_details = []

            box_score = position_result.get('box', {}).get('score', 0)
            box_max = position_result.get('box', {}).get('max_score', 7)
            if box_score >= box_max * 0.9:
                position_conditions += 1
                position_resonance_details.append('箱体低位')

            zhongshu_score = position_result.get('weekly_zhongshu', {}).get('score', 0)
            zhongshu_max = position_result.get('weekly_zhongshu', {}).get('max_score', 8)
            if zhongshu_score >= zhongshu_max:
                position_conditions += 1
                position_resonance_details.append('周线中枢下沿')

            bias_score = position_result.get('bias250', {}).get('score', 0)
            bias_max = position_result.get('bias250', {}).get('max_score', 5)
            if bias_score >= bias_max:
                position_conditions += 1
                position_resonance_details.append('Bias250低位')

            position_resonance_score = position_conditions
            score_breakdown['位置共振'] = position_resonance_score
            if position_resonance_details:
                details.append(f"位置共振{position_resonance_score}/3({','.join(position_resonance_details)})")

            # ========== 第二部分：动能共振（3分）==========
            momentum_conditions = 0
            momentum_resonance_details = []

            monthly_boll_score = momentum_result.get('monthly_boll', {}).get('score', 0)
            monthly_boll_max = momentum_result.get('monthly_boll', {}).get('max_score', 10)
            if monthly_boll_score >= monthly_boll_max * 0.9:
                momentum_conditions += 1
                momentum_resonance_details.append('月BOLL低位')

            weekly_boll_score = momentum_result.get('weekly_boll', {}).get('score', 0)
            weekly_boll_max = momentum_result.get('weekly_boll', {}).get('max_score', 10)
            if weekly_boll_score >= weekly_boll_max * 0.9:
                momentum_conditions += 1
                momentum_resonance_details.append('周BOLL低位')

            daily_boll_score = momentum_result.get('daily_boll', {}).get('score', 0)
            daily_boll_max = momentum_result.get('daily_boll', {}).get('max_score', 5)
            if daily_boll_score >= daily_boll_max * 0.9:
                momentum_conditions += 1
                momentum_resonance_details.append('日线BOLL超跌')

            momentum_resonance_score = momentum_conditions
            score_breakdown['动能共振'] = momentum_resonance_score
            if momentum_resonance_details:
                details.append(f"动能共振{momentum_resonance_score}/3({','.join(momentum_resonance_details)})")

            # ========== 第三部分：结构共振（4分）==========
            monthly_signal = cycle_result.get('monthly_structure', {}).get('info', {}).get('signal_type', '') or ''

            monthly_env_score = 0
            if '向下中继' in monthly_signal:
                monthly_env_score = 2
            elif '向下转折' in monthly_signal:
                monthly_env_score = 2
            elif '横盘' in monthly_signal:
                monthly_env_score = 1
            elif '向上转折' in monthly_signal or '向上中继' in monthly_signal:
                monthly_env_score = 0
            score_breakdown['月线结构'] = monthly_env_score

            weekly_signal = cycle_result.get('weekly_structure', {}).get('info', {}).get('signal_type', '') or ''

            weekly_struct_score = 0
            if '一买' in weekly_signal:
                weekly_struct_score = 2
            elif '类三买' in weekly_signal:
                weekly_struct_score = 1
            elif '向下转折' in weekly_signal:
                weekly_struct_score = 1
            elif '顶背驰' in weekly_signal:
                weekly_struct_score = -1
            elif '类三卖' in weekly_signal:
                weekly_struct_score = -2
            score_breakdown['周线结构'] = weekly_struct_score

            structure_resonance_score = max(0, min(monthly_env_score + weekly_struct_score, 4))
            score_breakdown['结构共振'] = structure_resonance_score
            details.append(
                f"结构共振{structure_resonance_score}/4(月线结构:{monthly_env_score},周线结构:{weekly_struct_score})")

            # ========== 总分计算 ==========
            total_score = position_resonance_score + momentum_resonance_score + structure_resonance_score
            total_score = max(0, min(total_score, 10))

            return {
                'score': total_score,
                'max_score': 10,
                'detail': f"{total_score}/10 ({','.join(details)})",
                'info': {
                    'position_resonance': position_resonance_score,
                    'momentum_resonance': momentum_resonance_score,
                    'structure_resonance': structure_resonance_score,
                    'position_conditions': position_conditions,
                    'momentum_conditions': momentum_conditions,
                    'position_details': position_resonance_details,
                    'momentum_details': momentum_resonance_details,
                    'monthly_env_score': monthly_env_score,
                    'weekly_struct_score': weekly_struct_score,
                    'monthly_signal': monthly_signal,
                    'weekly_signal': weekly_signal,
                    'score_breakdown': score_breakdown
                }
            }


# ========== 综合评分 ==========
def calculate_all_scores(symbol: str, info: dict, df_d: pd.DataFrame, df_w: pd.DataFrame, df_m: pd.DataFrame) -> dict:
    """计算五层综合评分"""
    if df_d is None or df_w is None or df_m is None or len(df_d) < 20:
        return None

    close = float(df_d.iloc[-1]['close'])

    close_arr_d = df_d['close'].values.astype(float)
    close_arr_w = df_w['close'].values.astype(float)
    close_arr_m = df_m['close'].values.astype(float)

    box_range = IndicatorLayer.get_box_range(close_arr_d, period=120)

    dy_val = 0.0
    dy_target = 0.0

    if info.get("calc_dy", False):
        if info["type"] == "stock":
            dy_val = DataLayer.get_stock_dividend_yield_ttm(symbol, close)
            recent_div = DataLayer.get_recent_dividend(symbol)
            if box_range['bottom'] > 0 and recent_div > 0:
                dy_target = round(recent_div / box_range['bottom'] * 100, 2)
        else:
            dy_val = DataLayer.calculate_etf_dividend(symbol)
            if box_range['bottom'] > 0 and close > 0 and dy_val > 0:
                recent_div = dy_val * close / 100
                dy_target = round(recent_div / box_range['bottom'] * 100, 2)

    boll_d = IndicatorLayer.calc_boll(close_arr_d)
    boll_m = IndicatorLayer.calc_boll(close_arr_m)
    boll_w = IndicatorLayer.calc_boll(close_arr_w)
    ma_d = IndicatorLayer.calc_ma(close_arr_d)
    rsi_d = IndicatorLayer.calc_rsi(close_arr_d, period=6)
    rsi_w = IndicatorLayer.calc_rsi(close_arr_w, period=6)

    value_result = ScoringLayer.ValueScorer.total_score(symbol, info, dy_val, dy_target)

    # 计算新增指标状态
    ma120_status = IndicatorLayer.get_ma_status(close, ma_d['ma120'])
    ma250_status = IndicatorLayer.get_ma_status(close, ma_d['ma250'])
    boll_d_status = IndicatorLayer.get_boll_status(close, boll_d['mid'], boll_d['lower'])
    boll_w_status = IndicatorLayer.get_boll_status(close, boll_w['mid'], boll_w['lower'])
    boll_m_status = IndicatorLayer.get_boll_status(close, boll_m['mid'], boll_m['lower'])
    rsi_d_status = IndicatorLayer.get_rsi_status(rsi_d)
    rsi_w_status = IndicatorLayer.get_rsi_status(rsi_w)

    # 获取PB分位
    pb_percentile = value_result.get('info', {}).get('pb_percentile', '-')
    position_result = ScoringLayer.PositionScorer.total_score(
        close=close,
        box_bottom=box_range['bottom'],
        box_top=box_range['top'],
        ma250=ma_d['ma250'],
        df_weekly=df_w
    )
    momentum_result = ScoringLayer.MomentumScorer.total_score(
        monthly_close=close,
        monthly_boll_mid=boll_m['mid'],
        monthly_boll_lower=boll_m['lower'],
        month_zone=info.get("month_zone", "lower"),
        weekly_close=close_arr_w[-1],
        weekly_boll_mid=boll_w['mid'],
        weekly_boll_lower=boll_w['lower'],
        daily_close=close,
        daily_boll_mid=boll_d['mid'],
        daily_boll_lower=boll_d['lower']
    )
    cycle_result = ScoringLayer.CycleStructureScorer.total_score(df_monthly=df_m, df_weekly=df_w,
                                                                 stock_name=info['name'])
    resonance_result = ScoringLayer.CycleResonanceScorer.calc_resonance_score(position_result, momentum_result,
                                                                              cycle_result)

    total_score = min(round(
        value_result['score'] +
        position_result['score'] +
        momentum_result['score'] +
        cycle_result['score'] +
        resonance_result['score'], 1), 100)

    return {
        '代码': symbol,
        '名称': info['name'],
        '类型': info['type'],
        '收盘价': round(close, 3) if info['type'] == 'etf' else round(close, 2),
        '股息率': dy_val,
        '目标股息': dy_target,
        '箱体底部': round(box_range['bottom'], 3) if info['type'] == 'etf' else box_range['bottom'],
        '箱体顶部': round(box_range['top'], 3) if info['type'] == 'etf' else box_range['top'],
        '箱体宽度': box_range['width'],
        '120日线': ma120_status,
        '250日线': ma250_status,
        '日布林': boll_d_status,
        '周布林': boll_w_status,
        '月布林': boll_m_status,
        '日RSI': rsi_d_status,
        '周RSI': rsi_w_status,
        'PB分位': pb_percentile,
        '价值层': value_result,
        '位置层': position_result,
        '动能层': momentum_result,
        '结构层': cycle_result,
        '共振层': resonance_result,
        '总分': total_score,
        '建议': StrategyLayer.get_trade_suggestion(total_score),
        'details': {
            'value': value_result,
            'position': position_result,
            'momentum': momentum_result,
            'cycle': cycle_result,
            'resonance': resonance_result
        }
    }


# ==================== 策略层：交易决策 ====================

class StrategyLayer:
    """策略层：交易决策和仓位管理"""

    @staticmethod
    def get_trade_suggestion(score: float) -> str:
        """根据评分给出交易建议"""
        if score >= 90:
            return "🔴 强烈买入"
        elif score >= 80:
            return "🟠 分批建仓"
        elif score >= 70:
            return "🟡 重点观察"
        elif score >= 60:
            return "🟡 观察等待"
        else:
            return "⚪ 不参与"

    @staticmethod
    def get_position_plan(symbol: str, info: dict, score: float, total_capital: float = 100000) -> dict:
        """根据评分计算分批建仓方案"""
        plan = {
            "股票名称": info["name"],
            "股票代码": symbol,
            "当前评分": score,
            "总计划资金": f"{total_capital:,.0f}",
            "建仓阶段": []
        }

        stages = []
        if score >= 90:
            stages = [
                {"阶段": "第1批", "仓位比例": 40, "金额": total_capital * 0.4, "触发条件": "评分>=90"},
                {"阶段": "第2批", "仓位比例": 30, "金额": total_capital * 0.3, "触发条件": "评分维持>=85或价格回落3%"},
                {"阶段": "第3批", "仓位比例": 30, "金额": total_capital * 0.3, "触发条件": "评分维持>=80或价格回落5%"},
            ]
        elif score >= 80:
            stages = [
                {"阶段": "第1批", "仓位比例": 25, "金额": total_capital * 0.25, "触发条件": "评分>=80"},
                {"阶段": "第2批", "仓位比例": 25, "金额": total_capital * 0.25, "触发条件": "评分>=85或价格回落3%"},
                {"阶段": "第3批", "仓位比例": 25, "金额": total_capital * 0.25, "触发条件": "评分>=88或价格回落5%"},
                {"阶段": "第4批", "仓位比例": 25, "金额": total_capital * 0.25, "触发条件": "评分>=90或价格回落8%"},
            ]
        elif score >= 70:
            stages = [
                {"阶段": "观察仓", "仓位比例": 10, "金额": total_capital * 0.1, "触发条件": "评分>=70"},
                {"阶段": "第1批", "仓位比例": 30, "金额": total_capital * 0.3, "触发条件": "评分>=80"},
                {"阶段": "第2批", "仓位比例": 30, "金额": total_capital * 0.3, "触发条件": "评分>=85"},
                {"阶段": "第3批", "仓位比例": 30, "金额": total_capital * 0.3, "触发条件": "评分>=90"},
            ]

        plan["建仓阶段"] = stages
        return plan


# ==================== 输出层：报告生成 ====================

class OutputLayer:
    """输出层：报告生成和邮件发送"""

    @staticmethod
    def generate_html(all_stocks_data: List[dict]) -> str:
        """生成HTML报告"""
        html_style = """
        <style>
            body { font-family: 'Microsoft YaHei', sans-serif; background: #f4f7f6; padding: 20px; }
            .container { max-width: 1600px; margin: auto; background: #fff; padding: 20px; border-radius: 10px; box-shadow: 0 4px 10px rgba(0,0,0,0.1); }
            table { width: 100%; border-collapse: collapse; margin-top: 20px; font-size: 13px; }
            th, td { padding: 8px 4px; border-bottom: 1px solid #eee; text-align: center; }
            th { background: #f8f9fa; color: #666; font-weight: bold; }
            .score-box { font-weight: bold; font-size: 16px; padding: 5px 15px; border-radius: 20px; }
            .score-high { color: #e74c3c; background: #fff5eb; }
            .score-mid { color: #e67e22; background: #fffbeb; }
            .score-low { color: #999; background: #f9fafb; }
            .check-green { color: #2ecc71; font-weight: bold; }
            .cross-red { color: #e74c3c; opacity: 0.6; }
            .stock-name { font-weight: bold; color: #333; }
            tr:hover { background-color: #fcfcfc; }
            .suggest-red { color: #e74c3c; font-weight: bold; }
            .suggest-orange { color: #e67e22; font-weight: bold; }
            .suggest-yellow { color: #f39c12; }
            .suggest-gray { color: #999; }
            .header-info { text-align: center; margin-bottom: 20px; }
            .strategy-box { background: #e8f4f8; padding: 15px; border-radius: 8px; margin-top: 20px; }
            .box-cell { padding: 5px; }
            .box-cell-inner { display: flex; flex-direction: column; align-items: center; }
            .box-item { font-weight: bold; }
            .box-label { font-size: 11px; color: #999; }
            .indicator-cell { display: flex; flex-direction: column; align-items: center; }
        </style>
        """

        def get_score_class(score):
            if score >= 80:
                return "score-high"
            elif score >= 60:
                return "score-mid"
            else:
                return "score-low"

        def get_suggest_class(suggest):
            if "强烈买入" in suggest:
                return "suggest-red"
            elif "分批建仓" in suggest:
                return "suggest-orange"
            elif "重点观察" in suggest or "观察等待" in suggest:
                return "suggest-yellow"
            else:
                return "suggest-gray"

        def format_cell(val):
            if not isinstance(val, str) or '(' not in val:
                return val
            parts = val.split('(')
            sym = parts[0].strip()
            desc = parts[1].replace(')', '')
            cls = "check-green" if '✔' in sym else "cross-red"
            return f'<div class="indicator-cell"><span class="{cls}">{sym}</span><span style="font-size:10px;color:#999;">{desc}</span></div>'

        rows = ""
        all_stocks_data.sort(key=lambda x: x.get('总分', 0), reverse=True)
        for d in all_stocks_data:
            is_etf = d.get('类型') == 'etf'
            price_format = '.3f' if is_etf else '.2f'
            rows += f"""
            <tr>
                <td style="color:#888;">{d['代码']}</td>
                <td class="stock-name">{d['名称']}</td>
                <td class="score-box {get_score_class(d['总分'])}">{d['总分']}</td>
                <td>{format(d['收盘价'], price_format)}</td>
                <td style="color:#2980b9; font-weight:bold;">{d['股息率']:.2f}%</td>
                <td style="color:#7f8c8d;">{d['目标股息']:.2f}%</td>
                <td class="box-cell">
                    <div class="box-cell-inner">
                        <span class="box-item">{format(d['箱体顶部'], price_format)}</span>
                        <span class="box-label">▲</span>
                        <span class="box-item">{format(d['箱体底部'], price_format)}</span>
                        <span class="box-label">宽度: {d['箱体宽度']:.2f}</span>
                    </div>
                </td>
                <td>{format_cell(d.get('120日线', '-'))}</td>
                <td>{format_cell(d.get('250日线', '-'))}</td>
                <td>{format_cell(d.get('日布林', '-'))}</td>
                <td>{format_cell(d.get('周布林', '-'))}</td>
                <td>{format_cell(d.get('月布林', '-'))}</td>
                <td>{format_cell(d.get('日RSI', '-'))}</td>
                <td>{format_cell(d.get('周RSI', '-'))}</td>
                <td>{d.get('PB分位', '-')}</td>
                <td>{d['价值层']['score']}/{d['价值层']['max_score']}</td>
                <td>{d['位置层']['score']}/{d['位置层']['max_score']}</td>
                <td>{d['动能层']['score']}/{d['动能层']['max_score']}</td>
                <td>{d['结构层']['score']}/{d['结构层']['max_score']}</td>
                <td>{d['共振层']['score']}/{d['共振层']['max_score']}</td>
                <td class="{get_suggest_class(d['建议'])}">{d['建议']}</td>
            </tr>"""

        strategy_note = """
        <div class="strategy-box">
            <h3>📋 交易策略说明</h3>
            <ul style="text-align:left;">
                <li><strong>评分 >= 90分</strong>：强烈买入信号，可分3批建仓（40%/30%/30%）</li>
                <li><strong>80分 <= 评分 < 90分</strong>：分批建仓信号，分4批逐步买入</li>
                <li><strong>70分 <= 评分 < 80分</strong>：重点观察，等待更佳买点</li>
                <li><strong>60分 <= 评分 < 70分</strong>：观察等待</li>
                <li><strong>评分 < 60分</strong>：暂不介入，等待价格回落</li>
            </ul>
            <p style="color:#7f8c8d; font-size:12px;">⚠️ 提示：评分仅供参考，投资有风险，决策需谨慎</p>
        </div>
        """

        return f"""<html><head>{html_style}</head><body><div class="container">
            <div class="header-info">
                <h2>📊 红利权重股五层评分监控看板</h2>
                <p style="color:#666;">更新时间：{time.strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
            <table>
                <tr>
                    <th>代码</th><th>名称</th><th>评分</th><th>收盘价</th><th>股息率</th><th>目标股息</th>
                    <th>箱体</th>
                    <th>120日线</th><th>250日线</th><th>日布林</th><th>周布林</th><th>月布林</th>
                    <th>日RSI</th><th>周RSI</th>
                    <th>PB分位</th><th>价值层</th><th>位置层</th><th>动能层</th><th>结构层</th><th>共振层</th><th>操作建议</th>
                </tr>
                {rows}
            </table>
            {strategy_note}
        </div></body></html>"""

    @staticmethod
    def send_email(html_content: str) -> bool:
        """发送邮件报告"""
        success_count = 0
        fail_list = []
        SLEEP_MIN = 1
        SLEEP_MAX = 3

        try:
            print(f"🚀 开始批量推送任务，总计 {len(EMAIL_TO_LIST)} 位接收者...")
            server = smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT)
            server.login(EMAIL_USER, EMAIL_PASS)

            for i, recipient in enumerate(EMAIL_TO_LIST):
                try:
                    msg = MIMEMultipart('alternative')
                    msg['From'] = EMAIL_USER
                    msg['To'] = recipient
                    msg['Subject'] = Header('📊 红利权重股五层评分监控报告', 'utf-8')
                    msg.attach(MIMEText(html_content, 'html', 'utf-8'))
                    server.send_message(msg)
                    success_count += 1
                    print(f"[{i + 1}/{len(EMAIL_TO_LIST)}] ✅ 已推送到: {recipient}")
                    if i < len(EMAIL_TO_LIST) - 1:
                        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
                except Exception as inner_e:
                    print(f"❌ 推送给 {recipient} 失败: {inner_e}")
                    fail_list.append(recipient)

            server.quit()
            print(f"\n✨ 任务完成！")
            print(f"📈 成功: {success_count} | 失败: {len(fail_list)}")
            if fail_list:
                print(f"⚠️ 失败名单: {fail_list}")

            return success_count > 0

        except Exception as e:
            print(f"💥 邮件服务器连接崩溃: {e}")
            return False

    @staticmethod
    def save_report(html_content: str, filename: str = "红利权重股五层评分监控.html") -> str:
        """保存报告到本地"""
        save_dir = os.path.dirname(os.path.abspath(__file__))  # 保存到当前脚本所在目录
        os.makedirs(save_dir, exist_ok=True)
        filepath = os.path.join(save_dir, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"✨ 报告已生成至: {filepath}")
        return filepath


# ==================== 主程序入口 ====================

def main():
    start_time = time.time()
    print("🚀 开始扫描红利权重股（五层评分体系）...")
    print(f"czsc库状态: {'✅ 已加载' if CZSC_AVAILABLE else '❌ 未安装，结构层使用简化计算'}")

    all_stocks = list(DataLayer.STOCK_POOL.items()) + list(DataLayer.ETF_POOL.items())
    tf_instances = [_tf1, _tf2, _tf3, _tf4]
    print(f"📦 共 {len(all_stocks)} 只股票，使用 {len(tf_instances)} 个API Key并发处理")

    def worker(tf_instance, stock_list, group_id):
        """单个worker处理一组股票"""
        results = []
        for j, (symbol, info) in enumerate(stock_list):
            print(f"🔄 [Group {group_id}] 正在处理: {info['name']} ({symbol})")
            info_copy = info.copy()
            info_copy['tf_instance'] = tf_instance
            res = process_stock(symbol, info_copy)
            if res:
                results.append(res)
                print(f"✅ [Group {group_id}] [{res['名称']}] 总分: {res['总分']} - {res['建议']}")
                print(f"   [Group {group_id}] 价值层: {res['价值层']['score']}/{res['价值层']['max_score']} | "
                      f"位置层: {res['位置层']['score']}/{res['位置层']['max_score']} | "
                      f"动能层: {res['动能层']['score']}/{res['动能层']['max_score']} | "
                      f"结构层: {res['结构层']['score']}/{res['结构层']['max_score']} | "
                      f"共振层: {res['共振层']['score']}/{res['共振层']['max_score']}")

            if j < len(stock_list) - 1:
                print(f"⏳ [Group {group_id}] 等待20秒后继续下一个股票查询...")
                time.sleep(20)
        return results

    queues = [[], [], [], []]
    for i, stock in enumerate(all_stocks):
        queues[i % 4].append(stock)

    print(
        f"📊 任务分配: Group1={len(queues[0])}只, Group2={len(queues[1])}只, Group3={len(queues[2])}只, Group4={len(queues[3])}只")

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for i, (tf, q) in enumerate(zip(tf_instances, queues)):
            futures.append(executor.submit(worker, tf, q, i + 1))

        results = []
        for f in futures:
            results.extend(f.result())

    if not results:
        print("❌ 未获取到有效数据")
        return

    results.sort(key=lambda x: x.get('总分', 0), reverse=True)

    html = OutputLayer.generate_html(results)
    # OutputLayer.save_report(html)
    OutputLayer.send_email(html)

    end_time = time.time()
    total_time = end_time - start_time
    hours = int(total_time // 3600)
    minutes = int((total_time % 3600) // 60)
    seconds = int(total_time % 60)
    print(f"\n⏱️ 总耗时: {hours}小时{minutes}分{seconds}秒")

    # print("\n📊 分批建仓方案建议（以10万元为例）:")
    # high_score_stocks = [r for r in results if r['总分'] >= 80]
    # if high_score_stocks:
    #     for stock in high_score_stocks:
    #         plan = StrategyLayer.get_position_plan(stock['代码'], {'name': stock['名称']}, stock['总分'])
    #         print(f"\n{stock['名称']} ({stock['代码']}) - 评分: {stock['总分']}")
    #         for stage in plan['建仓阶段']:
    #             print(f"  {stage['阶段']}: {stage['仓位比例']}% ({stage['金额']:,.0f}元) - {stage['触发条件']}")
    # else:
    #     print("  当前无评分>=80分的股票")


def process_stock(symbol: str, info: dict) -> Optional[dict]:
    """处理单个股票的评分计算"""
    try:
        print(f"\n正在处理: {info['name']} ({symbol})")
        df_d, df_w, df_m = DataLayer.get_price_data(symbol, info)
        if df_d is None or df_d.empty:
            print(f"  ❌ 日线数据为空")
            return None

        # close = float(df_d.iloc[-1]['close'])
        # print(f"  最新收盘价: {close}")

        result = calculate_all_scores(symbol, info, df_d, df_w, df_m)
        # if result:
        #     print(f"  ✅ 计算完成 - 总分: {result['总分']}")
        #
        #     print(f"\n  📊 各层评分明细:")
        #     print(f"  ┌─────────────────────────────────────────────────────────────────┐")
        #
        #     value = result['价值层']
        #     print(f"  │ 第一层：价值层      {value['score']}/{value['max_score']}分          │")
        #     print(f"  │   ├── 动态股息率: {value['dividend']['detail']}          │")
        #     print(f"  │   └── PB分位:     {value['pb']['detail']}                │")
        #
        #     position = result['位置层']
        #     print(f"  ├─────────────────────────────────────────────────────────────────┤")
        #     print(f"  │ 第二层：位置层   {position['score']}/{position['max_score']}分          │")
        #     print(f"  │   ├── 箱体位置:   {position['box']['detail']}            │")
        #     print(f"  │   │   计算细节: close={result['收盘价']:.2f}, box_bottom={result['箱体底部']:.2f}, box_top={result['箱体顶部']:.2f} │")
        #     print(f"  │   ├── 周线中枢:   {position['weekly_zhongshu']['detail']} │")
        #     if position['weekly_zhongshu']['info']:
        #         zs_info = position['weekly_zhongshu']['info']
        #         print(f"  │   │   计算细节: zd={zs_info.get('zd', 0):.2f}, zg={zs_info.get('zg', 0):.2f}, close={zs_info.get('close', 0):.2f} │")
        #     print(f"  │   └── Bias250:    {position['bias250']['detail']}       │")
        #     if position['bias250']['info']:
        #         bias_info = position['bias250']['info']
        #         print(f"  │       计算细节: close={bias_info.get('close', 0):.2f}, ma250={bias_info.get('ma250', 0):.2f}, bias={bias_info.get('bias', 0):.1f}% │")
        #
        #     momentum = result['动能层']
        #     print(f"  ├─────────────────────────────────────────────────────────────────┤")
        #     print(f"  │ 第三层：动能层      {momentum['score']}/{momentum['max_score']}分          │")
        #     print(f"  │   ├── 月线BOLL:   {momentum['monthly_boll']['detail']}   │")
        #     if momentum['monthly_boll']['info']:
        #         m_boll_info = momentum['monthly_boll']['info']
        #         print(f"  │   │   计算细节: close={m_boll_info.get('close', 0):.2f}, mid={m_boll_info.get('boll_mid', 0):.2f}, lower={m_boll_info.get('boll_lower', 0):.2f} │")
        #     print(f"  │   ├── 周BOLL:     {momentum['weekly_boll']['detail']}     │")
        #     print(f"  │   └── 日线BOLL:   {momentum['daily_boll']['detail']}     │")
        #
        #     cycle = result['结构层']
        #     print(f"  ├─────────────────────────────────────────────────────────────────┤")
        #     print(f"  │ 第四层：结构层   {cycle['score']}/{cycle['max_score']}分          │")
        #     print(f"  │   ├── 月线结构: {cycle['monthly_structure']['detail']} │")
        #     if cycle['monthly_structure']['info']:
        #         bi_info = cycle['monthly_structure']['info']
        #         print(f"  │   │   评分原因: {bi_info.get('reason', '未知')}")
        #         if bi_info.get('details'):
        #             print(f"  │   │   信号详情: {','.join(bi_info['details'])}")
        #         if bi_info.get('score_breakdown'):
        #             print(f"  │   │   得分明细: {bi_info['score_breakdown']}")
        #     print(f"  │   └── 周线结构: {cycle['weekly_structure']['detail']} │")
        #     if cycle['weekly_structure']['info']:
        #         weekly_info = cycle['weekly_structure']['info']
        #         print(f"  │       信号来源: {weekly_info.get('reason', '未知')}")
        #         if weekly_info.get('score_breakdown'):
        #             print(f"  │       得分明细: {weekly_info['score_breakdown']}")
        #
        #     resonance = result['共振层']
        #     print(f"  ├─────────────────────────────────────────────────────────────────┤")
        #     print(f"  │ 第五层：共振层   {resonance['score']}/{resonance['max_score']}分          │")
        #     res_info = resonance['info']
        #     if res_info.get('position_resonance') is not None:
        #         pos_cond = res_info.get('position_conditions', 0)
        #         pos_details = ','.join(res_info.get('position_details', []))
        #         pos_detail_str = f" (满足{pos_cond}/3: {pos_details})" if pos_details else f" (满足{pos_cond}/3)"
        #         print(f"  │   ├── 位置共振:   {res_info['position_resonance']}/3{pos_detail_str}     │")
        #     if res_info.get('momentum_resonance') is not None:
        #         mom_cond = res_info.get('momentum_conditions', 0)
        #         mom_details = ','.join(res_info.get('momentum_details', []))
        #         mom_detail_str = f" (满足{mom_cond}/3: {mom_details})" if mom_details else f" (满足{mom_cond}/3)"
        #         print(f"  │   ├── 动能共振:   {res_info['momentum_resonance']}/3{mom_detail_str}     │")
        #     if res_info.get('structure_resonance') is not None:
        #         monthly_env = res_info.get('monthly_env_score', 0)
        #         weekly_struct = res_info.get('weekly_struct_score', 0)
        #         print(f"  │   └── 结构共振:   {res_info['structure_resonance']}/4 (月线环境:{monthly_env}, 周线结构:{weekly_struct}) │")
        #
        #     print(f"  └─────────────────────────────────────────────────────────────────┘")
        #
        #     print(f"\n  🎯 操作建议: {result['建议']}")
        #     print(f"     当前股息率: {result['股息率']:.2f}% | 目标股息率: {result['目标股息']:.2f}%")
        #     print(f"     箱体区间: {result['箱体底部']:.2f} - {result['箱体顶部']:.2f} (宽度: {result['箱体宽度']:.1f}%)")
        # else:
        #     print(f"  ❌ 计算结果为空")
        return result
    except Exception as e:
        import traceback
        print(f"[ERROR] 处理 {symbol} 失败: {e}")
        traceback.print_exc()
        return None


if __name__ == "__main__":
    main()