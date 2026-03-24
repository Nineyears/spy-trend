#!/usr/bin/env python3
"""
CTA 数据代理服务
从新浪财经（行情）+ Tradingster（COT 持仓）获取数据
前端通过 http://localhost:8433/api/xxx 访问

启动: python3 cta-proxy.py
"""

import json
import re
import urllib.request
import urllib.error
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import threading
import time
import traceback
from datetime import datetime, date
import calendar

PORT = 8433

# ============ 新浪财经 数据源配置 ============

# 新浪期货代码映射 (Yahoo symbol -> 新浪代码)
SINA_FUTURES = {
    'ES=F':    'hf_ES',     # S&P 500 E-mini 期货
    'GC=F':    'hf_GC',     # 黄金
    'CL=F':    'hf_CL',     # 原油
    'HG=F':    'hf_HG',     # 铜
    'NQ=F':    'hf_NQ',     # Nasdaq 100 期货
    'SI=F':    'hf_SI',     # 白银
}

# 新浪美股代码映射
SINA_STOCKS = {
    '^GSPC':   'gb_$inx',   # S&P 500 指数
}

# 新浪外汇代码映射
SINA_FX = {
    'JPY=X':   'fx_susdjpy', # USD/JPY
}

# 新浪K线接口（美股）
SINA_KLINE_SYMBOL = {
    '^GSPC': '.INX',
}

# 新浪期货 K 线映射 (Yahoo symbol -> 新浪期货K线代码)
SINA_KLINE_FUTURES = {
    'ES=F': 'ES',
    'GC=F': 'GC',
    'CL=F': 'CL',
    'HG=F': 'HG',
    'NQ=F': 'NQ',
    'SI=F': 'SI',
}

# 新浪外汇 K 线映射
SINA_KLINE_FX = {
    'JPY=X': 'USDJPY',
}

# ============ Tradingster COT 配置 ============

TRADINGSTER_BASE = 'https://www.tradingster.com/api/cot/legacy-futures/'
COT_CONTRACTS = {
    '13874A': 'S&P 500 E-mini',
    '209742': 'Nasdaq 100',
    '088691': 'Gold',
    '067651': 'Crude Oil',
    '020601': 'US Treasury',
}

# ============ 数据缓存 ============
cache = {
    'quotes': {},        # 实时行情
    'klines': {},        # K线历史
    'cot': {},           # COT 持仓
    'timestamps': {},    # 缓存时间戳
}
CACHE_TTL_QUOTES = 60       # 行情缓存 60秒
CACHE_TTL_KLINES = 3600     # K线缓存 1小时
CACHE_TTL_COT = 7200        # COT 缓存 2小时（反正一周才更新一次）


def make_request(url, referer=None, timeout=15):
    """通用 HTTP GET 请求"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    if referer:
        headers['Referer'] = referer
    req = urllib.request.Request(url, headers=headers)
    resp = urllib.request.urlopen(req, timeout=timeout)
    return resp.read()


def parse_sina_futures(raw_text, code):
    """解析新浪期货数据
    格式: var hq_str_hf_GC="4524.026,,4525.500,4527.800,4773.300,4512.100,04:59:38,4640.500,4689.400,0,2,1,2026-03-21,纽约黄金,0";
    字段: 当前价,,开盘,最高?,最高,最低,时间,昨结算,昨收,?,买量,卖量,日期,名称,?
    """
    match = re.search(r'var hq_str_' + re.escape(code) + r'="([^"]*)"', raw_text)
    if not match or not match.group(1):
        return None
    fields = match.group(1).split(',')
    if len(fields) < 14:
        return None
    try:
        price = float(fields[0]) if fields[0] else None
        open_price = float(fields[2]) if fields[2] else None
        high = float(fields[4]) if fields[4] else None
        low = float(fields[5]) if fields[5] else None
        prev_settle = float(fields[7]) if fields[7] else None
        prev_close = float(fields[8]) if fields[8] else None
        date = fields[12] if len(fields) > 12 else ''
        ref = prev_settle or prev_close
        change_pct = ((price - ref) / ref * 100) if price and ref else 0
        return {
            'price': price,
            'open': open_price,
            'high': high,
            'low': low,
            'prevClose': ref,
            'change': round(change_pct, 2),
            'date': date,
            'source': 'sina_futures'
        }
    except (ValueError, IndexError):
        return None


def parse_sina_stock(raw_text, code):
    """解析新浪美股数据
    格式: var hq_str_gb_$inx="标普500指数,6506.4800,-1.51,2026-03-21 05:05:26,-100.0100,..."
    字段: 名称,价格,涨跌幅%,时间,涨跌额,开盘,开盘价(?),最低,最高52周,最低52周,...
    """
    match = re.search(r'var hq_str_' + re.escape(code) + r'="([^"]*)"', raw_text)
    if not match or not match.group(1):
        return None
    fields = match.group(1).split(',')
    if len(fields) < 8:
        return None
    try:
        price = float(fields[1]) if fields[1] else None
        change_pct = float(fields[2]) if fields[2] else 0
        change_amt = float(fields[4]) if fields[4] else 0
        open_price = float(fields[5]) if fields[5] else None
        high = float(fields[6]) if fields[6] else None
        low = float(fields[7]) if fields[7] else None
        prev_close = price - change_amt if price and change_amt else None
        return {
            'price': price,
            'open': open_price,
            'high': high,
            'low': low,
            'prevClose': prev_close,
            'change': round(change_pct, 2),
            'source': 'sina_stock'
        }
    except (ValueError, IndexError):
        return None


def parse_sina_fx(raw_text, code):
    """解析新浪外汇数据
    格式: var hq_str_fx_susdjpy="04:59:59,159.220000,159.240000,..."
    字段: 时间,买价,卖价,最新价,?,昨收,最高,最低,...
    """
    match = re.search(r'var hq_str_' + re.escape(code) + r'="([^"]*)"', raw_text)
    if not match or not match.group(1):
        return None
    fields = match.group(1).split(',')
    if len(fields) < 8:
        return None
    try:
        price = float(fields[3]) if fields[3] else float(fields[1])
        prev_close = float(fields[5]) if fields[5] else None
        high = float(fields[6]) if fields[6] else None
        low = float(fields[7]) if fields[7] else None
        change_pct = ((price - prev_close) / prev_close * 100) if price and prev_close else 0
        return {
            'price': price,
            'prevClose': prev_close,
            'high': high,
            'low': low,
            'change': round(change_pct, 2),
            'source': 'sina_fx'
        }
    except (ValueError, IndexError):
        return None


def fetch_quotes(symbols):
    """批量获取行情数据"""
    results = {}
    now = time.time()

    # 分组：期货、美股、外汇
    sina_codes = []
    symbol_map = {}  # sina_code -> original_symbol

    for sym in symbols:
        # 检查缓存
        if sym in cache['quotes'] and now - cache['timestamps'].get(f'q_{sym}', 0) < CACHE_TTL_QUOTES:
            results[sym] = cache['quotes'][sym]
            continue

        if sym in SINA_FUTURES:
            code = SINA_FUTURES[sym]
            sina_codes.append(code)
            symbol_map[code] = sym
        elif sym in SINA_STOCKS:
            code = SINA_STOCKS[sym]
            sina_codes.append(code)
            symbol_map[code] = sym
        elif sym in SINA_FX:
            code = SINA_FX[sym]
            sina_codes.append(code)
            symbol_map[code] = sym

    if not sina_codes:
        return results

    # 一次请求多个品种
    try:
        url = f'https://hq.sinajs.cn/list={",".join(sina_codes)}'
        raw = make_request(url, referer='https://finance.sina.com.cn')
        text = raw.decode('gb18030', errors='replace')

        for code, sym in symbol_map.items():
            parsed = None
            if code.startswith('hf_'):
                parsed = parse_sina_futures(text, code)
            elif code.startswith('gb_'):
                parsed = parse_sina_stock(text, code)
            elif code.startswith('fx_'):
                parsed = parse_sina_fx(text, code)

            if parsed:
                parsed['symbol'] = sym
                results[sym] = parsed
                cache['quotes'][sym] = parsed
                cache['timestamps'][f'q_{sym}'] = now
    except Exception as e:
        print(f'[ERROR] fetch_quotes: {e}')

    return results


def fetch_klines(symbol, num=60):
    """获取 K 线历史数据
    支持: 美股（新浪美股K线）、期货（新浪期货K线）、外汇（新浪外汇K线）
    """
    now = time.time()
    cache_key = f'kline_{symbol}_{num}'
    if cache_key in cache['klines'] and now - cache['timestamps'].get(cache_key, 0) < CACHE_TTL_KLINES:
        return cache['klines'][cache_key]

    # 美股 K 线
    sina_symbol = SINA_KLINE_SYMBOL.get(symbol)
    if sina_symbol:
        return _fetch_klines_stock(symbol, sina_symbol, num, cache_key, now)

    # 期货 K 线
    futures_symbol = SINA_KLINE_FUTURES.get(symbol)
    if futures_symbol:
        return _fetch_klines_futures(symbol, futures_symbol, num, cache_key, now)

    # 外汇 K 线
    fx_symbol = SINA_KLINE_FX.get(symbol)
    if fx_symbol:
        return _fetch_klines_fx(symbol, fx_symbol, num, cache_key, now)

    return None


def _fetch_klines_stock(symbol, sina_symbol, num, cache_key, now):
    """新浪美股 K 线"""
    try:
        url = f'https://stock.finance.sina.com.cn/usstock/api/jsonp_v2.php/var%20_=1/US_MinKService.getDailyK?symbol={sina_symbol}&type=daily&num={num}'
        raw = make_request(url, referer='https://finance.sina.com.cn')
        text = raw.decode('utf-8', errors='replace')

        match = re.search(r'\[.*\]', text, re.DOTALL)
        if not match:
            return None

        data = json.loads(match.group(0))
        klines = []
        for item in data:
            klines.append({
                'date': item.get('d', ''),
                'open': float(item.get('o', 0)),
                'high': float(item.get('h', 0)),
                'low': float(item.get('l', 0)),
                'close': float(item.get('c', 0)),
                'volume': int(item.get('v', 0)),
            })

        # 新浪美股接口会忽略 num 参数返回全部历史，需要手动截断
        klines = klines[-num:]
        cache['klines'][cache_key] = klines
        cache['timestamps'][cache_key] = now
        return klines
    except Exception as e:
        print(f'[ERROR] _fetch_klines_stock {symbol}: {e}')
        return None


def _fetch_klines_futures(symbol, sina_code, num, cache_key, now):
    """新浪外盘期货 K 线
    接口: https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var _=/GlobalFuturesService.getGlobalFuturesDailyKLine?symbol=GC
    返回: [{date:"2026-03-21",open:"3018.0",high:"3020.5",low:"3002.2",close:"3010.8",...}, ...]
    """
    try:
        url = f'https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var%20_=/GlobalFuturesService.getGlobalFuturesDailyKLine?symbol={sina_code}&_={int(now)}'
        raw = make_request(url, referer='https://finance.sina.com.cn')
        text = raw.decode('utf-8', errors='replace')

        match = re.search(r'\[.*\]', text, re.DOTALL)
        if not match:
            return None

        data = json.loads(match.group(0))
        klines = []
        for item in data:
            klines.append({
                'date': item.get('date', item.get('d', '')),
                'open': float(item.get('open', item.get('o', 0))),
                'high': float(item.get('high', item.get('h', 0))),
                'low': float(item.get('low', item.get('l', 0))),
                'close': float(item.get('close', item.get('c', 0))),
                'volume': int(float(item.get('volume', item.get('v', 0)))),
            })

        # 只取最近 num 条
        klines = klines[-num:]
        cache['klines'][cache_key] = klines
        cache['timestamps'][cache_key] = now
        return klines
    except Exception as e:
        print(f'[ERROR] _fetch_klines_futures {symbol}: {e}')
        return None


def _fetch_klines_fx(symbol, sina_code, num, cache_key, now):
    """新浪外汇 K 线 — 目前不可用，返回 None"""
    # 新浪外汇 K 线接口 (NewService.getDailyKLine) 已下线
    # 外汇品种暂不支持历史 K 线
    print(f'[WARN] FX kline not available for {symbol}')
    return None


def fetch_cot(contract_code):
    """从 Tradingster 获取 COT 数据"""
    now = time.time()
    cache_key = f'cot_{contract_code}'
    # 禁用缓存强制重新拉取（测试用）
    # if cache_key in cache['cot'] and now - cache['timestamps'].get(cache_key, 0) < CACHE_TTL_COT:
    #     return cache['cot'][cache_key]

    try:
        url = f'{TRADINGSTER_BASE}{contract_code}'
        # 添加 Referer 避免 Tradingster 缓存问题
        raw = make_request(url, referer='https://www.tradingster.com/cot/legacy-futures/')
        data = json.loads(raw.decode('utf-8'))

        if not data or not isinstance(data, list):
            return None

        # 取最近 20 条（约 20 周）
        recent = data[-20:]
        result = {
            'contract': contract_code,
            'name': COT_CONTRACTS.get(contract_code, contract_code),
            'data': []
        }

        for row in recent:
            long_pos = row.get('Noncommercial_Positions_Long_All', 0)
            short_pos = row.get('Noncommercial_Positions_Short_All', 0)
            result['data'].append({
                'date': row.get('As_of_Date', ''),
                'longPos': long_pos,
                'shortPos': short_pos,
                'netPos': long_pos - short_pos,
            })

        cache['cot'][cache_key] = result
        cache['timestamps'][cache_key] = now
        return result
    except Exception as e:
        print(f'[ERROR] fetch_cot {contract_code}: {e}')
        traceback.print_exc()
        return None


def fetch_all_cot():
    """获取所有合约的 COT 数据"""
    results = {}
    for code in COT_CONTRACTS:
        data = fetch_cot(code)
        if data:
            results[code] = data
    return results


# ============ 食物链监控数据 ============

def get_foodchain_data():
    """获取食物链监控面板数据"""
    from datetime import datetime, date
    import calendar

    now = datetime.now()
    current_date = now.date()
    current_month = now.month
    current_year = now.year

    # 1. 计算 OPEX 日（每月第三个周五）
    def get_opex_date(year, month):
        # 获取该月所有周五
        fridays = []
        for day in range(1, calendar.monthrange(year, month)[1] + 1):
            if date(year, month, day).weekday() == 4:  # 0=周一, 4=周五
                fridays.append(day)
        return fridays[2] if len(fridays) >= 3 else fridays[-1]

    # 如果本月 OPEX 已过，计算下个月
    opex_day = get_opex_date(current_year, current_month)
    opex_date = date(current_year, current_month, opex_day)
    days_to_opex = (opex_date - current_date).days

    if days_to_opex < 0:
        next_month = (current_month % 12) + 1
        next_year = current_year + (1 if next_month == 1 else 0)
        next_opex_day = get_opex_date(next_year, next_month)
        opex_date = date(next_year, next_month, next_opex_day)
        days_to_opex = (opex_date - current_date).days

    # 2. 计算季末再平衡日（3/6/9/12月25日左右）
    def get_rebalance_dates(year, month):
        # 季末前5天开始预警
        if month in [3, 6, 9, 12]:
            rebalance_day = 25
            rebalance_date = date(year, month, rebalance_day)
            if rebalance_date > current_date:
                days_to_rebalance = (rebalance_date - current_date).days
                return {'date': rebalance_date.strftime('%Y-%m-%d'), 'days': days_to_rebalance}
        return None

    # 当前季度和下季度的再平衡日
    rebalance_current = get_rebalance_dates(current_year, current_month)
    next_month = (current_month % 12) + 1
    next_year = current_year + (1 if next_month == 1 else 0)
    rebalance_next = get_rebalance_dates(next_year, next_month)
    rebalance_dates = [d for d in [rebalance_current, rebalance_next] if d]

    # 3. 获取 COT 数据（CTA 持仓）
    cot_data = {}
    try:
        spx_cot = fetch_cot('13874A')  # SPX
        if spx_cot and spx_cot.get('data'):
            latest = spx_cot['data'][0]
            net_pos = latest['netPos']
            historical = spx_cot['data'][:12]  # 最近12周
            net_positions = [d['netPos'] for d in historical]
            avg_net = sum(net_positions) / len(net_positions)

            # 判断 CTA 仓位状态
            if net_pos > avg_net * 1.2:
                cta_status = 'extreme_long'
                cta_signal = '⚠️ CTA 极度看多 - 可能反转'
            elif net_pos < avg_net * 0.8:
                cta_status = 'extreme_short'
                cta_signal = '⚠️ CTA 极度看空 - 可能反转'
            elif net_pos > avg_net:
                cta_status = 'long'
                cta_signal = '📈 CTA 看多'
            else:
                cta_status = 'short'
                cta_signal = '📉 CTA 看空'

            cot_data = {
                'contract': 'SPX',
                'latestDate': latest['date'],
                'netPosition': net_pos,
                'avgNetPosition': round(avg_net, 0),
                'status': cta_status,
                'signal': cta_signal,
                'trend': 'up' if net_pos > net_positions[-4] else 'down'  # 与4周前对比
            }
    except Exception as e:
        print(f'[ERROR] get CTA signal: {e}')
        cot_data = {'error': str(e)}

    # 4. 散户情绪（模拟数据，实际可接入 CNN Fear & Greed）
    # 这里用模拟数据，因为需要付费 API
    sentiment_data = {
        'fearGreed': 45,  # 0-100, <25 极度恐惧, >75 极度贪婪
        'status': 'neutral',
        'signal': '😐 情绪中性 - 无明显方向'
    }
    if sentiment_data['fearGreed'] < 25:
        sentiment_data['status'] = 'extreme_fear'
        sentiment_data['signal'] = '😰 极度恐惧 - 或是买入机会'
    elif sentiment_data['fearGreed'] > 75:
        sentiment_data['status'] = 'extreme_greed'
        sentiment_data['signal'] = '🤑 极度贪婪 - 或是卖出信号'
    elif sentiment_data['fearGreed'] < 45:
        sentiment_data['status'] = 'fear'
        sentiment_data['signal'] = '😨 恐惧 - 谨慎乐观'
    elif sentiment_data['fearGreed'] > 55:
        sentiment_data['status'] = 'greed'
        sentiment_data['signal'] = '😃 贪婪 - 注意风险'

    # 5. Gamma Wall（模拟数据，实际可接入 SpotGamma API）
    gamma_data = {
        'status': 'positive',  # positive/negative
        'levels': [
            {'level': 5200, 'type': 'call_wall', 'strength': 'high'},
            {'level': 5100, 'type': 'put_wall', 'strength': 'medium'}
        ],
        'signal': '📊 正 Gamma - 波动被压制，市场稳定'
    }

    # 组装返回数据
    return {
        'timestamp': now.strftime('%Y-%m-%d %H:%M:%S'),
        'opex': {
            'date': opex_date.strftime('%Y-%m-%d'),
            'daysAway': days_to_opex,
            'signal': '📅 下次 OPEX 还有 {} 天'.format(days_to_opex) if days_to_opex > 0 else '📅 今日 OPEX - 波动可能放大'
        },
        'rebalancing': {
            'dates': rebalance_dates,
            'signal': f'🔄 季末再平衡: {len(rebalance_dates)} 个预警日'
        },
        'cta': cot_data,
        'sentiment': sentiment_data,
        'gamma': gamma_data,
        'alerts': generate_foodchain_alerts(opex_date, rebalance_dates, cot_data, sentiment_data)
    }


def generate_foodchain_alerts(opex_date, rebalance_dates, cot_data, sentiment_data):
    """生成食物链预警信息"""
    alerts = []

    # OPEX 预警（只在临近 3 天内预警）
    if opex_date:
        days_to_opex = (opex_date - date.today()).days
        if days_to_opex >= 0 and days_to_opex <= 3:
            alerts.append({
                'level': 'warning',
                'source': 'OPEX',
                'message': f'OPEX 还有 {days_to_opex} 天，波动可能放大'
            })

    # 季末再平衡预警
    for rebalance in rebalance_dates:
        if rebalance and rebalance['days'] <= 5:
            alerts.append({
                'level': 'warning',
                'source': 'Rebalancing',
                'message': f'季末再平衡还有 {rebalance["days"]} 天'
            })

    # CTA 极端仓位预警
    if cot_data.get('status') in ['extreme_long', 'extreme_short']:
        alerts.append({
            'level': 'danger',
            'source': 'CTA',
            'message': cot_data.get('signal', 'CTA 极端仓位')
        })

    # 散户情绪极端预警
    if sentiment_data.get('status') in ['extreme_fear', 'extreme_greed']:
        alerts.append({
            'level': 'info',
            'source': 'Sentiment',
            'message': sentiment_data.get('signal', '散户情绪极端')
        })

    return alerts


# ============ HTTP 服务 ============

class ProxyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)

        # CORS headers
        self.send_response(200)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', '*')
        self.send_header('Cache-Control', 'no-cache')

        try:
            if path == '/api/quotes':
                # 批量行情: /api/quotes?symbols=^GSPC,ES=F,GC=F,...
                symbols = params.get('symbols', [''])[0].split(',')
                symbols = [s.strip() for s in symbols if s.strip()]
                data = fetch_quotes(symbols)
                self._send_json({'ok': True, 'data': data})

            elif path == '/api/klines':
                # K线: /api/klines?symbol=^GSPC&num=60
                symbol = params.get('symbol', ['^GSPC'])[0]
                num = int(params.get('num', ['60'])[0])
                data = fetch_klines(symbol, num)
                if data:
                    self._send_json({'ok': True, 'data': data})
                else:
                    self._send_json({'ok': False, 'error': f'无法获取 {symbol} K线数据'})

            elif path == '/api/cot':
                # COT: /api/cot 返回全部 or /api/cot?contract=13874A
                contract = params.get('contract', [None])[0]
                if contract:
                    data = fetch_cot(contract)
                    if data:
                        self._send_json({'ok': True, 'data': {contract: data}})
                    else:
                        self._send_json({'ok': False, 'error': f'无法获取 {contract} COT 数据'})
                else:
                    data = fetch_all_cot()
                    self._send_json({'ok': True, 'data': data})

            elif path == '/api/health':
                self._send_json({'ok': True, 'port': PORT, 'cache_keys': len(cache['timestamps'])})

            elif path == '/api/foodchain':
                # 食物链监控面板数据
                data = get_foodchain_data()
                self._send_json({'ok': True, 'data': data})

            else:
                self._send_json({'ok': False, 'error': f'未知路径: {path}',
                                 'routes': ['/api/quotes', '/api/klines', '/api/cot', '/api/health', '/api/foodchain']})

        except Exception as e:
            print(f'[ERROR] {path}: {e}')
            traceback.print_exc()
            self._send_json({'ok': False, 'error': str(e)})

    def do_OPTIONS(self):
        """CORS preflight"""
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', '*')
        self.end_headers()

    def _send_json(self, data):
        body = json.dumps(data, ensure_ascii=False).encode('utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        # 简化日志
        print(f'[{self.log_date_time_string()}] {args[0]}')


def main():
    print(f'''
╔══════════════════════════════════════════════╗
║   CTA 数据代理服务 v1.0                       ║
║   端口: {PORT}                                 ║
║                                              ║
║   API 端点:                                   ║
║   GET /api/quotes?symbols=^GSPC,ES=F,GC=F   ║
║   GET /api/klines?symbol=^GSPC&num=60        ║
║   GET /api/cot                               ║
║   GET /api/health                            ║
║                                              ║
║   数据源: 新浪财经 + Tradingster              ║
║   Ctrl+C 退出                                ║
╚══════════════════════════════════════════════╝
''')

    server = HTTPServer(('0.0.0.0', PORT), ProxyHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\n代理服务已停止')
        server.server_close()


if __name__ == '__main__':
    main()
