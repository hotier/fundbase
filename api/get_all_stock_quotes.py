"""
获取港股和A股混合实时行情数据
支持腾讯证券、新浪财经、网易财经、雪球接口
"""

import requests
import pandas as pd
from datetime import datetime
import random
import time


# User-Agent池，模拟不同浏览器
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]

# 代理池配置
# 格式: {'http': 'http://proxy_ip:port', 'https': 'https://proxy_ip:port'}
# 如果没有代理，设置为None或空列表
PROXY_POOL = [
    # 示例代理配置（请替换为实际可用的代理）
    # {'http': 'http://127.0.0.1:7890', 'https': 'http://127.0.0.1:7890'},
    # {'http': 'http://proxy1.example.com:8080', 'https': 'http://proxy1.example.com:8080'},
    # {'http': 'http://proxy2.example.com:8080', 'https': 'http://proxy2.example.com:8080'},
]

# 是否启用代理
ENABLE_PROXY = False  # 设置为True时启用代理池


def get_random_headers(referer):
    """获取随机请求头"""
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Referer': referer,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0',
    }


def get_random_proxy():
    """获取随机代理"""
    if not ENABLE_PROXY or not PROXY_POOL:
        return None
    return random.choice(PROXY_POOL)


def random_delay(min_delay=0.1, max_delay=0.5):
    """随机延迟"""
    time.sleep(random.uniform(min_delay, max_delay))


class TencentRealtime:
    """腾讯证券实时行情接口（支持港股和A股）"""
    
    def __init__(self):
        self.base_url = "http://qt.gtimg.cn/q="
        self.session = requests.Session()
        # 设置连接池大小
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=3
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
    
    def get_multiple_stocks(self, codes):
        """
        批量获取多只股票行情（支持港股和A股混合）
        
        参数:
            codes: 代码列表，如 ['600519', '000858', '00700', '09988']
        
        返回: DataFrame
        """
        all_data = []
        
        if not codes:
            return pd.DataFrame()
        
        # 一次性请求所有股票
        symbols = []
        for code in codes:
            if len(code) == 5 and code.startswith('0'):  # 港股
                symbols.append(f"hk{code}")
            elif code.startswith('60') or code.startswith('90') or code.startswith('68'):  # 上海A股
                symbols.append(f"sh{code}")
            elif code.startswith('8') or code.startswith('9'):  # 北交所股票
                symbols.append(f"bj{code}")
            else:  # 深圳A股
                symbols.append(f"sz{code}")
        
        query_str = ','.join(symbols)
        url = f"{self.base_url}{query_str}"
        
        try:
            # 随机延迟
            random_delay(0.1, 0.3)
            
            # 使用随机请求头和代理
            headers = get_random_headers('http://gu.qq.com/')
            proxies = get_random_proxy()
            
            response = self.session.get(url, headers=headers, proxies=proxies, timeout=10)
            response.encoding = 'gbk'
            lines = response.text.strip().split(';')
            
            for line in lines:
                if '=' in line:
                    stock_data = self._parse_line(line)
                    if stock_data and stock_data['code'] in codes:
                        all_data.append(stock_data)
                    
        except Exception as e:
            print(f"腾讯证券批量获取失败: {e}")
        
        return pd.DataFrame(all_data) if all_data else pd.DataFrame()
    
    def _parse_line(self, line):
        """解析单行数据"""
        try:
            line = line.strip()
            if not line:
                return None
            
            code_part, data_part = line.split('=')
            code = code_part.replace('v_hk', '').replace('v_sh', '').replace('v_sz', '').replace('v_bj', '').strip()
            raw_str = data_part.strip('"')
            parts = raw_str.split('~')
            
            if len(parts) > 33:
                current_price = self._safe_float(parts[3])
                previous_close = self._safe_float(parts[4])
                change_amount = current_price - previous_close
                
                return {
                    'code': code,          # 代码
                    'name': parts[1] or 'N/A',          # 名称
                    'current': current_price,      # 当前价
                    'change': round(change_amount, 3),       # 涨跌额
                    'change_pct': parts[32] if parts[32] else "0",         # 涨跌幅%
                    'open': self._safe_float(parts[4]),             # 今开
                    'high': self._safe_float(parts[5]),             # 最高
                    'low': self._safe_float(parts[6]),              # 最低
                    'volume': self._safe_int(parts[6]),             # 成交量
                    'turnover': self._safe_float(parts[37]),         # 成交额
                    'time': parts[30] if parts[30] else ''
                }
        except:
            pass
        return None
    
    def _safe_float(self, value):
        """安全转换为float"""
        try:
            if value in ['', '--', None]:
                return 0.0
            return float(value)
        except:
            return 0.0
    
    def _safe_int(self, value):
        """安全转换为int"""
        try:
            if value in ['', '--', None]:
                return 0
            return int(float(value))
        except:
            return 0


class SinaRealtime:
    """新浪财经实时行情接口"""
    
    def __init__(self):
        self.base_url = "http://hq.sinajs.cn/list="
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=3
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
    
    def get_multiple_stocks(self, codes):
        """
        批量获取多只股票行情
        
        参数:
            codes: 代码列表，如 ['600519', '000858', '00700', '09988']
        
        返回: DataFrame
        """
        all_data = []
        
        if not codes:
            return pd.DataFrame()
        
        # 构建新浪接口代码格式
        symbols = []
        for code in codes:
            if len(code) == 5 and code.startswith('0'):  # 港股
                symbols.append(f"hk{code}")
            elif code.startswith('60') or code.startswith('90') or code.startswith('68'):  # 上海A股
                symbols.append(f"sh{code}")
            elif code.startswith('8') or code.startswith('9'):  # 北交所股票
                symbols.append(f"bj{code}")
            else:  # 深圳A股
                symbols.append(f"sz{code}")
        
        query_str = ','.join(symbols)
        url = f"{self.base_url}{query_str}"
        
        try:
            # 随机延迟
            random_delay(0.1, 0.3)
            
            # 使用随机请求头和代理
            headers = get_random_headers('http://finance.sina.com.cn/')
            proxies = get_random_proxy()
            
            response = self.session.get(url, headers=headers, proxies=proxies, timeout=10)
            response.encoding = 'gbk'
            lines = response.text.strip().split(';')
            
            for line in lines:
                if '=' in line:
                    stock_data = self._parse_line(line)
                    if stock_data and stock_data['code'] in codes:
                        all_data.append(stock_data)
                    
        except Exception as e:
            print(f"新浪财经批量获取失败: {e}")
        
        return pd.DataFrame(all_data) if all_data else pd.DataFrame()
    
    def _parse_line(self, line):
        """解析单行数据"""
        try:
            line = line.strip()
            if not line:
                return None
            
            code_part, data_part = line.split('=')
            code = code_part.replace('var hq_str_', '').strip()
            raw_str = data_part.strip('"')
            parts = raw_str.split(',')
            
            if len(parts) > 32:
                current_price = self._safe_float(parts[1])
                previous_close = self._safe_float(parts[2])
                change_amount = current_price - previous_close
                change_pct = self._safe_float(parts[3])
                
                return {
                    'code': code,
                    'name': parts[0] or 'N/A',
                    'current': current_price,
                    'change': round(change_amount, 3),
                    'change_pct': f"{change_pct:.2f}",
                    'open': self._safe_float(parts[5]),
                    'high': self._safe_float(parts[6]),
                    'low': self._safe_float(parts[7]),
                    'volume': self._safe_int(parts[8]),
                    'turnover': self._safe_float(parts[9]),
                    'time': datetime.now().strftime('%Y/%m/%d %H:%M:%S')
                }
        except:
            pass
        return None
    
    def _safe_float(self, value):
        """安全转换为float"""
        try:
            if value in ['', '--', None]:
                return 0.0
            return float(value)
        except:
            return 0.0
    
    def _safe_int(self, value):
        """安全转换为int"""
        try:
            if value in ['', '--', None]:
                return 0
            return int(float(value))
        except:
            return 0


class NetEaseRealtime:
    """网易财经实时行情接口"""
    
    def __init__(self):
        self.base_url = "http://api.money.126.net/data/feed/"
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=3
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
    
    def get_multiple_stocks(self, codes):
        """
        批量获取多只股票行情
        
        参数:
            codes: 代码列表，如 ['600519', '000858', '00700', '09988']
        
        返回: DataFrame
        """
        all_data = []
        
        if not codes:
            return pd.DataFrame()
        
        # 构建网易接口代码格式
        symbols = []
        for code in codes:
            if len(code) == 5 and code.startswith('0'):  # 港股
                symbols.append(f"0{code}")
            elif code.startswith('60') or code.startswith('90') or code.startswith('68'):  # 上海A股
                symbols.append(f"0{code}")
            elif code.startswith('8') or code.startswith('9'):  # 北交所股票
                symbols.append(f"0{code}")
            else:  # 深圳A股
                symbols.append(f"1{code}")
        
        query_str = ','.join(symbols)
        url = f"{self.base_url}{query_str}"
        
        try:
            # 随机延迟
            random_delay(0.1, 0.3)
            
            # 使用随机请求头和代理
            headers = get_random_headers('http://money.163.com/')
            proxies = get_random_proxy()
            
            response = self.session.get(url, headers=headers, proxies=proxies, timeout=10)
            response.encoding = 'utf-8'
            content = response.text
            
            # 网易返回JSONP格式，需要解析
            import re
            pattern = r'_ntes_quote_callback\((.*?)\);'
            match = re.search(pattern, content)
            
            if match:
                json_str = match.group(1)
                import json
                data = json.loads(json_str)
                
                for code, stock_data in data.items():
                    if stock_data:
                        code_clean = code[1:]  # 去掉前缀
                        if code_clean in codes:
                            current_price = self._safe_float(stock_data.get('price', 0))
                            previous_close = self._safe_float(stock_data.get('yestclose', 0))
                            change_amount = current_price - previous_close
                            change_pct = self._safe_float(stock_data.get('percent', 0))
                            
                            all_data.append({
                                'code': code_clean,
                                'name': stock_data.get('name', 'N/A'),
                                'current': current_price,
                                'change': round(change_amount, 3),
                                'change_pct': f"{change_pct:.2f}",
                                'open': self._safe_float(stock_data.get('open', 0)),
                                'high': self._safe_float(stock_data.get('high', 0)),
                                'low': self._safe_float(stock_data.get('low', 0)),
                                'volume': self._safe_int(stock_data.get('volume', 0)),
                                'turnover': self._safe_float(stock_data.get('turnover', 0)),
                                'time': datetime.now().strftime('%Y/%m/%d %H:%M:%S')
                            })
                    
        except Exception as e:
            print(f"网易财经批量获取失败: {e}")
        
        return pd.DataFrame(all_data) if all_data else pd.DataFrame()
    
    def _safe_float(self, value):
        """安全转换为float"""
        try:
            if value in ['', '--', None]:
                return 0.0
            return float(value)
        except:
            return 0.0
    
    def _safe_int(self, value):
        """安全转换为int"""
        try:
            if value in ['', '--', None]:
                return 0
            return int(float(value))
        except:
            return 0


class XueqiuRealtime:
    """雪球实时行情接口"""
    
    def __init__(self):
        self.base_url = "https://stock.xueqiu.com/v5/stock/batch/quote.json"
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=3
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
    
    def get_multiple_stocks(self, codes):
        """
        批量获取多只股票行情
        
        参数:
            codes: 代码列表，如 ['600519', '000858', '00700', '09988']
        
        返回: DataFrame
        """
        all_data = []
        
        if not codes:
            return pd.DataFrame()
        
        # 构建雪球接口代码格式
        symbols = []
        for code in codes:
            if len(code) == 5 and code.startswith('0'):  # 港股
                symbols.append(f"HK{code}")
            elif code.startswith('60') or code.startswith('90') or code.startswith('68'):  # 上海A股
                symbols.append(f"SH{code}")
            elif code.startswith('8') or code.startswith('9'):  # 北交所股票
                symbols.append(f"BJ{code}")
            else:  # 深圳A股
                symbols.append(f"SZ{code}")
        
        params = {
            'symbol': ','.join(symbols),
            'extend': 'detail'
        }
        
        try:
            # 随机延迟
            random_delay(0.1, 0.3)
            
            # 使用随机请求头和代理
            headers = get_random_headers('https://xueqiu.com/')
            proxies = get_random_proxy()
            
            response = self.session.get(self.base_url, headers=headers, params=params, proxies=proxies, timeout=10)
            response.encoding = 'utf-8'
            data = response.json()
            
            if data.get('data') and data['data'].get('items'):
                for item in data['data']['items']:
                    quote = item.get('quote', {})
                    symbol = quote.get('symbol', '')
                    code_clean = symbol[2:] if len(symbol) > 2 else symbol
                    
                    if code_clean in codes:
                        current_price = self._safe_float(quote.get('current', 0))
                        previous_close = self._safe_float(quote.get('last_close', 0))
                        change_amount = current_price - previous_close
                        change_pct = self._safe_float(quote.get('percent', 0))
                        
                        all_data.append({
                            'code': code_clean,
                            'name': quote.get('name', 'N/A'),
                            'current': current_price,
                            'change': round(change_amount, 3),
                            'change_pct': f"{change_pct:.2f}",
                            'open': self._safe_float(quote.get('open', 0)),
                            'high': self._safe_float(quote.get('high', 0)),
                            'low': self._safe_float(quote.get('low', 0)),
                            'volume': self._safe_int(quote.get('volume', 0)),
                            'turnover': self._safe_float(quote.get('turnover', 0)),
                            'time': datetime.now().strftime('%Y/%m/%d %H:%M:%S')
                        })
                    
        except Exception as e:
            print(f"雪球批量获取失败: {e}")
        
        return pd.DataFrame(all_data) if all_data else pd.DataFrame()
    
    def _safe_float(self, value):
        """安全转换为float"""
        try:
            if value in ['', '--', None]:
                return 0.0
            return float(value)
        except:
            return 0.0
    
    def _safe_int(self, value):
        """安全转换为int"""
        try:
            if value in ['', '--', None]:
                return 0
            return int(float(value))
        except:
            return 0


def get_all_stock_quotes(stock_codes, timeout=10):
    """
    获取混合股票实时行情（支持港股和A股）
    支持腾讯证券、新浪财经、网易财经、雪球接口
    依次尝试每个接口，合并所有正确结果
    
    Parameters:
    -----------
    stock_codes : list
        股票代码列表（如 ['600519', '000858', '00700', '09988']）
    timeout : int
        超时时间（秒），默认10秒
        
    Returns:
    --------
    pd.DataFrame
        股票实时行情数据
    """
    print(f"正在获取 {len(stock_codes)} 只股票的实时行情...")
    
    if ENABLE_PROXY and PROXY_POOL:
        print(f"代理池已启用，共 {len(PROXY_POOL)} 个代理")
    
    all_results = []
    remaining_codes = stock_codes.copy()
    
    # 方法1: 腾讯证券
    print("尝试腾讯证券接口...")
    try:
        realtime = TencentRealtime()
        df = realtime.get_multiple_stocks(remaining_codes)
        
        if not df.empty:
            # 转换为统一格式
            for _, row in df.iterrows():
                change_pct_str = row['change_pct']
                if change_pct_str:
                    try:
                        change_pct = float(change_pct_str)
                    except:
                        change_pct = 0.0
                else:
                    change_pct = 0.0
                
                time_str = row['time']
                if len(time_str) == 14 and time_str.isdigit():
                    try:
                        dt = datetime.strptime(time_str, '%Y%m%d%H%M%S')
                        time_str = dt.strftime('%Y/%m/%d %H:%M:%S')
                    except:
                        pass
                
                all_results.append({
                    '代码': row['code'],
                    '名称': row['name'],
                    '最新价': row['current'],
                    '涨跌': row['change'],
                    '涨跌幅': f"{change_pct:.2f}%",
                    '时间': time_str,
                    '成交量': row['volume'],
                    '成交额': row['turnover']
                })
            
            # 更新剩余未查询到的代码
            found_codes = set(row['code'] for _, row in df.iterrows())
            remaining_codes = [code for code in remaining_codes if code not in found_codes]
            print(f"腾讯证券成功: 获取 {len(found_codes)}/{len(stock_codes)} 只股票行情")
        else:
            print("腾讯证券: 未获取到股票数据")
                
    except Exception as e:
        print(f"腾讯证券接口失败: {e}")
    
    # 方法2: 新浪财经（查询剩余代码）
    if remaining_codes:
        print(f"尝试新浪财经接口（剩余 {len(remaining_codes)} 只股票）...")
        try:
            sina = SinaRealtime()
            df = sina.get_multiple_stocks(remaining_codes)
            
            if not df.empty:
                for _, row in df.iterrows():
                    change_pct_str = row['change_pct']
                    try:
                        change_pct = float(change_pct_str)
                    except:
                        change_pct = 0.0
                    
                    all_results.append({
                        '代码': row['code'],
                        '名称': row['name'],
                        '最新价': row['current'],
                        '涨跌': row['change'],
                        '涨跌幅': f"{change_pct:.2f}%",
                        '时间': row['time'],
                        '成交量': row['volume'],
                        '成交额': row['turnover']
                    })
                
                found_codes = set(row['code'] for _, row in df.iterrows())
                remaining_codes = [code for code in remaining_codes if code not in found_codes]
                print(f"新浪财经成功: 获取 {len(found_codes)} 只股票行情")
            else:
                print("新浪财经: 未获取到股票数据")
                
        except Exception as e:
            print(f"新浪财经接口失败: {e}")
    
    # 方法3: 网易财经（查询剩余代码）
    if remaining_codes:
        print(f"尝试网易财经接口（剩余 {len(remaining_codes)} 只股票）...")
        try:
            netease = NetEaseRealtime()
            df = netease.get_multiple_stocks(remaining_codes)
            
            if not df.empty:
                for _, row in df.iterrows():
                    change_pct_str = row['change_pct']
                    try:
                        change_pct = float(change_pct_str)
                    except:
                        change_pct = 0.0
                    
                    all_results.append({
                        '代码': row['code'],
                        '名称': row['name'],
                        '最新价': row['current'],
                        '涨跌': row['change'],
                        '涨跌幅': f"{change_pct:.2f}%",
                        '时间': row['time'],
                        '成交量': row['volume'],
                        '成交额': row['turnover']
                    })
                
                found_codes = set(row['code'] for _, row in df.iterrows())
                remaining_codes = [code for code in remaining_codes if code not in found_codes]
                print(f"网易财经成功: 获取 {len(found_codes)} 只股票行情")
            else:
                print("网易财经: 未获取到股票数据")
                
        except Exception as e:
            print(f"网易财经接口失败: {e}")
    
    # 方法4: 雪球（查询剩余代码）
    if remaining_codes:
        print(f"尝试雪球接口（剩余 {len(remaining_codes)} 只股票）...")
        try:
            xueqiu = XueqiuRealtime()
            df = xueqiu.get_multiple_stocks(remaining_codes)
            
            if not df.empty:
                for _, row in df.iterrows():
                    change_pct_str = row['change_pct']
                    try:
                        change_pct = float(change_pct_str)
                    except:
                        change_pct = 0.0
                    
                    all_results.append({
                        '代码': row['code'],
                        '名称': row['name'],
                        '最新价': row['current'],
                        '涨跌': row['change'],
                        '涨跌幅': f"{change_pct:.2f}%",
                        '时间': row['time'],
                        '成交量': row['volume'],
                        '成交额': row['turnover']
                    })
                
                found_codes = set(row['code'] for _, row in df.iterrows())
                remaining_codes = [code for code in remaining_codes if code not in found_codes]
                print(f"雪球成功: 获取 {len(found_codes)} 只股票行情")
            else:
                print("雪球: 未获取到股票数据")
                
        except Exception as e:
            print(f"雪球接口失败: {e}")
    
    # 合并所有结果
    if all_results:
        result = pd.DataFrame(all_results)
        print(f"\n总计成功获取 {len(result)}/{len(stock_codes)} 只股票行情")
        if remaining_codes:
            print(f"未获取到的股票: {', '.join(remaining_codes)}")
        return result
    else:
        print("\n所有接口均未获取到股票数据")
        return None


if __name__ == "__main__":
    print("="*60)
    print("港股和A股混合实时行情数据获取工具")
    print("="*60)
    
    # 输入股票代码
    input_str = input("请输入股票代码（多个代码用逗号分隔，例如：600519,000858,00700）: ").strip()
    stock_codes = [code.strip() for code in input_str.split(',') if code.strip()]
    
    if not stock_codes:
        print("股票代码不能为空")
        exit(1)
    
    # 获取行情数据
    quotes = get_all_stock_quotes(stock_codes, timeout=10)
    
    if quotes is None:
        print("获取股票行情失败")
        exit(1)
    
    print(f"\n成功获取 {len(quotes)} 只股票的实时行情")
    print("\n股票行情明细:")
    print("="*60)
    
    # 只显示用户要求的字段
    columns_to_show = ['代码', '名称', '最新价', '涨跌', '涨跌幅', '时间']
    # 确保只包含存在的列
    columns_to_show = [col for col in columns_to_show if col in quotes.columns]
    
    print(quotes[columns_to_show].to_string(index=False))
    
    # 保存到CSV
    save_choice = input("\n是否保存到CSV文件？(y/n): ").strip().lower()
    if save_choice == 'y':
        filename = f"all_quotes_{datetime.now().strftime('%Y%m%d')}.csv"
        quotes.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"数据已保存: {filename}")
    
    print("\n完成！")
