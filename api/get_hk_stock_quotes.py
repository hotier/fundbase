"""
获取港股实时行情数据
支持腾讯证券、新浪财经、网易财经等多个数据源
"""

import requests
import pandas as pd
import json
from datetime import datetime


class HKTencentRealtime:
    """腾讯港股实时行情接口"""
    
    def __init__(self):
        self.base_url = "http://qt.gtimg.cn/q="
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'http://gu.qq.com/'
        }
    
    def get_multiple_stocks(self, codes, batch_size=20):
        """
        批量获取多只港股行情
        
        参数:
            codes: 代码列表，如 ['00700', '09988', '00005']
            batch_size: 每批请求的数量（腾讯接口支持批量）
        
        返回: DataFrame
        """
        all_data = []
        
        if not codes:
            return pd.DataFrame()
        
        # 一次性请求所有股票
        symbols = [f"hk{code}" for code in codes]
        query_str = ','.join(symbols)
        url = f"{self.base_url}{query_str}"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.encoding = 'gbk'
            lines = response.text.strip().split(';')
            
            for line in lines:
                if '=' in line:
                    stock_data = self._parse_line(line)
                    if stock_data and stock_data['code'] in codes:
                        all_data.append(stock_data)
                    
        except Exception as e:
            print(f"批量获取失败: {e}")
        
        return pd.DataFrame(all_data) if all_data else pd.DataFrame()
    
    def _parse_line(self, line):
        """解析单行数据"""
        try:
            line = line.strip()
            if not line:
                return None
            
            code_part, data_part = line.split('=')
            code = code_part.replace('v_hk', '').strip()
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


def get_hk_quotes_tencent(stock_codes, timeout=10):
    """
    通过腾讯证券接口获取港股实时行情
    
    Parameters:
    -----------
    stock_codes : list
        港股代码列表（如 ['02600', '00700']）
    timeout : int
        超时时间（秒），默认10秒
        
    Returns:
    --------
    pd.DataFrame
        港股实时行情数据
    """
    print(f"尝试腾讯证券接口获取港股行情...")
    
    try:
        hk_realtime = HKTencentRealtime()
        df = hk_realtime.get_multiple_stocks(stock_codes)
        
        if not df.empty:
            # 转换为统一格式
            stock_list = []
            for _, row in df.iterrows():
                # 解析涨跌幅
                change_pct_str = row['change_pct']
                if change_pct_str:
                    # 直接使用数值，不需要添加额外的%符号
                    try:
                        change_pct = float(change_pct_str)
                    except:
                        change_pct = 0.0
                else:
                    change_pct = 0.0
                
                stock_list.append({
                    '代码': row['code'],
                    '名称': row['name'],
                    '最新价': row['current'],
                    '涨跌': row['change'],
                    '涨跌幅': f"{change_pct:.2f}%",
                    '时间': row['time'],
                    '成交量': row['volume'],
                    '成交额': row['turnover']
                })
            
            result = pd.DataFrame(stock_list)
            matched_count = len(result)
            print(f"腾讯证券成功: 获取 {matched_count}/{len(stock_codes)} 只港股行情")
            return result
        else:
            print("腾讯证券: 未获取到港股数据")
            return None
                
    except Exception as e:
        print(f"腾讯证券接口失败: {e}")
        return None


def get_hk_quotes_sina(stock_codes, timeout=10):
    """
    通过新浪财经接口获取港股实时行情
    
    Parameters:
    -----------
    stock_codes : list
        港股代码列表（如 ['02600', '00700']）
    timeout : int
        超时时间（秒），默认10秒
        
    Returns:
    --------
    pd.DataFrame
        港股实时行情数据
    """
    print(f"尝试新浪财经接口获取港股行情...")
    stock_list = []
    
    try:
        # 新浪财经港股接口
        url = "http://hq.sinajs.cn/list="
        symbols = []
        for code in stock_codes:
            symbols.append(f"rt_hk{code}")
        
        params = {
            '_': int(datetime.now().timestamp() * 1000)
        }
        
        response = requests.get(url + ','.join(symbols), params=params, timeout=timeout)
        response.encoding = 'gbk'
        data = response.text
        
        # 解析新浪返回的数据格式
        if data:
            items = data.strip().split('\n')
            for item in items:
                if 'var hq_str_' in item:
                    try:
                        # 提取数据部分
                        start = item.find('"')
                        end = item.rfind('"')
                        if start > 0 and end > start:
                            data_str = item[start+1:end]
                            parts = data_str.split(',')
                            
                            if len(parts) > 10:
                                stock_code = parts[0]
                                stock_name = parts[1]
                                current_price = float(parts[6]) if parts[6] else 0
                                open_price = float(parts[2]) if parts[2] else 0
                                close_price = float(parts[3]) if parts[3] else 0
                                volume = float(parts[8]) if parts[8] else 0
                                amount = float(parts[9]) if parts[9] else 0
                                
                                # 计算涨跌额和涨跌幅
                            change_amount = current_price - close_price
                            if close_price > 0:
                                change_percent = ((current_price - close_price) / close_price) * 100
                            else:
                                change_percent = 0
                            
                            stock_list.append({
                                '代码': stock_code,
                                '名称': stock_name,
                                '最新价': current_price,
                                '涨跌': change_amount,
                                '涨跌幅': f"{change_percent:.2f}%",
                                '时间': datetime.now().strftime('%H:%M:%S'),
                                '成交量': volume,
                                '成交额': amount
                            })
                    except Exception as e:
                        continue
            
            if stock_list:
                result = pd.DataFrame(stock_list)
                matched_count = len(result)
                print(f"新浪财经成功: 获取 {matched_count}/{len(stock_codes)} 只港股行情")
                return result
            else:
                print("新浪财经: 未获取到港股数据")
                return None
                
    except Exception as e:
        print(f"新浪财经接口失败: {e}")
        return None


def get_hk_quotes_163(stock_codes, timeout=10):
    """
    通过网易财经接口获取港股实时行情
    
    Parameters:
    -----------
    stock_codes : list
        港股代码列表（如 ['02600', '00700']）
    timeout : int
        超时时间（秒），默认10秒
        
    Returns:
    --------
    pd.DataFrame
        港股实时行情数据
    """
    print(f"尝试网易财经接口获取港股行情...")
    stock_list = []
    
    try:
        # 网易财经港股接口
        url = "http://api.money.126.net/data/feed/"
        symbols = []
        for code in stock_codes:
            symbols.append(f"0{code}")
        
        params = {
            'money': 'api',
            'callback': 'jsonp_callback'
        }
        
        response = requests.get(url + ','.join(symbols), params=params, timeout=timeout)
        data = response.text
        
        # 解析网易返回的JSONP格式数据
        if data and 'jsonp_callback' in data:
            try:
                # 提取JSON部分
                json_str = data.replace('jsonp_callback(', '').replace(');', '')
                json_data = json.loads(json_str)
                
                for code, stock_data in json_data.items():
                    if code.startswith('0'):
                        stock_code = code[1:]
                        if stock_code in stock_codes:
                            stock_name = stock_data.get('name', stock_code)
                            current_price = stock_data.get('price', 0)
                            open_price = stock_data.get('open', 0)
                            close_price = stock_data.get('yestclose', 0)
                            volume = stock_data.get('volume', 0)
                            amount = stock_data.get('turnover', 0)
                            
                            # 计算涨跌额和涨跌幅
                    change_amount = current_price - close_price
                    if close_price > 0:
                                    change_percent = ((current_price - close_price) / close_price) * 100
                    else:
                                    change_percent = 0
                                
                                    stock_list.append({
                                    '代码': stock_code,
                                    '名称': stock_name,
                                    '最新价': current_price,
                                    '涨跌': change_amount,
                                    '涨跌幅': f"{change_percent:.2f}%",
                                    '时间': datetime.now().strftime('%H:%M:%S'),
                                    '成交量': volume,
                                    '成交额': amount
                                })
                
                if stock_list:
                    result = pd.DataFrame(stock_list)
                    matched_count = len(result)
                    print(f"网易财经成功: 获取 {matched_count}/{len(stock_codes)} 只港股行情")
                    return result
                else:
                    print("网易财经: 未获取到港股数据")
                    return None
                    
            except Exception as e:
                print(f"网易财经数据解析失败: {e}")
                return None
        else:
            print("网易财经: 未获取到港股数据")
            return None
                
    except Exception as e:
        print(f"网易财经接口失败: {e}")
        return None


def get_hk_quotes(stock_codes, timeout=10):
    """
    获取港股实时行情（自动尝试多个数据源）
    
    Parameters:
    -----------
    stock_codes : list
        港股代码列表（如 ['02600', '00700']）
    timeout : int
        超时时间（秒），默认10秒
        
    Returns:
    --------
    pd.DataFrame
        港股实时行情数据
    """
    print(f"正在获取 {len(stock_codes)} 只港股的实时行情...")
    
    # 方法1: 腾讯证券
    result = get_hk_quotes_tencent(stock_codes, timeout)
    if result is not None and not result.empty:
        return result
    
    # 方法2: 新浪财经
    result = get_hk_quotes_sina(stock_codes, timeout)
    if result is not None and not result.empty:
        return result
    
    # 方法3: 网易财经
    result = get_hk_quotes_163(stock_codes, timeout)
    if result is not None and not result.empty:
        return result
    
    print("所有方法均未获取到港股行情数据")
    return None


if __name__ == "__main__":
    print("="*60)
    print("港股实时行情数据获取工具")
    print("="*60)
    
    # 输入港股代码
    input_str = input("请输入港股代码（多个代码用逗号分隔，例如：02600,00700）: ").strip()
    stock_codes = [code.strip() for code in input_str.split(',') if code.strip()]
    
    if not stock_codes:
        print("港股代码不能为空")
        exit(1)
    
    # 获取行情数据
    quotes = get_hk_quotes(stock_codes, timeout=10)
    
    if quotes is None:
        print("获取港股行情失败")
        exit(1)
    
    print(f"\n成功获取 {len(quotes)} 只港股的实时行情")
    print("\n港股行情明细:")
    print("="*60)
    
    # 只显示用户要求的字段
    columns_to_show = ['代码', '名称', '最新价', '涨跌', '涨跌幅', '时间']
    # 确保只包含存在的列
    columns_to_show = [col for col in columns_to_show if col in quotes.columns]
    
    print(quotes[columns_to_show].to_string(index=False))
    
    # 保存到CSV
    save_choice = input("\n是否保存到CSV文件？(y/n): ").strip().lower()
    if save_choice == 'y':
        filename = f"hk_quotes_{datetime.now().strftime('%Y%m%d')}.csv"
        quotes.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"数据已保存: {filename}")
    
    print("\n完成！")
