"""
基金实时估值计算脚本
通过基金重仓股的实时涨跌幅推算基金实时估值
"""

import akshare as ak
import pandas as pd
from datetime import datetime, time
import sys
import os

try:
    import efinance as ef
    HAS_EFINANCE = True
except ImportError:
    HAS_EFINANCE = False

try:
    from api.get_hk_stock_quotes import get_hk_quotes
    HAS_HK_QUOTES = True
except ImportError:
    HAS_HK_QUOTES = False

try:
    from api.get_all_stock_quotes import get_all_stock_quotes
    HAS_ALL_QUOTES = True
except ImportError:
    HAS_ALL_QUOTES = False


def is_trading_time():
    """
    判断当前是否为交易时间（周一至周五 9:30-11:30, 13:00-15:00）

    Returns:
        bool: True表示交易时间
    """
    now = datetime.now()

    # 检查是否为工作日（周一至周五）
    if now.weekday() >= 5:  # 5=周六, 6=周日
        return False

    # 检查是否在交易时间段
    current_time = now.time()
    morning_start = time(9, 30)
    morning_end = time(11, 30)
    afternoon_start = time(13, 0)
    afternoon_end = time(15, 0)

    if not ((morning_start <= current_time <= morning_end) or (afternoon_start <= current_time <= afternoon_end)):
        return False

    # 检查是否为法定节假日（使用 efinance）
    if HAS_EFINANCE:
        try:
            # 获取交易日历
            trade_date = now.strftime("%Y-%m-%d")
            trade_calendar = ef.stock.get_trade_calendar()
            if trade_calendar is not None and not trade_calendar.empty:
                # 检查今天是否为交易日
                is_trading_day = trade_date['交易日期'].apply(lambda x: x.strftime("%Y-%m-%d")).isin([trade_date]).any()
                return is_trading_day
        except Exception:
            pass

    # 如果无法获取交易日历，返回当前时间判断结果
    return True



class FundRealtimeCalculator:
    """基金实时估值计算器"""
    
    def __init__(self):
        self.fund_code = None
        self.fund_name = None
        self.portfolio = None  # 重仓股持仓
        self.stock_quotes = None  # 股票实时行情
        self.last_nav = None  # 最新净值
        self.calc_result = None  # 计算结果
        # 本地基金信息缓存
        self.local_fund_info = None
        self.local_fund_info_loaded = False
        # 缓存基金名称信息（带过期时间）
        self.fund_name_cache = {
            '110011': {'name': '易方达优质精选混合(QDII)', 'expire': 0},
            '000001': {'name': '华夏成长混合', 'expire': 0},
            '161725': {'name': '招商中证白酒指数(LOF)A', 'expire': 0}
        }
    
    @staticmethod
    def get_latest_quarter():
        """
        获取最新的季度信息
        
        根据当前日期返回最新的已公布季度：
        - Q1季报：4月公布
        - Q2季报：7月公布
        - Q3季报：10月公布
        - Q4季报：次年1月公布
        
        Returns:
        --------
        dict
            包含 year 和 quarter 的字典
        """
        now = datetime.now()
        year = now.year
        month = now.month
        
        # 根据月份判断最新的已公布季度
        if 1 <= month < 4:
            # 1-3月：最新是去年Q4
            quarter = 4
            year -= 1
        elif 4 <= month < 7:
            # 4-6月：最新是当年Q1
            quarter = 1
        elif 7 <= month < 10:
            # 7-9月：最新是当年Q2
            quarter = 2
        else:
            # 10-12月：最新是当年Q3
            quarter = 3
        
        return {'year': year, 'quarter': quarter}
    
    def get_latest_report_date(self):
        """
        获取最新的季报日期字符串
        
        Returns:
        --------
        str
            年份字符串，格式为 "YYYY"
        """
        latest = self.get_latest_quarter()
        print(f"当前时间: {datetime.now().strftime('%Y-%m-%d')}")
        print(f"最新可用季度: {latest['year']}年 第{latest['quarter']}季度")
        return str(latest['year'])
    
    def get_fund_portfolio(self, fund_code, year=None, auto_detect_latest=True):
        """
        获取基金重仓股持仓信息
        
        Parameters:
        -----------
        fund_code : str
            基金代码
        year : str
            年份，格式"YYYY"，默认自动检测最新季度
        auto_detect_latest : bool
            是否自动检测最新季度，默认True
            
        Returns:
        --------
        pd.DataFrame
            重仓股持仓数据
        """
        try:
            print(f"正在获取基金【{fund_code}】的最新持仓数据...")
            self.fund_code = fund_code
            
            # 优先使用 efinance 接口获取最新持仓数据
            if HAS_EFINANCE:
                print(f"方法1: 尝试 efinance 接口获取基金持仓...")
                try:
                    # 直接获取最新持仓，不需要指定年份和季度
                    raw_data = ef.fund.get_invest_position(fund_code)
                    
                    if raw_data is not None and not raw_data.empty:
                        print(f"efinance 接口成功获取持仓数据")
                        
                        # 获取基金名称
                        # 优化：缓存基金名称信息（带过期时间）
                        try:
                            # 优先使用本地缓存
                            cache_valid = False
                            if hasattr(self, 'fund_name_cache') and fund_code in self.fund_name_cache:
                                cache_entry = self.fund_name_cache[fund_code]
                                # 缓存有效期为7天
                                expire_time = cache_entry.get('expire', 0)
                                current_time = datetime.now().timestamp()
                                if expire_time == 0 or current_time < expire_time:
                                    name_data = cache_entry.get('name', f'基金{fund_code}')
                                    # 如果name是字典格式，提取名称字段
                                    if isinstance(name_data, dict):
                                        self.fund_name = name_data.get('名称', f'基金{fund_code}')
                                    else:
                                        self.fund_name = name_data
                                    cache_valid = True
                            
                            if not cache_valid:
                                # 优先使用本地缓存的基金信息
                                name_found = False
                                
                                # 方法0: 本地缓存（最快）
                                if not self.local_fund_info_loaded:
                                    try:
                                        import json
                                        cache_path = os.path.join(os.path.dirname(__file__), '..', 'cache', 'fund_info.json')
                                        with open(cache_path, 'r', encoding='utf-8') as f:
                                            self.local_fund_info = json.load(f)
                                            self.local_fund_info_loaded = True
                                        print(f"成功加载本地基金信息缓存，共 {len(self.local_fund_info)} 只基金")
                                    except Exception as e:
                                        print(f"加载本地基金信息缓存失败: {e}")
                                        self.local_fund_info = None
                                        self.local_fund_info_loaded = True
                                
                                # 检查基金代码是否在本地缓存中
                                code_in_cache = False
                                if self.local_fund_info and fund_code in self.local_fund_info:
                                    fund_info = self.local_fund_info[fund_code]
                                    # 如果是字典格式，提取名称字段
                                    if isinstance(fund_info, dict):
                                        self.fund_name = fund_info.get('名称', f'基金{fund_code}')
                                    else:
                                        self.fund_name = fund_info
                                    name_found = True
                                    code_in_cache = True
                                    print(f"从本地缓存获取基金名称: {self.fund_name}")
                                
                                # 如果基金代码不在缓存中，自动更新基金信息
                                if not code_in_cache:
                                    print(f"基金代码 {fund_code} 不在本地缓存中，开始更新基金信息...")
                                    try:
                                        # 导入更新脚本
                                        from scripts.update_fund_info import update_fund_info
                                        updated_fund_dict = update_fund_info()
                                        if updated_fund_dict and fund_code in updated_fund_dict:
                                            self.local_fund_info = updated_fund_dict
                                            fund_info = updated_fund_dict[fund_code]
                                            # 如果是字典格式，提取名称字段
                                            if isinstance(fund_info, dict):
                                                self.fund_name = fund_info.get('名称', f'基金{fund_code}')
                                            else:
                                                self.fund_name = fund_info
                                            name_found = True
                                            print(f"更新基金信息成功，获取基金名称: {self.fund_name}")
                                        else:
                                            print(f"更新基金信息后仍未找到 {fund_code}")
                                    except Exception as e:
                                        print(f"自动更新基金信息失败: {e}")
                                
                                # 方法1: efinance（次快）
                                if not name_found:
                                    try:
                                        fund_info = ef.fund.get_fund_info(fund_code)
                                        if fund_info:
                                            self.fund_name = fund_info.get('基金名称', f'基金{fund_code}')
                                            name_found = True
                                    except:
                                        pass
                                
                                # 方法2: akshare基金名称接口（缓存优化）
                                if not name_found:
                                    try:
                                        # 缓存akshare基金名称数据
                                        if not hasattr(self, 'akshare_name_cache'):
                                            # 只在第一次调用时获取全部基金名称
                                            self.akshare_name_cache = ak.fund_name_em()
                                        
                                        fund_name_match = self.akshare_name_cache[self.akshare_name_cache['基金代码'] == fund_code]
                                        if not fund_name_match.empty:
                                            self.fund_name = fund_name_match.iloc[0]['基金简称']
                                            name_found = True
                                    except:
                                        pass
                                
                                # 方法3: akshare基金基本信息接口
                                if not name_found:
                                    try:
                                        fund_info = ak.fund_info_em(symbol=fund_code)
                                        if not fund_info.empty:
                                            self.fund_name = fund_info.iloc[0]['基金简称']
                                            name_found = True
                                    except:
                                        pass
                                
                                # 方法4: akshare基金净值接口
                                if not name_found:
                                    try:
                                        fund_info = ak.fund_net_value_em(symbol=fund_code)
                                        if not fund_info.empty:
                                            self.fund_name = fund_info.iloc[0]['基金简称']
                                            name_found = True
                                    except:
                                        pass
                                
                                # 如果所有方法都失败
                                if not name_found:
                                    self.fund_name = f'基金{fund_code}'
                            
                            # 更新缓存
                            if hasattr(self, 'fund_name_cache'):
                                self.fund_name_cache[fund_code] = {
                                    'name': self.fund_name,
                                    'expire': datetime.now().timestamp() + 7 * 24 * 60 * 60  # 7天有效期
                                }
                            
                            print(f"基金名称: {self.fund_name}")
                        except Exception as e:
                            self.fund_name = f'基金{fund_code}'
                            print(f"基金名称: {self.fund_name}")
                        
                        # 重命名列以匹配后续处理逻辑
                        column_mapping = {}
                        if '股票代码' in raw_data.columns:
                            pass
                        elif '代码' in raw_data.columns:
                            column_mapping['代码'] = '股票代码'
                        
                        if '股票简称' in raw_data.columns:
                            column_mapping['股票简称'] = '股票名称'
                        elif '股票名称' in raw_data.columns:
                            pass
                        elif '名称' in raw_data.columns:
                            column_mapping['名称'] = '股票名称'
                        
                        if '占净值比例' in raw_data.columns:
                            pass
                        elif '持仓占比' in raw_data.columns:
                            column_mapping['持仓占比'] = '占净值比例'
                        elif '持仓比例' in raw_data.columns:
                            column_mapping['持仓比例'] = '占净值比例'
                        
                        if column_mapping:
                            raw_data = raw_data.rename(columns=column_mapping)
                        
                        self.portfolio = raw_data.copy()
                        print(f"成功获取 {len(self.portfolio)} 只重仓股数据")
                        return self.portfolio
                    else:
                        print("efinance 接口未获取到持仓数据，尝试其他方法...")
                except Exception as e:
                    print(f"efinance 接口失败: {e}，尝试其他方法...")
            
            # 备用方法：使用 akshare 接口获取持仓数据
            print(f"方法2: 尝试 akshare 接口获取基金持仓...")
            # 自动检测最新季度
            if year is None and auto_detect_latest:
                year = self.get_latest_report_date()
            elif year is None:
                year = str(datetime.now().year)
            
            raw_data = ak.fund_portfolio_hold_em(symbol=fund_code, date=year)
            
            if raw_data is not None and not raw_data.empty:
                # 筛选最新季度的数据
                quarter_info = self.get_latest_quarter()
                target_quarter = f"{quarter_info['year']}年第{quarter_info['quarter']}季度"
                
                # 检查数据中是否有'季度'字段
                if '季度' in raw_data.columns:
                    # 筛选目标季度
                    self.portfolio = raw_data[raw_data['季度'] == target_quarter].copy()
                    
                    if self.portfolio.empty:
                        print(f"警告：未找到{target_quarter}的数据")
                        print(f"可用季度: {raw_data['季度'].unique()}")
                        
                        # 如果找不到目标季度，使用最新的可用季度
                        latest_available = raw_data['季度'].iloc[-1]
                        print(f"使用最新可用季度: {latest_available}")
                        self.portfolio = raw_data[raw_data['季度'] == latest_available].copy()
                else:
                    # 如果没有季度字段，直接使用全部数据
                    print("警告：数据中未找到'季度'字段，使用全部数据")
                    self.portfolio = raw_data.copy()
                
                if not self.portfolio.empty:
                    print(f"成功获取 {len(self.portfolio)} 只重仓股数据")
                    
                    # 显示使用的季度信息
                    if '季度' in self.portfolio.columns:
                        used_quarter = self.portfolio['季度'].iloc[0]
                        print(f"使用季度数据: {used_quarter}")
                    
                    # 获取基金名称
                    fund_info = ak.fund_name_em()
                    fund_name_match = fund_info[fund_info['基金代码'] == fund_code]
                    if not fund_name_match.empty:
                        self.fund_name = fund_name_match.iloc[0]['基金简称']
                        print(f"基金名称: {self.fund_name}")
                    
                    return self.portfolio
                else:
                    print("未获取到重仓股数据")
                    return None
            else:
                print("未获取到重仓股数据")
                return None
                
        except Exception as e:
            print(f"获取基金持仓失败: {e}")
            return None
    
    def get_stock_realtime_quotes(self, stock_codes=None, timeout=10):
        """
        获取股票实时行情（根据交易时间自动选择分时或收盘价）

        Parameters:
        -----------
        stock_codes : list
            股票代码列表，如果为None则使用持仓中的股票
        timeout : int
            每个接口的超时时间（秒），默认10秒

        Returns:
        --------
        pd.DataFrame
            股票实时行情数据
        """
        import socket

        # 设置全局超时
        socket.setdefaulttimeout(timeout)

        try:
            if stock_codes is None:
                if self.portfolio is None or self.portfolio.empty:
                    print("请先获取基金持仓数据")
                    return None
                stock_codes = self.portfolio['股票代码'].tolist()

            # 判断是否为交易时间
            trading = is_trading_time()
            if trading:
                print(f"\n当前为交易时间，将获取分时实时行情")
            else:
                print(f"\n当前为非交易时间，将获取最新收盘价信息")

            print(f"正在获取 {len(stock_codes)} 只股票的实时行情...")

            # 分离港股和A股代码
            hk_codes = [code for code in stock_codes if len(code) == 5 and code.isdigit()]
            a_codes = [code for code in stock_codes if code not in hk_codes]
            
            # 优先使用统一接口获取所有股票行情
            if HAS_ALL_QUOTES:
                print(f"尝试使用统一接口获取所有股票行情...")
                all_quotes = get_all_stock_quotes(stock_codes, timeout=timeout)
                if all_quotes is not None and not all_quotes.empty:
                    self.stock_quotes = all_quotes
                    print(f"统一接口成功获取 {len(self.stock_quotes)}/{len(stock_codes)} 只股票行情")
                    return self.stock_quotes
                else:
                    print("统一接口未获取到股票行情数据，尝试其他方法...")
            
            # 备用方案：分离处理港股和A股
            # 处理港股
            hk_quotes = None
            if hk_codes and HAS_HK_QUOTES:
                print(f"检测到 {len(hk_codes)} 只港股，使用港股专用接口获取行情...")
                hk_quotes = get_hk_quotes(hk_codes, timeout=timeout)
                if hk_quotes is not None and not hk_quotes.empty:
                    print(f"港股接口成功获取 {len(hk_quotes)} 只港股行情")

            # 处理A股
            a_quotes = None
            if a_codes:
                print(f"\n处理 {len(a_codes)} 只A股...")

                # 优先尝试A股腾讯接口
                try:
                    from get_a_stock_quotes import get_a_quotes_tencent
                    print(f"方法1: 尝试 A股腾讯接口（超时{timeout}秒）...")
                    a_quotes = get_a_quotes_tencent(a_codes, timeout=timeout)
                    if a_quotes is not None and not a_quotes.empty:
                        print(f"A股腾讯接口成功获取 {len(a_quotes)}/{len(a_codes)} 只A股的实时行情")
                        if trading:
                            print("注: 当前为交易时间，价格和涨跌幅为实时数据")
                        else:
                            print("注: 当前为非交易时间，价格为最新收盘价，涨跌幅为日涨跌幅")
                except ImportError:
                    print("A股腾讯接口未安装，跳过")

                # 方法2: efinance 接口
                if (a_quotes is None or a_quotes.empty) and HAS_EFINANCE:
                    print(f"方法2: 尝试 efinance 接口（超时{timeout}秒）...")
                    try:
                        stock_list = []
                        missing_codes = []

                        for code in a_codes:
                            try:
                                # 尝试获取股票实时行情
                                data = ef.stock.get_quote_snapshot(code)

                                if data is not None:
                                    change_value = data.get('涨跌幅', 0)
                                    if change_value >= 0:
                                        change_str = f"+{change_value:.2f}%"
                                    else:
                                        change_str = f"{change_value:.2f}%"
                                    stock_list.append({
                                                '代码': data.get('代码', code),
                                                '名称': data.get('名称', code),
                                                '最新价': data.get('最新价', 0),
                                                '涨跌幅': change_str,
                                                '成交量': data.get('成交量', 0),
                                                '成交额': data.get('成交额', 0)
                                            })
                                else:
                                    missing_codes.append(code)
                            except Exception as e:
                                missing_codes.append(code)
                                continue

                        if stock_list:
                            a_quotes = pd.DataFrame(stock_list)
                            matched_count = len(a_quotes)
                            total_count = len(a_codes)
                            print(f"方法2成功: 获取 {matched_count}/{total_count} 只A股的实时行情")

                            if matched_count < total_count and missing_codes:
                                print(f"未找到的A股代码: {missing_codes}")

                            if trading:
                                print("注: 当前为交易时间，价格和涨跌幅为实时数据")
                            else:
                                print("注: 当前为非交易时间，价格为最新收盘价，涨跌幅为日涨跌幅")
                        else:
                            print("方法2: 未匹配到任何A股")
                    except Exception as e1:
                        print(f"方法2失败: {e1}")
                else:
                    print("方法2: efinance 未安装，跳过")

                # 方法3: 东方财富实时行情接口
                if (a_quotes is None or a_quotes.empty) and HAS_AKSHARE:
                    print(f"方法3: 尝试东方财富接口（超时{timeout}秒）...")
                    try:
                        all_stocks = ak.stock_zh_a_spot_em()
                        a_quotes = all_stocks[all_stocks['代码'].isin(a_codes)]

                        if a_quotes is not None and not a_quotes.empty:
                            matched_count = len(a_quotes)
                            total_count = len(a_codes)
                            print(f"方法3成功: 获取 {matched_count}/{total_count} 只A股的实时行情")

                            if matched_count < total_count:
                                matched_codes = set(a_quotes['代码'].tolist())
                                missing_codes = set(a_codes) - matched_codes
                                if missing_codes:
                                    print(f"未找到的股票代码: {missing_codes}")

                            if trading:
                                print("注: 当前为交易时间，价格和涨跌幅为实时数据")
                            else:
                                print("注: 当前为非交易时间，价格为最新收盘价，涨跌幅为日涨跌幅")
                        else:
                            print("方法3: 未匹配到任何A股")
                    except Exception as e2:
                        print(f"方法3失败: {e2}")

            # 合并港股和A股行情
            if hk_quotes is not None and not hk_quotes.empty and a_quotes is not None and not a_quotes.empty:
                self.stock_quotes = pd.concat([hk_quotes, a_quotes], ignore_index=True)
                print(f"\n总计获取 {len(self.stock_quotes)} 只股票的实时行情")
                return self.stock_quotes
            elif hk_quotes is not None and not hk_quotes.empty:
                self.stock_quotes = hk_quotes
                print(f"\n总计获取 {len(self.stock_quotes)} 只股票的实时行情")
                return self.stock_quotes
            elif a_quotes is not None and not a_quotes.empty:
                self.stock_quotes = a_quotes
                print(f"\n总计获取 {len(self.stock_quotes)} 只股票的实时行情")
                return self.stock_quotes
            else:
                # 备用方案：使用统一接口获取所有股票行情
                if HAS_ALL_QUOTES:
                    print(f"\n尝试使用统一接口获取所有股票行情...")
                    all_quotes = get_all_stock_quotes(stock_codes, timeout=timeout)
                    if all_quotes is not None and not all_quotes.empty:
                        self.stock_quotes = all_quotes
                        print(f"统一接口成功获取 {len(self.stock_quotes)}/{len(stock_codes)} 只股票行情")
                        return self.stock_quotes
                    else:
                        print("统一接口也未获取到股票行情数据")
                
                print("所有方法均未获取到股票行情数据")
                return None

        except Exception as e:
            print(f"获取股票行情失败: {e}")
            import traceback
            traceback.print_exc()
            return None

    def is_index_fund(self):
        """
        判断基金是否为指数型基金

        Returns:
        --------
        bool
            True表示是指数型基金
        """
        if not self.local_fund_info_loaded:
            # 尝试加载本地基金信息
            try:
                import json
                cache_path = os.path.join(os.path.dirname(__file__), '..', 'cache', 'fund_info.json')
                with open(cache_path, 'r', encoding='utf-8') as f:
                    self.local_fund_info = json.load(f)
                    self.local_fund_info_loaded = True
            except Exception as e:
                print(f"加载本地基金信息失败: {e}")
                return False

        if self.fund_code in self.local_fund_info:
            fund_info = self.local_fund_info[self.fund_code]
            if isinstance(fund_info, dict):
                fund_type = fund_info.get('类型', '')
            else:
                fund_type = ''
            return fund_type.startswith('指数型-股票') or fund_type.startswith('指数型-海外股票')
        return False

    def get_index_etf_quotes(self, index_code):
        """
        获取指数或ETF的实时行情

        Parameters:
        -----------
        index_code : str
            指数或ETF代码

        Returns:
        --------
        dict
            指数或ETF的实时行情数据
        """
        try:
            print(f"正在获取指数/ETF【{index_code}】的实时行情...")
            
            # 优先使用 efinance 接口
            if HAS_EFINANCE:
                try:
                    data = ef.stock.get_quote_snapshot(index_code)
                    if data is not None:
                        return {
                            '代码': index_code,
                            '名称': data.get('名称', index_code),
                            '最新价': data.get('最新价', 0),
                            '涨跌幅': f"{data.get('涨跌幅', 0):+.2f}%"
                        }
                except Exception as e:
                    print(f"efinance 接口获取指数行情失败: {e}")
            
            # 备用方法：使用 akshare 接口
            try:
                if index_code.startswith('000') or index_code.startswith('399'):
                    # 大盘指数
                    index_data = ak.stock_zh_index_spot_em()
                    index_data = index_data[index_data['代码'] == index_code]
                    if not index_data.empty:
                        row = index_data.iloc[0]
                        return {
                            '代码': index_code,
                            '名称': row['名称'],
                            '最新价': row['最新价'],
                            '涨跌幅': f"{row['涨跌幅']:+.2f}%"
                        }
                else:
                    # ETF
                    etf_data = ak.fund_etf_spot_em()
                    etf_data = etf_data[etf_data['代码'] == index_code]
                    if not etf_data.empty:
                        row = etf_data.iloc[0]
                        return {
                            '代码': index_code,
                            '名称': row['名称'],
                            '最新价': row['最新价'],
                            '涨跌幅': f"{row['涨跌幅']:+.2f}%"
                        }
            except Exception as e:
                print(f"akshare 接口获取指数行情失败: {e}")
            
            return None
        except Exception as e:
            print(f"获取指数/ETF行情失败: {e}")
            return None

    def calculate_realtime_value(self):
        """
        计算基金实时估值（使用重仓股涨跌幅加权）

        Returns:
        --------
        pd.DataFrame
            计算结果
        """
        if self.portfolio is None or self.portfolio.empty:
            # 检查是否为指数型基金
            if self.is_index_fund():
                print("\n" + "="*60)
                print("基金为指数型，尝试使用指数/ETF估算估值...")
                print("="*60)
                
                # 这里可以根据基金代码映射对应的指数或ETF代码
                # 简单起见，我们假设指数型基金名称中包含对应的指数名称
                index_code = None
                if self.fund_name:
                    # 示例：根据基金名称提取指数代码
                    fund_name_lower = self.fund_name.lower()
                    if '沪深300' in self.fund_name or 'hs300' in fund_name_lower:
                        index_code = '000300'
                    elif '中证500' in self.fund_name or 'zz500' in fund_name_lower:
                        index_code = '000905'
                    elif '上证指数' in self.fund_name or 'sh000001' in fund_name_lower or '上证指数etf' in fund_name_lower:
                        index_code = '000001'
                    elif '深证成指' in self.fund_name or 'sz399001' in fund_name_lower:
                        index_code = '399001'
                    elif '创业板指' in self.fund_name or 'cyb' in fund_name_lower or '创业板' in self.fund_name:
                        index_code = '399006'
                    elif '医药100' in self.fund_name or '中证医药100' in self.fund_name:
                        index_code = '000933'  # 中证医药100指数
                    elif '上证50' in self.fund_name or 'sh000016' in fund_name_lower:
                        index_code = '000016'  # 上证50指数
                    elif '中证100' in self.fund_name or 'zz100' in fund_name_lower:
                        index_code = '000903'  # 中证100指数
                    elif '中证800' in self.fund_name or 'zz800' in fund_name_lower:
                        index_code = '000906'  # 中证800指数
                    elif '中小板指' in self.fund_name or 'sz399005' in fund_name_lower:
                        index_code = '399005'  # 中小板指数
                    elif '科创50' in self.fund_name or 'kcb50' in fund_name_lower:
                        index_code = '000688'  # 科创50指数
                
                if index_code:
                    index_data = self.get_index_etf_quotes(index_code)
                    if index_data:
                        print(f"成功获取指数【{index_code}】的实时行情")
                        print(f"指数名称: {index_data['名称']}")
                        print(f"指数涨跌幅: {index_data['涨跌幅']}")
                        
                        # 使用指数涨跌幅作为基金估值
                        weighted_change = float(index_data['涨跌幅'].rstrip('%')) / 100
                        
                        # 构建结果
                        result = {
                            'fund_code': self.fund_code,
                            'fund_name': self.fund_name,
                            'weighted_change': weighted_change,
                            'stock_details': pd.DataFrame(),
                            'calc_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                        
                        self.calc_result = result
                        return result
                
                print("未找到对应指数/ETF代码，无法估算估值")
                return None
            else:
                print("请先获取基金持仓数据")
                return None

        if self.stock_quotes is None or self.stock_quotes.empty:
            print("请先获取股票实时行情")
            return None

        print("\n" + "="*60)
        print("开始计算基金实时估值（重仓股涨跌幅加权）...")
        print("="*60)

        # 合并持仓和行情数据
        merged = pd.merge(
            self.portfolio,
            self.stock_quotes,
            left_on='股票代码',
            right_on='代码',
            how='left'
        )

        if merged.empty:
            print("持仓和行情数据合并失败")
            return None

        # 处理涨跌幅数据（去掉%符号并转换为数值）
        merged['涨跌幅_num'] = merged['涨跌幅'].apply(
            lambda x: float(str(x).rstrip('%').rstrip('％')) / 100 if pd.notna(x) else 0
        )

        # 加权平均法：根据持仓比例加权
        merged['持仓比例_num'] = merged['占净值比例'].apply(
            lambda x: float(str(x).rstrip('%').rstrip('％')) / 100 if pd.notna(x) else 0
        )

        # 加权平均涨跌幅
        weighted_change = (merged['涨跌幅_num'] * merged['持仓比例_num']).sum()

        print(f"\n重仓股加权平均涨跌幅: {weighted_change*100:.2f}%")
        print(f"重仓股合计持仓比例: {merged['持仓比例_num'].sum()*100:.2f}%")

        # 整理输出结果，确保涨跌幅带有+/-号
        result = merged[['股票代码', '股票名称', '最新价']].copy()
        result['涨跌幅_num'] = merged['涨跌幅_num']
        result['持仓比例_num'] = merged['持仓比例_num']
        
        # 为涨跌幅添加+/-号
        result['涨跌幅'] = result['涨跌幅_num'].apply(
            lambda x: f"+{x*100:.2f}%" if x >= 0 else f"{x*100:.2f}%"
        )
        
        # 确保持仓比例以百分比形式显示
        result['占净值比例'] = result['持仓比例_num'].apply(
            lambda x: f"{x*100:.2f}%"
        )

        # 添加+/-号，使用format方法
        change_value = weighted_change * 100
        if change_value >= 0:
            weighted_change_str = f"+{change_value:.2f}%"
        else:
            weighted_change_str = f"{change_value:.2f}%"

        self.calc_result = {
            'fund_code': self.fund_code,
            'fund_name': self.fund_name,
            'weighted_change': weighted_change,
            'stock_details': result,
            'calc_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        return self.calc_result

    def print_result(self, result=None):
        """
        打印计算结果

        Parameters:
        -----------
        result : dict
            计算结果，如果为None则使用self.calc_result
        """
        if result is None:
            result = self.calc_result

        if result is None:
            print("没有可显示的结果")
            return

        print("\n" + "="*60)
        print(f"基金实时估值计算结果（基于重仓股涨跌幅加权）")
        print("="*60)
        print(f"基金代码: {result['fund_code']}")
        print(f"基金名称: {result['fund_name']}")
        print(f"计算时间: {result['calc_time']}")
        print(f"\n估值信息:")
        print(f"  重仓股加权涨跌幅: {result['weighted_change']*100:+.2f}%")
        
        print(f"\n重仓股表现:")
        print("-"*60)
        print(f"{'股票代码':<10}{'股票名称':<15}{'持仓比例':<12}{'最新价':<12}{'涨跌幅':<10}")
        print("-"*60)

        for _, row in result['stock_details'].iterrows():
            print(f"{row['股票代码']:<10}{row['股票名称']:<15}{row['占净值比例']:<12}"
                  f"{row['最新价']:<12}{row['涨跌幅']:<10}")

        print("="*60)

    def save_result(self, filename=None):
        """
        保存计算结果到CSV

        Parameters:
        -----------
        filename : str
            文件名，默认为时间戳
        """
        if self.calc_result is None:
            print("没有可保存的结果")
            return

        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"fund_estimate_{self.fund_code}_{timestamp}.csv"

        # 保存重仓股详情
        self.calc_result['stock_details'].to_csv(filename, index=False, encoding='utf-8-sig')

        # 保存摘要信息
        summary_filename = filename.replace('.csv', '_summary.txt')
        with open(summary_filename, 'w', encoding='utf-8') as f:
            f.write(f"基金实时估值计算结果（基于重仓股涨跌幅加权）\n")
            f.write(f"{'='*60}\n")
            f.write(f"基金代码: {self.calc_result['fund_code']}\n")
            f.write(f"基金名称: {self.calc_result['fund_name']}\n")
            f.write(f"计算时间: {self.calc_result['calc_time']}\n")
            f.write(f"重仓股加权涨跌幅: {self.calc_result['weighted_change']*100:+.2f}%\n")

        print(f"\n结果已保存:")
        print(f"  详情: {filename}")
        print(f"  摘要: {summary_filename}")


def main():
    """主函数"""
    print("="*60)
    print("基金实时估值计算器（基于重仓股）")
    print("="*60)
    print("提示：系统将自动获取最新季度的重仓股数据")
    
    # 输入基金代码
    fund_code = input("\n请输入基金代码（例如：110011）: ").strip()
    if not fund_code:
        print("基金代码不能为空")
        return
    
    # 创建计算器实例
    calculator = FundRealtimeCalculator()
    
    # 1. 获取基金持仓（自动检测最新季度）
    print("\n正在自动检测最新可用的季度...")
    portfolio = calculator.get_fund_portfolio(fund_code, year=None, auto_detect_latest=True)
    if portfolio is None:
        return
    
    print("\n重仓股持仓:")
    # 动态获取可用列
    available_columns = ['股票代码', '股票名称', '占净值比例']
    for col in ['持股数', '持仓市值']:
        if col in portfolio.columns:
            available_columns.append(col)
    print(portfolio[available_columns])

    # 2. 获取股票实时行情
    quotes = calculator.get_stock_realtime_quotes()
    if quotes is None:
        return

    # 3. 计算实时估值（重仓股涨跌幅加权）
    result = calculator.calculate_realtime_value()

    # 4. 显示结果
    calculator.print_result()

    # 5. 保存结果
    choice = input("\n是否保存计算结果？(y/n): ").strip().lower()
    if choice == 'y':
        calculator.save_result()

    print("\n计算完成！")


def test_index_fund_valuation():
    """
    测试指数型基金的估值逻辑
    """
    print("\n" + "="*60)
    print("测试指数型基金估值逻辑...")
    print("="*60)
    
    # 测试基金代码列表（指数型基金）
    test_funds = [
        '000008',  # 嘉实中证500ETF联接A（指数型-股票）
        '000051',  # 华夏沪深300ETF联接A（指数型-股票）
        '000059',  # 国联安中证医药100A（指数型-股票）
        '000154',  # 富国沪深300指数增强A(后端)（指数型-股票）
        '000164',  # 富国上证指数ETF联接A(后端)（指数型-股票）
    ]
    
    for fund_code in test_funds:
        print(f"\n测试基金【{fund_code}】...")
        
        # 创建计算器实例
        calculator = FundRealtimeCalculator()
        
        # 获取基金持仓（可能会失败，因为是新基金）
        portfolio = calculator.get_fund_portfolio(fund_code, year=None, auto_detect_latest=True)
        
        # 计算估值
        result = calculator.calculate_realtime_value()
        
        if result:
            print(f"成功估算基金【{fund_code}】的估值")
            print(f"基金名称: {result['fund_name']}")
            print(f"估值涨跌幅: {result['weighted_change']*100:+.2f}%")
        else:
            print(f"无法估算基金【{fund_code}】的估值")
        
        # 强制测试指数估值逻辑（清空portfolio）
        print(f"\n强制测试指数估值逻辑【{fund_code}】...")
        calculator.portfolio = None
        result = calculator.calculate_realtime_value()
        
        if result:
            print(f"成功使用指数估算基金【{fund_code}】的估值")
            print(f"基金名称: {result['fund_name']}")
            print(f"估值涨跌幅: {result['weighted_change']*100:+.2f}%")
        else:
            print(f"无法使用指数估算基金【{fund_code}】的估值")
    
    print("\n测试完成！")

if __name__ == "__main__":
    # 测试指数型基金估值逻辑
    # test_index_fund_valuation()
    # 运行主函数
    main()
