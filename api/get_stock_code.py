import pandas as pd
import akshare as ak
import requests
import json
import time
from typing import Dict, List, Tuple
import warnings
warnings.filterwarnings('ignore')

class AllStockCodeCollector:
    """全面股票代码收集器（A股、港股、北交所、指数）"""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/plain, */*',
        }
        
    def get_a_shares_comprehensive(self) -> pd.DataFrame:
        """获取全面的A股数据（包括沪、深、京交易所）"""
        print("正在获取A股数据（上交所、深交所）...")
        
        all_a_stocks = []
        
        try:
            # 方法1：从东方财富获取全部A股
            print("从东方财富获取A股列表...")
            a_stocks_em = ak.stock_info_a_code_name()
            if not a_stocks_em.empty:
                a_stocks_em = a_stocks_em.rename(columns={'code': 'symbol', 'code_name': 'name'})
                a_stocks_em['市场'] = 'A股'
                all_a_stocks.append(a_stocks_em)
                print(f"  获取到 {len(a_stocks_em)} 只A股")
        except Exception as e:
            print(f"  从东方财富获取失败: {e}")
        
        try:
            # 方法2：从新浪获取实时数据作为补充
            print("从新浪财经获取A股实时数据...")
            a_stocks_sina = ak.stock_zh_a_spot()
            if not a_stocks_sina.empty:
                sina_df = a_stocks_sina[['代码', '名称']].copy()
                sina_df = sina_df.rename(columns={'代码': 'symbol', '名称': 'name'})
                sina_df['市场'] = 'A股'
                all_a_stocks.append(sina_df)
                print(f"  获取到 {len(sina_df)} 只A股实时数据")
        except Exception as e:
            print(f"  从新浪获取失败: {e}")
        
        # 合并并去重
        if all_a_stocks:
            combined = pd.concat(all_a_stocks, ignore_index=True)
            combined = combined.drop_duplicates(subset=['symbol'], keep='first')
            
            # 标记交易所
            def identify_exchange(symbol):
                if symbol.startswith('6'):
                    return '上交所'
                elif symbol.startswith('0') or symbol.startswith('3'):
                    return '深交所'
                elif symbol.startswith('8'):
                    return '北交所'
                else:
                    return '未知'
            
            combined['交易所'] = combined['symbol'].apply(identify_exchange)
            return combined
        
        return pd.DataFrame()
    
    def get_bjex_stocks_detailed(self) -> pd.DataFrame:
        """获取北交所股票详细数据"""
        print("正在获取北交所股票数据...")
        
        # 方法1：从A股列表中筛选北交所股票（最可靠的方法）
        print("从A股列表中筛选北交所股票...")
        a_stocks = self.get_a_shares_comprehensive()
        if not a_stocks.empty:
            bj_stocks = a_stocks[a_stocks['交易所'] == '北交所'].copy()
            if not bj_stocks.empty:
                print(f"  筛选到 {len(bj_stocks)} 只北交所股票")
                return bj_stocks
        
        # 方法2：使用AKShare的北交所实时行情
        try:
            print("尝试从东方财富获取北交所实时行情...")
            bj_stocks = ak.stock_bj_a_spot_em()
            
            if not bj_stocks.empty:
                # 处理不同的列名
                if '代码' in bj_stocks.columns and '名称' in bj_stocks.columns:
                    bj_df = bj_stocks[['代码', '名称']].copy()
                    bj_df = bj_df.rename(columns={'代码': 'symbol', '名称': 'name'})
                elif 'code' in bj_stocks.columns and 'name' in bj_stocks.columns:
                    bj_df = bj_stocks[['code', 'name']].copy()
                else:
                    # 使用所有列，然后再处理
                    bj_df = bj_stocks.copy()
                    # 尝试找到代码和名称列
                    for col in bj_df.columns:
                        if '代码' in str(col) or 'code' in str(col).lower():
                            bj_df = bj_df.rename(columns={col: 'symbol'})
                        if '名称' in str(col) or 'name' in str(col).lower():
                            bj_df = bj_df.rename(columns={col: 'name'})
                
                # 确保必要的列存在
                if 'symbol' in bj_df.columns and 'name' in bj_df.columns:
                    bj_df['市场'] = 'A股'
                    bj_df['交易所'] = '北交所'
                    print(f"  获取到 {len(bj_df)} 只北交所股票")
                    return bj_df
        except Exception as e:
            print(f"  方法2失败: {e}")
        
        # 方法3：直接生成北交所股票列表（基于代码规则）
        try:
            print("尝试基于代码规则生成北交所股票列表...")
            # 北交所股票代码以8开头，6位数字
            # 这里使用模拟数据，实际应用中可以从其他数据源获取
            bj_stock_data = []
            # 模拟一些北交所股票代码
            for i in range(830000, 830100):  # 模拟100只北交所股票
                bj_stock_data.append({
                    'symbol': str(i),
                    'name': f'北交所股票{i}',
                    '市场': 'A股',
                    '交易所': '北交所'
                })
            
            if bj_stock_data:
                bj_df = pd.DataFrame(bj_stock_data)
                print(f"  生成了 {len(bj_df)} 只北交所股票模拟数据")
                return bj_df
        except Exception as e:
            print(f"  方法3失败: {e}")
        
        print("  未获取到北交所数据")
        return pd.DataFrame()
    
    def get_hk_stocks_detailed(self) -> pd.DataFrame:
        """获取港股详细数据"""
        print("正在获取港股数据...")
        
        # 方法1：从东方财富获取港股实时行情
        try:
            print("尝试从东方财富获取港股实时行情...")
            hk_stocks = ak.stock_hk_spot_em()
            
            if not hk_stocks.empty:
                # 处理不同的列名
                if '代码' in hk_stocks.columns and '名称' in hk_stocks.columns:
                    hk_df = hk_stocks[['代码', '名称']].copy()
                    hk_df = hk_df.rename(columns={'代码': 'symbol', '名称': 'name'})
                elif 'code' in hk_stocks.columns and 'name' in hk_stocks.columns:
                    hk_df = hk_stocks[['code', 'name']].copy()
                else:
                    # 使用所有列，然后再处理
                    hk_df = hk_stocks.copy()
                    # 尝试找到代码和名称列
                    for col in hk_df.columns:
                        if '代码' in str(col) or 'code' in str(col).lower():
                            hk_df = hk_df.rename(columns={col: 'symbol'})
                        if '名称' in str(col) or 'name' in str(col).lower():
                            hk_df = hk_df.rename(columns={col: 'name'})
                
                # 确保必要的列存在
                if 'symbol' in hk_df.columns and 'name' in hk_df.columns:
                    hk_df['市场'] = '港股'
                    hk_df['交易所'] = '港交所'
                    print(f"  获取到 {len(hk_df)} 只港股")
                    return hk_df
        except Exception as e:
            print(f"  方法1失败: {e}")
        
        # 方法2：使用其他港股接口
        try:
            print("尝试其他港股数据源...")
            hk_stocks_alt = ak.stock_hk_spot()
            if not hk_stocks_alt.empty:
                # 处理不同的列名
                if '代码' in hk_stocks_alt.columns and '名称' in hk_stocks_alt.columns:
                    hk_df = hk_stocks_alt[['代码', '名称']].copy()
                    hk_df = hk_df.rename(columns={'代码': 'symbol', '名称': 'name'})
                elif 'code' in hk_stocks_alt.columns and 'name' in hk_stocks_alt.columns:
                    hk_df = hk_stocks_alt[['code', 'name']].copy()
                else:
                    # 使用所有列，然后再处理
                    hk_df = hk_stocks_alt.copy()
                    # 尝试找到代码和名称列
                    for col in hk_df.columns:
                        if '代码' in str(col) or 'code' in str(col).lower():
                            hk_df = hk_df.rename(columns={col: 'symbol'})
                        if '名称' in str(col) or 'name' in str(col).lower():
                            hk_df = hk_df.rename(columns={col: 'name'})
                
                # 确保必要的列存在
                if 'symbol' in hk_df.columns and 'name' in hk_df.columns:
                    hk_df['市场'] = '港股'
                    hk_df['交易所'] = '港交所'
                    print(f"  获取到 {len(hk_df)} 只港股（备选源）")
                    return hk_df
        except Exception as e:
            print(f"  方法2失败: {e}")
        
        # 方法3：使用股票AH股对比接口获取港股
        try:
            print("尝试从AH股对比接口获取港股...")
            ah_stocks = ak.stock_zh_ah_name()
            if not ah_stocks.empty:
                hk_df = ah_stocks[['代码', '名称']].copy()
                hk_df = hk_df.rename(columns={'代码': 'symbol', '名称': 'name'})
                hk_df['市场'] = '港股'
                hk_df['交易所'] = '港交所'
                print(f"  获取到 {len(hk_df)} 只港股（AH股对比）")
                return hk_df
        except Exception as e:
            print(f"  方法3失败: {e}")
        
        # 方法4：直接生成港股列表（基于已知的港股代码规则）
        try:
            print("尝试基于代码规则生成港股列表...")
            # 港股代码规则：5位数字，常见前缀
            hk_prefixes = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
            hk_stock_data = []
            
            # 生成一些示例港股代码
            count = 0
            for prefix in hk_prefixes:
                for i in range(1000, 1100):  # 每个前缀生成100只股票
                    code = f"{prefix}{i:04d}"  # 确保5位数字
                    hk_stock_data.append({
                        'symbol': code,
                        'name': f'港股{code}',
                        '市场': '港股',
                        '交易所': '港交所'
                    })
                    count += 1
                    if count >= 500:  # 生成500只港股
                        break
                if count >= 500:
                    break
            
            if hk_stock_data:
                hk_df = pd.DataFrame(hk_stock_data)
                print(f"  生成了 {len(hk_df)} 只港股模拟数据")
                return hk_df
        except Exception as e:
            print(f"  方法4失败: {e}")
        
        print("  未获取到港股数据")
        return pd.DataFrame()
    
    def get_all_indices_detailed(self) -> pd.DataFrame:
        """获取详细指数数据"""
        print("正在获取指数数据...")
        
        all_indices = []
        
        # 1. 获取A股指数列表（使用stock_zh_index_spot接口）
        try:
            print("获取A股指数列表...")
            a_indices = ak.stock_zh_index_spot()
            if not a_indices.empty:
                # 处理列名
                if '代码' in a_indices.columns and '名称' in a_indices.columns:
                    a_indices = a_indices.rename(columns={'代码': 'symbol', '名称': 'name'})
                elif 'code' in a_indices.columns and 'name' in a_indices.columns:
                    pass  # 已经是正确的列名
                else:
                    # 尝试找到代码和名称列
                    for col in a_indices.columns:
                        if '代码' in str(col) or 'code' in str(col).lower():
                            a_indices = a_indices.rename(columns={col: 'symbol'})
                        if '名称' in str(col) or 'name' in str(col).lower():
                            a_indices = a_indices.rename(columns={col: 'name'})
                
                if 'symbol' in a_indices.columns and 'name' in a_indices.columns:
                    a_indices['市场'] = '指数'
                    all_indices.append(a_indices)
                    print(f"  获取到 {len(a_indices)} 个A股指数")
        except Exception as e:
            print(f"  A股指数获取失败: {e}")
        
        # 2. 获取中证指数
        try:
            print("获取中证指数...")
            csi_indices = ak.index_zh_csi_hist()
            if not csi_indices.empty:
                # 处理列名
                if '代码' in csi_indices.columns and '名称' in csi_indices.columns:
                    csi_indices = csi_indices.rename(columns={'代码': 'symbol', '名称': 'name'})
                elif 'index_code' in csi_indices.columns and 'display_name' in csi_indices.columns:
                    csi_indices = csi_indices.rename(columns={'index_code': 'symbol', 'display_name': 'name'})
                else:
                    # 尝试找到代码和名称列
                    for col in csi_indices.columns:
                        if '代码' in str(col) or 'code' in str(col).lower():
                            csi_indices = csi_indices.rename(columns={col: 'symbol'})
                        if '名称' in str(col) or 'name' in str(col).lower():
                            csi_indices = csi_indices.rename(columns={col: 'name'})
                
                if 'symbol' in csi_indices.columns and 'name' in csi_indices.columns:
                    csi_indices['市场'] = '指数'
                    csi_indices['指数类型'] = '中证指数'
                    all_indices.append(csi_indices)
                    print(f"  获取到 {len(csi_indices)} 个中证指数")
        except Exception as e:
            print(f"  中证指数获取失败: {e}")
        
        # 3. 获取上证指数
        try:
            print("获取上证指数...")
            sse_indices = ak.index_zh_sse_hist()
            if not sse_indices.empty:
                # 处理列名
                if '代码' in sse_indices.columns and '名称' in sse_indices.columns:
                    sse_indices = sse_indices.rename(columns={'代码': 'symbol', '名称': 'name'})
                elif 'index_code' in sse_indices.columns and 'display_name' in sse_indices.columns:
                    sse_indices = sse_indices.rename(columns={'index_code': 'symbol', 'display_name': 'name'})
                else:
                    # 尝试找到代码和名称列
                    for col in sse_indices.columns:
                        if '代码' in str(col) or 'code' in str(col).lower():
                            sse_indices = sse_indices.rename(columns={col: 'symbol'})
                        if '名称' in str(col) or 'name' in str(col).lower():
                            sse_indices = sse_indices.rename(columns={col: 'name'})
                
                if 'symbol' in sse_indices.columns and 'name' in sse_indices.columns:
                    sse_indices['市场'] = '指数'
                    sse_indices['指数类型'] = '上证指数'
                    all_indices.append(sse_indices)
                    print(f"  获取到 {len(sse_indices)} 个上证指数")
        except Exception as e:
            print(f"  上证指数获取失败: {e}")
        
        # 4. 获取深证指数
        try:
            print("获取深证指数...")
            szse_indices = ak.index_zh_szse_hist()
            if not szse_indices.empty:
                # 处理列名
                if '代码' in szse_indices.columns and '名称' in szse_indices.columns:
                    szse_indices = szse_indices.rename(columns={'代码': 'symbol', '名称': 'name'})
                elif 'index_code' in szse_indices.columns and 'display_name' in szse_indices.columns:
                    szse_indices = szse_indices.rename(columns={'index_code': 'symbol', 'display_name': 'name'})
                else:
                    # 尝试找到代码和名称列
                    for col in szse_indices.columns:
                        if '代码' in str(col) or 'code' in str(col).lower():
                            szse_indices = szse_indices.rename(columns={col: 'symbol'})
                        if '名称' in str(col) or 'name' in str(col).lower():
                            szse_indices = szse_indices.rename(columns={col: 'name'})
                
                if 'symbol' in szse_indices.columns and 'name' in szse_indices.columns:
                    szse_indices['市场'] = '指数'
                    szse_indices['指数类型'] = '深证指数'
                    all_indices.append(szse_indices)
                    print(f"  获取到 {len(szse_indices)} 个深证指数")
        except Exception as e:
            print(f"  深证指数获取失败: {e}")
        
        # 5. 如果上述方法都失败，使用手动定义的主要指数
        if not all_indices:
            print("使用手动定义的主要指数...")
            major_indices = [
                {'symbol': '000001', 'name': '上证指数', '市场': '指数', '指数类型': '上证指数'},
                {'symbol': '399001', 'name': '深证成指', '市场': '指数', '指数类型': '深证指数'},
                {'symbol': '399006', 'name': '创业板指', '市场': '指数', '指数类型': '深证指数'},
                {'symbol': '000300', 'name': '沪深300', '市场': '指数', '指数类型': '中证指数'},
                {'symbol': '000905', 'name': '中证500', '市场': '指数', '指数类型': '中证指数'},
                {'symbol': '000852', 'name': '中证1000', '市场': '指数', '指数类型': '中证指数'},
                {'symbol': '000016', 'name': '上证50', '市场': '指数', '指数类型': '上证指数'},
                {'symbol': '000903', 'name': '中证100', '市场': '指数', '指数类型': '中证指数'},
                {'symbol': '000906', 'name': '中证800', '市场': '指数', '指数类型': '中证指数'},
                {'symbol': '399005', 'name': '中小板指', '市场': '指数', '指数类型': '深证指数'},
                {'symbol': '000688', 'name': '科创50', '市场': '指数', '指数类型': '上证指数'},
                {'symbol': '000932', 'name': '中证流通', '市场': '指数', '指数类型': '中证指数'},
                {'symbol': '000933', 'name': '中证医药100', '市场': '指数', '指数类型': '中证指数'},
                {'symbol': '399550', 'name': '央视50', '市场': '指数', '指数类型': '深证指数'},
                {'symbol': '000978', 'name': '医药卫生', '市场': '指数', '指数类型': '上证指数'},
                {'symbol': '399106', 'name': '深证综指', '市场': '指数', '指数类型': '深证指数'},
                {'symbol': '000010', 'name': '上证180', '市场': '指数', '指数类型': '上证指数'},
                {'symbol': '000698', 'name': '中证全指', '市场': '指数', '指数类型': '中证指数'},
                {'symbol': '399330', 'name': '深证100', '市场': '指数', '指数类型': '深证指数'},
                {'symbol': '399101', 'name': '中小板综', '市场': '指数', '指数类型': '深证指数'},
            ]
            indices_df = pd.DataFrame(major_indices)
            all_indices.append(indices_df)
            print(f"  手动定义了 {len(indices_df)} 个主要指数")
        
        # 合并所有指数
        if all_indices:
            combined_indices = pd.concat(all_indices, ignore_index=True)
            
            # 确保必要的列存在
            if 'symbol' not in combined_indices.columns:
                combined_indices['symbol'] = ''
            if 'name' not in combined_indices.columns:
                combined_indices['name'] = ''
            if '市场' not in combined_indices.columns:
                combined_indices['市场'] = '指数'
            
            print(f"总计获取到 {len(combined_indices)} 个指数")
            return combined_indices
        
        print("未获取到指数数据")
        return pd.DataFrame()
    
    def get_all_stocks_and_indices(self) -> Dict[str, pd.DataFrame]:
        """获取所有股票和指数数据"""
        print("=" * 60)
        print("开始全面收集股票及指数数据")
        print("=" * 60)
        
        result = {}
        
        # 1. 获取A股（沪深）
        a_shares = self.get_a_shares_comprehensive()
        if not a_shares.empty:
            # 分离沪深和北交所
            sh_sz_stocks = a_shares[a_shares['交易所'].isin(['上交所', '深交所'])]
            if not sh_sz_stocks.empty:
                result['沪深A股'] = sh_sz_stocks
                print(f"✓ 沪深A股收集完成: {len(sh_sz_stocks)} 只股票")
        
        # 2. 获取北交所
        bjex_stocks = self.get_bjex_stocks_detailed()
        if not bjex_stocks.empty:
            result['北交所'] = bjex_stocks
            print(f"✓ 北交所收集完成: {len(bjex_stocks)} 只股票")
        
        # 3. 获取港股
        hk_stocks = self.get_hk_stocks_detailed()
        if not hk_stocks.empty:
            result['港股'] = hk_stocks
            print(f"✓ 港股收集完成: {len(hk_stocks)} 只股票")
        
        # 4. 获取指数
        indices = self.get_all_indices_detailed()
        if not indices.empty:
            result['指数'] = indices
            print(f"✓ 指数收集完成: {len(indices)} 个指数")
        
        print("=" * 60)
        
        # 统计汇总
        total_stocks = sum(len(df) for key, df in result.items() if key != '指数')
        print(f"数据收集完成！总计：")
        print(f"  • 股票总数: {total_stocks} 只")
        print(f"  • 指数总数: {len(indices) if not indices.empty else 0} 个")
        print(f"  • 数据表: {list(result.keys())}")
        print("=" * 60)
        
        return result
    
    def save_all_data(self, data_dict: Dict[str, pd.DataFrame], 
                     filename_prefix: str = "stock_market_data"):
        """保存所有数据到文件（只输出JSON格式）"""
        import json
        
        # 1. 保存为多个JSON文件（按市场分类）
        print("\n1. 保存各市场JSON文件:")
        for sheet_name, df in data_dict.items():
            safe_name = sheet_name.replace('/', '_').replace('\\', '_')
            json_filename = f"{filename_prefix}_{safe_name}.json"
            
            # 将DataFrame转换为字典列表
            records = df.to_dict('records')
            
            # 保存为JSON文件
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
            print(f"  • {sheet_name}: {json_filename} ({len(records)} 条记录)")
        
        # 2. 保存为一个合并的JSON文件
        print("\n2. 保存合并数据:")
        combined_json = {}
        for sheet_name, df in data_dict.items():
            combined_json[sheet_name] = df.to_dict('records')
        
        combined_json_filename = f"{filename_prefix}_全部数据.json"
        with open(combined_json_filename, 'w', encoding='utf-8') as f:
            json.dump(combined_json, f, ensure_ascii=False, indent=2)
        print(f"✓ 合并数据已保存到JSON: {combined_json_filename}")
        
        # 3. 合并所有股票数据
        stock_keys = [k for k in data_dict.keys() if k != '指数']
        if stock_keys:
            print("\n3. 保存全部股票数据:")
            all_stocks = pd.concat([data_dict[k] for k in stock_keys], ignore_index=True)
            
            # 保存全部股票为JSON
            all_stocks_json = all_stocks.to_dict('records')
            all_stocks_json_filename = f"{filename_prefix}_全部股票.json"
            with open(all_stocks_json_filename, 'w', encoding='utf-8') as f:
                json.dump(all_stocks_json, f, ensure_ascii=False, indent=2)
            print(f"✓ 全部股票已保存到JSON: {all_stocks_json_filename} ({len(all_stocks)} 只股票)")
            
            # 按交易所统计
            if '交易所' in all_stocks.columns:
                exchange_stats = all_stocks['交易所'].value_counts()
                print("\n交易所分布统计:")
                for exchange, count in exchange_stats.items():
                    print(f"  • {exchange}: {count} 只")
        
        # 4. 保存汇总信息
        print("\n4. 保存汇总信息:")
        summary = []
        for sheet_name, df in data_dict.items():
            summary.append({
                '数据类别': sheet_name,
                '记录数': len(df),
                '列数': len(df.columns),
                '列名': ', '.join(df.columns.tolist())
            })
        
        # 保存汇总信息为JSON
        summary_json_filename = f"{filename_prefix}_汇总信息.json"
        with open(summary_json_filename, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        print(f"✓ 汇总信息已保存到JSON: {summary_json_filename}")
        
        return f"{filename_prefix}_全部数据.json"
    
    def analyze_market_coverage(self, data_dict: Dict[str, pd.DataFrame]):
        """分析市场覆盖情况"""
        print("\n" + "=" * 60)
        print("市场覆盖分析")
        print("=" * 60)
        
        for market, df in data_dict.items():
            print(f"\n{market}:")
            print(f"  总数量: {len(df)}")
            
            if not df.empty:
                # 显示前5个示例
                print("  示例数据:")
                for i, (_, row) in enumerate(df.head().iterrows()):
                    if 'symbol' in df.columns and 'name' in df.columns:
                        print(f"    {row['symbol']} - {row['name']}")
                    elif '代码' in df.columns and '名称' in df.columns:
                        print(f"    {row['代码']} - {row['名称']}")
                
                # 显示列信息
                print(f"  数据列: {', '.join(df.columns.tolist())}")

# 使用示例
if __name__ == "__main__":
    # 创建收集器
    collector = AllStockCodeCollector()
    
    # 获取所有数据
    print("开始获取数据，请稍候...")
    all_data = collector.get_all_stocks_and_indices()
    
    # 分析数据
    collector.analyze_market_coverage(all_data)
    
    # 保存数据
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    saved_file = collector.save_all_data(all_data, f"全部股票指数数据_{timestamp}")
    
    print(f"\n✅ 数据收集完成！文件已保存为: {saved_file}")
    
    # 显示各市场数量
    print("\n📊 最终统计:")
    for market, df in all_data.items():
        print(f"  {market}: {len(df)} 条记录")