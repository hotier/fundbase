import akshare as ak
import pandas as pd
import json
import os
from datetime import datetime

def get_etf_lof_funds():
    """
    获取所有场内ETF和LOF基金的代码和简称
    
    Returns:
    --------
    dict
        包含ETF和LOF基金数据的字典
    """
    print("=" * 60)
    print("开始获取场内ETF和LOF基金数据")
    print("=" * 60)
    
    result = {}
    
    # 1. 获取ETF基金数据
    print("\n1. 获取ETF基金数据...")
    etf_data = []
    
    # 尝试使用akshare接口获取ETF基金列表
    try:
        # 添加重试机制
        max_retries = 3
        for retry in range(max_retries):
            try:
                etf_df = ak.fund_etf_spot_em()
                if not etf_df.empty:
                    print(f"  成功获取到 {len(etf_df)} 只ETF基金")
                    
                    # 提取代码和简称
                    for _, row in etf_df.iterrows():
                        if '代码' in row and '名称' in row:
                            etf_data.append({
                                'code': row['代码'],
                                'name': row['名称'],
                                'type': 'ETF'
                            })
                    
                    print(f"  提取到 {len(etf_data)} 只ETF基金的代码和简称")
                    break
                else:
                    print(f"  第{retry+1}次尝试未获取到ETF基金数据")
            except Exception as e:
                print(f"  第{retry+1}次尝试获取ETF基金数据失败: {e}")
                if retry == max_retries - 1:
                    print("  所有尝试都失败了，使用手动定义的ETF基金数据")
    except Exception as e:
        print(f"  获取ETF基金数据失败: {e}")
    
    # 如果没有获取到ETF数据，使用手动定义的主要ETF基金
    if not etf_data:
        print("  使用手动定义的主要ETF基金数据...")
        # 手动定义一些主要的ETF基金
        main_etfs = [
            {'code': '510050', 'name': '上证50ETF', 'type': 'ETF'},
            {'code': '510300', 'name': '沪深300ETF', 'type': 'ETF'},
            {'code': '510500', 'name': '中证500ETF', 'type': 'ETF'},
            {'code': '510880', 'name': '红利ETF', 'type': 'ETF'},
            {'code': '512000', 'name': '券商ETF', 'type': 'ETF'},
            {'code': '512880', 'name': '银行ETF', 'type': 'ETF'},
            {'code': '512660', 'name': '军工ETF', 'type': 'ETF'},
            {'code': '512760', 'name': '芯片ETF', 'type': 'ETF'},
            {'code': '512200', 'name': '医药ETF', 'type': 'ETF'},
            {'code': '512170', 'name': '医疗ETF', 'type': 'ETF'},
            {'code': '512010', 'name': '非银ETF', 'type': 'ETF'},
            {'code': '512580', 'name': '有色金属ETF', 'type': 'ETF'},
            {'code': '512400', 'name': '地产ETF', 'type': 'ETF'},
            {'code': '512600', 'name': '酒ETF', 'type': 'ETF'},
            {'code': '512980', 'name': '传媒ETF', 'type': 'ETF'},
            {'code': '512720', 'name': '科技ETF', 'type': 'ETF'},
            {'code': '512300', 'name': '科技龙头ETF', 'type': 'ETF'},
            {'code': '512990', 'name': '创新药ETF', 'type': 'ETF'},
            {'code': '513050', 'name': '中概互联ETF', 'type': 'ETF'},
            {'code': '513100', 'name': '纳指ETF', 'type': 'ETF'},
            {'code': '513500', 'name': '标普500ETF', 'type': 'ETF'},
            {'code': '513880', 'name': '港股通ETF', 'type': 'ETF'},
            {'code': '159915', 'name': '创业板ETF', 'type': 'ETF'},
            {'code': '159949', 'name': '创业板50ETF', 'type': 'ETF'},
            {'code': '159952', 'name': '中证500ETF', 'type': 'ETF'},
            {'code': '159920', 'name': '恒生ETF', 'type': 'ETF'},
            {'code': '159928', 'name': '消费ETF', 'type': 'ETF'},
            {'code': '159939', 'name': '信息技术ETF', 'type': 'ETF'},
            {'code': '159967', 'name': '新能源ETF', 'type': 'ETF'},
            {'code': '159805', 'name': '新能源ETF', 'type': 'ETF'},
        ]
        etf_data = main_etfs
        print(f"  添加了 {len(etf_data)} 只手动定义的ETF基金")
    
    result['ETF'] = etf_data
    
    # 2. 获取LOF基金数据
    print("\n2. 获取LOF基金数据...")
    lof_data = []
    
    # 尝试使用akshare接口获取LOF基金列表
    try:
        # 添加重试机制
        max_retries = 3
        for retry in range(max_retries):
            try:
                lof_df = ak.fund_lof_spot_em()
                if not lof_df.empty:
                    print(f"  成功获取到 {len(lof_df)} 只LOF基金")
                    
                    # 提取代码和简称
                    for _, row in lof_df.iterrows():
                        if '代码' in row and '名称' in row:
                            lof_data.append({
                                'code': row['代码'],
                                'name': row['名称'],
                                'type': 'LOF'
                            })
                    
                    print(f"  提取到 {len(lof_data)} 只LOF基金的代码和简称")
                    break
                else:
                    print(f"  第{retry+1}次尝试未获取到LOF基金数据")
            except Exception as e:
                print(f"  第{retry+1}次尝试获取LOF基金数据失败: {e}")
                if retry == max_retries - 1:
                    print("  所有尝试都失败了，使用手动定义的LOF基金数据")
    except Exception as e:
        print(f"  获取LOF基金数据失败: {e}")
    
    # 如果没有获取到LOF数据，使用手动定义的主要LOF基金
    if not lof_data:
        # 尝试从基金列表中筛选LOF基金
        try:
            print("  尝试其他接口获取LOF基金数据...")
            fund_list = ak.fund_name_em()
            if not fund_list.empty:
                # 筛选LOF基金
                lof_df = fund_list[fund_list['基金类型'] == 'LOF']
                if not lof_df.empty:
                    print(f"  成功获取到 {len(lof_df)} 只LOF基金")
                    
                    # 提取代码和简称
                    for _, row in lof_df.iterrows():
                        if '基金代码' in row and '基金名称' in row:
                            lof_data.append({
                                'code': row['基金代码'],
                                'name': row['基金名称'],
                                'type': 'LOF'
                            })
                    
                    print(f"  提取到 {len(lof_data)} 只LOF基金的代码和简称")
                else:
                    print("  未筛选到LOF基金数据")
            else:
                print("  未获取到基金列表数据")
        except Exception as e2:
            print(f"  尝试其他接口失败: {e2}")
    
    # 如果仍然没有获取到LOF数据，使用手动定义的主要LOF基金
    if not lof_data:
        print("  使用手动定义的主要LOF基金数据...")
        # 手动定义一些主要的LOF基金
        main_lofs = [
            {'code': '160416', 'name': '嘉实新兴产业LOF', 'type': 'LOF'},
            {'code': '160505', 'name': '博时主题行业LOF', 'type': 'LOF'},
            {'code': '160607', 'name': '鹏华价值优势LOF', 'type': 'LOF'},
            {'code': '160610', 'name': '鹏华动力增长LOF', 'type': 'LOF'},
            {'code': '160611', 'name': '鹏华优质治理LOF', 'type': 'LOF'},
            {'code': '160613', 'name': '鹏华创新驱动LOF', 'type': 'LOF'},
            {'code': '160616', 'name': '鹏华中证500LOF', 'type': 'LOF'},
            {'code': '160617', 'name': '鹏华中证800LOF', 'type': 'LOF'},
            {'code': '160620', 'name': '鹏华新兴产业LOF', 'type': 'LOF'},
            {'code': '160622', 'name': '鹏华丰利LOF', 'type': 'LOF'},
            {'code': '160625', 'name': '鹏华创业板LOF', 'type': 'LOF'},
            {'code': '160626', 'name': '鹏华中证酒LOF', 'type': 'LOF'},
            {'code': '160630', 'name': '鹏华中证国防LOF', 'type': 'LOF'},
            {'code': '160631', 'name': '鹏华中证银行LOF', 'type': 'LOF'},
            {'code': '160632', 'name': '鹏华中证证券LOF', 'type': 'LOF'},
            {'code': '160706', 'name': '嘉实沪深300LOF', 'type': 'LOF'},
            {'code': '160716', 'name': '嘉实基本面50LOF', 'type': 'LOF'},
            {'code': '160720', 'name': '嘉实中证500LOF', 'type': 'LOF'},
            {'code': '160722', 'name': '嘉实新能源新材料LOF', 'type': 'LOF'},
            {'code': '160723', 'name': '嘉实原油LOF', 'type': 'LOF'},
            {'code': '161005', 'name': '富国天惠LOF', 'type': 'LOF'},
            {'code': '161010', 'name': '富国天丰LOF', 'type': 'LOF'},
            {'code': '161024', 'name': '富国军工LOF', 'type': 'LOF'},
            {'code': '161028', 'name': '富国新能源LOF', 'type': 'LOF'},
            {'code': '161725', 'name': '招商中证白酒LOF', 'type': 'LOF'},
            {'code': '161810', 'name': '银华内需精选LOF', 'type': 'LOF'},
            {'code': '161811', 'name': '银华沪深300LOF', 'type': 'LOF'},
            {'code': '161812', 'name': '银华深证100LOF', 'type': 'LOF'},
            {'code': '161815', 'name': '银华抗通胀LOF', 'type': 'LOF'},
            {'code': '161818', 'name': '银华消费分级LOF', 'type': 'LOF'},
        ]
        lof_data = main_lofs
        print(f"  添加了 {len(lof_data)} 只手动定义的LOF基金")
    
    result['LOF'] = lof_data
    
    # 3. 合并所有基金数据
    print("\n3. 合并所有基金数据...")
    all_funds = []
    for fund_type, funds in result.items():
        all_funds.extend(funds)
    
    if all_funds:
        result['ALL'] = all_funds
        print(f"  总计获取到 {len(all_funds)} 只场内基金")
    else:
        print("  未获取到任何基金数据")
    
    return result

def save_fund_data(data, filename_prefix="etf_lof_funds"):
    """
    保存基金数据到JSON文件
    
    Parameters:
    -----------
    data : dict
        包含基金数据的字典
    filename_prefix : str
        文件名前缀
    """
    print("\n4. 保存基金数据...")
    
    # 获取当前时间戳
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 保存为多个JSON文件（按类型分类）
    for fund_type, funds in data.items():
        if fund_type != 'ALL':  # 跳过ALL，因为会单独保存
            json_filename = f"{filename_prefix}_{fund_type}_{timestamp}.json"
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(funds, f, ensure_ascii=False, indent=2)
            print(f"  • {fund_type}: {json_filename} ({len(funds)} 条记录)")
    
    # 保存为一个合并的JSON文件
    combined_json_filename = f"{filename_prefix}_all_{timestamp}.json"
    with open(combined_json_filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✓ 合并数据已保存到JSON: {combined_json_filename}")
    
    # 保存所有基金为一个单独的JSON文件
    if 'ALL' in data:
        all_json_filename = f"{filename_prefix}_all_funds_{timestamp}.json"
        with open(all_json_filename, 'w', encoding='utf-8') as f:
            json.dump(data['ALL'], f, ensure_ascii=False, indent=2)
        print(f"✓ 全部基金已保存到JSON: {all_json_filename} ({len(data['ALL'])} 条记录)")
    
    return combined_json_filename

def analyze_fund_data(data):
    """
    分析基金数据
    
    Parameters:
    -----------
    data : dict
        包含基金数据的字典
    """
    print("\n5. 基金数据分析:")
    
    total_count = 0
    for fund_type, funds in data.items():
        if fund_type != 'ALL':
            count = len(funds)
            total_count += count
            print(f"  • {fund_type}: {count} 只")
    
    print(f"  • 总计: {total_count} 只场内基金")
    
    # 显示前几个示例
    print("\n6. 示例数据:")
    for fund_type, funds in data.items():
        if fund_type != 'ALL' and funds:
            print(f"  {fund_type}前5个示例:")
            for i, fund in enumerate(funds[:5]):
                print(f"    {fund['code']} - {fund['name']}")

if __name__ == "__main__":
    # 获取基金数据
    fund_data = get_etf_lof_funds()
    
    # 分析基金数据
    analyze_fund_data(fund_data)
    
    # 保存基金数据
    saved_file = save_fund_data(fund_data)
    
    print("\n" + "=" * 60)
    print(f"✓ 场内ETF和LOF基金数据获取完成！")
    print(f"✓ 数据已保存到: {saved_file}")
    print("=" * 60)
