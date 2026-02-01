#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基金信息更新脚本
当查询的基金代码不在本地缓存中时，自动更新基金信息
"""

import json
import os
import akshare as ak
from datetime import datetime

def update_fund_info():
    """
    更新基金信息缓存
    """
    print("开始更新基金信息缓存...")
    
    try:
        # 使用akshare获取全部基金信息
        fund_info = ak.fund_name_em()
        print(f"成功获取 {len(fund_info)} 只基金信息")
        
        # 转换为字典格式，方便快速查询
        fund_dict = {}
        for _, row in fund_info.iterrows():
            fund_code = row['基金代码']
            fund_name = row['基金简称']
            fund_pinyin = row['拼音缩写']
            fund_type = row['基金类型']
            fund_dict[fund_code] = {
                '名称': fund_name,
                '拼音缩写': fund_pinyin,
                '类型': fund_type
            }
        
        # 保存为JSON文件
        json_path = os.path.join(os.path.dirname(__file__), '..', 'cache', 'fund_info.json')
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(fund_dict, f, ensure_ascii=False, indent=2)
        print(f"基金信息已保存到 {json_path}")
        
        # 保存为CSV文件（可选）
        csv_path = os.path.join(os.path.dirname(__file__), '..', 'cache', 'fund_info.csv')
        fund_info.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"基金信息已保存到 {csv_path}")
        
        # 保存爬取元数据
        metadata = {
            "crawl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "fund_count": len(fund_dict),
            "version": "1.0"
        }
        metadata_path = os.path.join(os.path.dirname(__file__), '..', 'cache', 'fund_info_metadata.json')
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        return fund_dict
        
    except Exception as e:
        print(f"更新基金信息失败: {e}")
        return None

def check_fund_code(fund_code):
    """
    检查基金代码是否在本地缓存中
    
    Args:
        fund_code (str): 基金代码
        
    Returns:
        bool: True表示在缓存中，False表示不在
    """
    try:
        # 检查JSON文件是否存在
        json_path = os.path.join(os.path.dirname(__file__), '..', 'cache', 'fund_info.json')
        if not os.path.exists(json_path):
            return False
        
        # 读取JSON文件
        with open(json_path, 'r', encoding='utf-8') as f:
            fund_dict = json.load(f)
        
        return fund_code in fund_dict
        
    except Exception as e:
        print(f"检查基金代码失败: {e}")
        return False

if __name__ == "__main__":
    update_fund_info()