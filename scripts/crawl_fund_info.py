#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基金信息爬虫脚本
爬取基金基本信息并保存到本地文件
"""

import json
import time
import os
import pandas as pd
import akshare as ak
from datetime import datetime

def crawl_fund_info():
    """
    爬取基金基本信息
    """
    print("开始爬取基金基本信息...")
    start_time = time.time()
    
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
        
        # 记录爬取时间
        crawl_time = time.time() - start_time
        print(f"爬取完成，耗时 {crawl_time:.2f} 秒")
        
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
        print(f"爬取基金信息失败: {e}")
        return None

if __name__ == "__main__":
    crawl_fund_info()