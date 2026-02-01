#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基金搜索工具
支持通过基金代码、基金简称、拼音缩写进行多关键字查询
"""

import json
import os

def load_fund_info():
    """
    加载基金信息缓存
    
    Returns:
        dict: 基金信息字典
    """
    try:
        json_path = os.path.join(os.path.dirname(__file__), '..', 'cache', 'fund_info.json')
        with open(json_path, 'r', encoding='utf-8') as f:
            fund_dict = json.load(f)
        print(f"成功加载 {len(fund_dict)} 只基金信息")
        return fund_dict
    except Exception as e:
        print(f"加载基金信息失败: {e}")
        return None

def search_funds(keyword, fund_dict=None):
    """
    多关键字搜索基金
    
    Args:
        keyword (str): 搜索关键字（基金代码、简称、拼音缩写）
        fund_dict (dict): 基金信息字典，默认自动加载
        
    Returns:
        list: 匹配的基金列表
    """
    if fund_dict is None:
        fund_dict = load_fund_info()
        if fund_dict is None:
            return []
    
    results = []
    keyword_lower = keyword.lower()
    
    for fund_code, fund_info in fund_dict.items():
        # 检查基金代码
        if keyword_lower in fund_code.lower():
            results.append({
                '代码': fund_code,
                '名称': fund_info['名称'],
                '拼音缩写': fund_info['拼音缩写'],
                '类型': fund_info['类型']
            })
            continue
        
        # 检查基金简称
        if keyword_lower in fund_info['名称'].lower():
            results.append({
                '代码': fund_code,
                '名称': fund_info['名称'],
                '拼音缩写': fund_info['拼音缩写'],
                '类型': fund_info['类型']
            })
            continue
        
        # 检查拼音缩写
        if keyword_lower in fund_info['拼音缩写'].lower():
            results.append({
                '代码': fund_code,
                '名称': fund_info['名称'],
                '拼音缩写': fund_info['拼音缩写'],
                '类型': fund_info['类型']
            })
            continue
    
    return results

def print_search_results(results):
    """
    打印搜索结果
    
    Args:
        results (list): 搜索结果列表
    """
    if not results:
        print("未找到匹配的基金")
        return
    
    print(f"\n找到 {len(results)} 只匹配的基金:")
    print("-" * 100)
    print(f"{'代码':<8} {'名称':<30} {'拼音缩写':<15} {'类型':<20}")
    print("-" * 100)
    
    for fund in results:
        print(f"{fund['代码']:<8} {fund['名称']:<30} {fund['拼音缩写']:<15} {fund['类型']:<20}")
    
    print("-" * 100)

if __name__ == "__main__":
    # 示例使用
    keyword = input("请输入搜索关键字（基金代码、简称、拼音缩写）: ")
    results = search_funds(keyword)
    print_search_results(results)