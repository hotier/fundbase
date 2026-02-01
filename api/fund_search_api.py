#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基金搜索API接口
提供前端搜索功能，支持多关键字查询
"""

import json
import os
from flask import Blueprint, request, jsonify

fund_search_bp = Blueprint('fund_search', __name__)

# 全局缓存基金信息
FUND_DICT = None

def load_fund_info():
    """
    加载基金信息缓存
    
    Returns:
        dict: 基金信息字典
    """
    global FUND_DICT
    if FUND_DICT is not None:
        return FUND_DICT
    
    try:
        json_path = os.path.join(os.path.dirname(__file__), '..', 'cache', 'fund_info.json')
        with open(json_path, 'r', encoding='utf-8') as f:
            FUND_DICT = json.load(f)
        print(f"成功加载 {len(FUND_DICT)} 只基金信息")
        return FUND_DICT
    except Exception as e:
        print(f"加载基金信息失败: {e}")
        return None

def search_funds(keyword):
    """
    多关键字搜索基金
    
    Args:
        keyword (str): 搜索关键字（基金代码、简称、拼音缩写）
        
    Returns:
        list: 匹配的基金列表
    """
    fund_dict = load_fund_info()
    if fund_dict is None:
        return []
    
    results = []
    keyword_lower = keyword.lower()
    
    for fund_code, fund_info in fund_dict.items():
        # 检查基金代码
        if keyword_lower in fund_code.lower():
            results.append({
                'code': fund_code,
                'name': fund_info['名称'],
                'pinyin': fund_info['拼音缩写'],
                'type': fund_info['类型']
            })
            continue
        
        # 检查基金简称
        if keyword_lower in fund_info['名称'].lower():
            results.append({
                'code': fund_code,
                'name': fund_info['名称'],
                'pinyin': fund_info['拼音缩写'],
                'type': fund_info['类型']
            })
            continue
        
        # 检查拼音缩写
        if keyword_lower in fund_info['拼音缩写'].lower():
            results.append({
                'code': fund_code,
                'name': fund_info['名称'],
                'pinyin': fund_info['拼音缩写'],
                'type': fund_info['类型']
            })
            continue
    
    return results

@fund_search_bp.route('/api/search', methods=['GET'])
def api_search_funds():
    """
    基金搜索API接口
    
    Query Parameters:
        keyword (str): 搜索关键字
        
    Returns:
        json: 匹配的基金列表
    """
    keyword = request.args.get('keyword', '')
    if not keyword:
        return jsonify({'error': '缺少搜索关键字'}), 400
    
    results = search_funds(keyword)
    return jsonify({
        'total': len(results),
        'results': results
    })

if __name__ == "__main__":
    # 测试API
    from flask import Flask
    app = Flask(__name__)
    app.register_blueprint(fund_search_bp)
    
    print("基金搜索API服务启动...")
    app.run(debug=True, port=5001)