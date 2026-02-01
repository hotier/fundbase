"""
基金实时估值 Web 应用
"""

from flask import Flask, render_template, jsonify, request
from core.fund_realtime_calc import FundRealtimeCalculator, is_trading_time
from api.fund_search_api import fund_search_bp
import os
import pandas as pd

app = Flask(__name__)

# 注册基金搜索API蓝图
app.register_blueprint(fund_search_bp)

app.config['JSON_AS_ASCII'] = False

# 配置模板路径
template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
app.template_folder = template_path

# 创建计算器实例
calculator = FundRealtimeCalculator()


@app.route('/')
def index():
    """首页"""
    trading = is_trading_time()
    return render_template('index.html', is_trading=trading)


@app.route('/api/calculate', methods=['POST'])
def calculate():
    """计算基金估值接口"""
    try:
        data = request.get_json()
        fund_code = data.get('fund_code', '').strip()

        if not fund_code:
            return jsonify({'success': False, 'message': '基金代码不能为空'})

        # 重置计算器
        global calculator
        calculator = FundRealtimeCalculator()

        # 1. 获取基金持仓
        portfolio = calculator.get_fund_portfolio(fund_code, year=None, auto_detect_latest=True)
        if portfolio is None:
            return jsonify({'success': False, 'message': '获取基金持仓失败，请检查基金代码'})

        # 2. 获取股票实时行情
        quotes = calculator.get_stock_realtime_quotes()
        if quotes is None:
            return jsonify({'success': False, 'message': '获取股票行情失败'})

        # 3. 计算估值
        result = calculator.calculate_realtime_value()
        if result is None:
            return jsonify({'success': False, 'message': '计算估值失败'})

        # 格式化返回数据
        stock_details = []
        for _, row in result['stock_details'].iterrows():
            change_str = str(row['涨跌幅'])  # 确保是字符串
            # 处理 NaN 值，替换为 null
            price = row['最新价']
            if pd.isna(price):
                price = None
            ratio = row['占净值比例']
            if pd.isna(ratio):
                ratio = None
            stock_details.append({
                'code': row['股票代码'],
                'name': row['股票名称'],
                'ratio': ratio,
                'price': price,
                'change': change_str
            })

        # 格式化加权涨跌幅为字符串
        weighted_change = result['weighted_change']
        change_value = weighted_change * 100
        if change_value >= 0:
            weighted_change_str = f"+{change_value:.2f}%"
        else:
            weighted_change_str = f"{change_value:.2f}%"

        return jsonify({
            'success': True,
            'data': {
                'fund_code': result['fund_code'],
                'fund_name': result['fund_name'],
                'weighted_change': weighted_change_str,  # 确保是字符串格式
                'calc_time': result['calc_time'],
                'stock_details': stock_details
            }
        })

    except Exception as e:
        return jsonify({'success': False, 'message': f'计算出错: {str(e)}'})


@app.route('/api/trading-time')
def trading_time():
    """获取当前是否为交易时间"""
    trading = is_trading_time()
    return jsonify({'is_trading': trading})


if __name__ == '__main__':
    # 确保 templates 目录存在
    if not os.path.exists('templates'):
        os.makedirs('templates')

    app.run(debug=True, host='0.0.0.0', port=5000)
