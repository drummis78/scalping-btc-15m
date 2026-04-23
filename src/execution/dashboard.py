from flask import Flask, render_template_string
import pandas as pd
import os
from datetime import datetime, timedelta

app = Flask(__name__)

@app.route('/')
def dashboard():
    log_file = 'registro_simulacion.csv'
    initial_capital = 1000.0
    
    if not os.path.exists(log_file):
        return "<html><body style='background:#121212; color:white; font-family:sans-serif; padding:50px;'><h1>🚀 Scalping Bot Live</h1><p>Esperando la primera operación...</p></body></html>"
    
    try:
        df = pd.read_csv(log_file)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Calcular Métricas
        closed_trades = df[df['status'] == 'CLOSED']
        
        # PnL USD = sumatoria de (pnl_pct/100 * usd_invested)
        total_pnl_usd = ((closed_trades['pnl'] / 100) * closed_trades['usd_invested']).sum() if not closed_trades.empty else 0
        current_balance = initial_capital + total_pnl_usd
        
        # Rendimiento Diario
        today = datetime.now().date()
        trades_today = closed_trades[df['timestamp'].dt.date == today]
        daily_pnl_usd = ((trades_today['pnl'] / 100) * trades_today['usd_invested']).sum() if not trades_today.empty else 0
        daily_pct = (daily_pnl_usd / initial_capital) * 100
        daily_progress = min(100, max(0, (daily_pnl_usd / (initial_capital * 0.01)) * 100))
        
        # Rendimiento Mensual (últimos 30 días)
        month_ago = datetime.now() - timedelta(days=30)
        trades_month = closed_trades[df['timestamp'] >= month_ago]
        monthly_pnl_usd = ((trades_month['pnl'] / 100) * trades_month['usd_invested']).sum() if not trades_month.empty else 0
        monthly_pct = (monthly_pnl_usd / initial_capital) * 100
        
        # Proyectado Anual
        yearly_pnl_usd = monthly_pnl_usd * 12
        yearly_pct = monthly_pct * 12
        
        open_positions = df[df['status'] == 'OPEN'].iloc[::-1].to_dict('records')
        history = df[df['status'] == 'CLOSED'].iloc[::-1].to_dict('records')
        
    except Exception as e:
        return f"<html><body style='background:#121212; color:white; padding:50px;'><h1>Error</h1><p>{str(e)}</p></body></html>"

    return render_template_string("""
    <html>
        <head>
            <title>Scalping Bot Elite v5.1</title>
            <meta http-equiv="refresh" content="15">
            <style>
                body { font-family: 'Segoe UI', sans-serif; background: #0a0a0a; color: #e0e0e0; margin: 0; padding: 20px; }
                .container { max-width: 1200px; margin: auto; }
                .header { display: flex; justify-content: space-between; align-items: center; background: #1a1a1a; padding: 20px; border-radius: 15px; border: 1px solid #333; }
                .stats-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px; margin-top: 20px; }
                .stat-card { background: #161616; padding: 20px; border-radius: 12px; border: 1px solid #222; text-align: center; }
                .stat-value { font-size: 26px; font-weight: bold; color: #00ffcc; margin-top: 10px; }
                .stat-pct { font-size: 14px; margin-top: 5px; font-weight: bold; }
                .stat-label { font-size: 11px; color: #777; text-transform: uppercase; letter-spacing: 1px; }
                
                .progress-bg { background: #222; border-radius: 10px; height: 12px; margin-top: 15px; }
                .progress-fill { background: linear-gradient(90deg, #00d2ff 0%, #3a7bd5 100%); height: 100%; border-radius: 10px; transition: width 1s; }
                
                h2 { color: #fff; margin-top: 40px; font-weight: 300; display: flex; align-items: center; }
                h2::after { content: ''; flex: 1; height: 1px; background: #333; margin-left: 20px; }
                
                table { width: 100%; border-collapse: collapse; margin-top: 15px; background: #111; border-radius: 10px; overflow: hidden; }
                th, td { padding: 14px; text-align: left; border-bottom: 1px solid #222; }
                th { background: #1a1a1a; color: #00ffcc; font-size: 10px; text-transform: uppercase; }
                
                .badge { padding: 5px 10px; border-radius: 6px; font-size: 10px; font-weight: bold; background: rgba(0, 255, 136, 0.1); color: #00ff88; }
                .pos-pnl { color: #00ff88; }
                .neg-pnl { color: #ff4444; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <div>
                        <h1 style="margin:0; color:#00ffcc;">SCALPING BOT <span style="font-weight:100; color:#555;">v5.1 ELITE</span></h1>
                        <p style="margin:5px 0 0 0; color:#777; font-size:12px;">🟢 SISTEMA AUTÓNOMO ACTIVO | 20 ACTIVOS</p>
                    </div>
                    <div style="text-align: right">
                        <div class="stat-label">Balance Total</div>
                        <div class="stat-value" style="font-size:36px; color:#fff;">${{ "%.2f"|format(balance) }}</div>
                    </div>
                </div>

                <div class="stats-grid">
                    <div class="stat-card">
                        <div class="stat-label">PnL Hoy</div>
                        <div class="stat-value {{ 'pos-pnl' if daily_pnl >= 0 else 'neg-pnl' }}">${{ "%.2f"|format(daily_pnl) }}</div>
                        <div class="stat-pct {{ 'pos-pnl' if daily_pct >= 0 else 'neg-pnl' }}">{{ "%.2f"|format(daily_pct) }}%</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Meta Diaria (1%)</div>
                        <div class="progress-bg"><div class="progress-fill" style="width: {{ daily_progress }}%"></div></div>
                        <div style="font-size:10px; margin-top:8px; color:#555;">{{ daily_progress|round(1) }}% COMPLETADO</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Rendimiento Mensual</div>
                        <div class="stat-value" style="color:#3a7bd5;">${{ "%.2f"|format(monthly_pnl) }}</div>
                        <div class="stat-pct" style="color:#3a7bd5;">{{ "%.2f"|format(monthly_pct) }}%</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Proyectado Anual</div>
                        <div class="stat-value" style="color:#f2994a;">${{ "%.2f"|format(yearly_pnl) }}</div>
                        <div class="stat-pct" style="color:#f2994a;">{{ "%.2f"|format(yearly_pct) }}%</div>
                    </div>
                </div>
                
                <h2>📈 POSICIONES ABIERTAS</h2>
                <table>
                    <thead>
                        <tr><th>Inicio</th><th>Símbolo</th><th>Inversión</th><th>Entrada</th><th>SL</th><th>TP</th><th>IA Score</th></tr>
                    </thead>
                    <tbody>
                        {% for trade in open_positions %}
                        <tr>
                            <td>{{ trade.timestamp.strftime('%H:%M:%S') }}</td>
                            <td><span class="badge">{{ trade.symbol }}</span></td>
                            <td>${{ "%.2f"|format(trade.usd_invested) }}</td>
                            <td>${{ trade.price }}</td>
                            <td style="color:#ff4444;">${{ trade.sl }}</td>
                            <td style="color:#00ff88;">${{ trade.tp }}</td>
                            <td style="color:#00d2ff;">{{ trade.sentiment }}</td>
                        </tr>
                        {% endfor %}
                        {% if not open_positions %}<tr><td colspan="7" style="text-align:center; padding:30px; color:#444;">Buscando oportunidades en 15m...</td></tr>{% endif %}
                    </tbody>
                </table>

                <h2>📜 HISTORIAL</h2>
                <table>
                    <thead>
                        <tr><th>Fecha</th><th>Símbolo</th><th>Inversión</th><th>Neto PnL</th><th>PnL %</th></tr>
                    </thead>
                    <tbody>
                        {% for trade in history[:10] %}
                        <tr>
                            <td>{{ trade.timestamp.strftime('%d/%m %H:%M') }}</td>
                            <td>{{ trade.symbol }}</td>
                            <td>${{ "%.2f"|format(trade.usd_invested) }}</td>
                            <td class="{{ 'pos-pnl' if trade.pnl > 0 else 'neg-pnl' }}">${{ "%.2f"|format((trade.pnl/100) * trade.usd_invested) }}</td>
                            <td class="{{ 'pos-pnl' if trade.pnl > 0 else 'neg-pnl' }}">{{ trade.pnl|round(2) }}%</td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </body>
    </html>
    """, balance=current_balance, daily_pnl=daily_pnl_usd, daily_pct=daily_pct, 
       monthly_pnl=monthly_pnl_usd, monthly_pct=monthly_pct,
       yearly_pnl=yearly_pnl_usd, yearly_pct=yearly_pct,
       daily_progress=daily_progress, open_positions=open_positions, history=history)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001)
