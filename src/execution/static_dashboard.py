import pandas as pd
import os
import html
from datetime import datetime, timedelta

def generate_static_dashboard():
    log_file = 'registro_simulacion.csv'
    dec_file = 'bitacora_decisiones.csv'
    html_file = 'DASHBOARD_LIVE.html'
    initial_capital = 1000.0
    
    if not os.path.exists(log_file):
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write("<html><body style='background:#121212; color:white; padding:50px;'><h1>🚀 Bot v5.5 Iniciando...</h1></body></html>")
        return

    try:
        df = pd.read_csv(log_file)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        closed = df[df['status'] == 'CLOSED']
        total_pnl_usd = ((closed['pnl'] / 100) * closed['usd_invested']).sum() if not closed.empty else 0
        current_balance = initial_capital + total_pnl_usd
        
        open_pos = df[df['status'] == 'OPEN'].iloc[::-1].to_dict('records')
        
        decisions = []
        if os.path.exists(dec_file):
            df_dec = pd.read_csv(dec_file)
            decisions = df_dec.iloc[::-1].head(15).to_dict('records')

        def sanitize(val): return html.escape(str(val))

        dec_rows = ""
        for d in decisions:
            eff_color = "#aaa"
            if "CORRECT" in str(d['efficacy']): eff_color = "#00ff88"
            elif "FALSE" in str(d['efficacy']): eff_color = "#ff4444"
            
            dec_rows += f"""
            <tr>
                <td>{sanitize(d['timestamp'])}</td>
                <td>{sanitize(d['symbol'])}</td>
                <td>{sanitize(d['side'])}</td>
                <td>${d['price']}</td>
                <td>{d['sentiment']}</td>
                <td style="color:{'#00ff88' if d['verdict']=='OPENED' else '#ffa500'}">{d['verdict']}</td>
                <td style="color:{eff_color}; font-weight:bold;">{sanitize(d['efficacy'])}</td>
            </tr>
            """

        html_content = f"""
        <html>
        <head>
            <title>Audit Dashboard v5.5</title>
            <meta http-equiv="refresh" content="30">
            <style>
                body {{ font-family: 'Segoe UI', sans-serif; background: #0a0a0a; color: #e0e0e0; margin: 0; padding: 20px; }}
                .container {{ max-width: 1200px; margin: auto; }}
                .header {{ display: flex; justify-content: space-between; align-items: center; background: #1a1a1a; padding: 20px; border-radius: 15px; border: 1px solid #333; }}
                .stat-value {{ font-size: 26px; font-weight: bold; color: #00ffcc; }}
                h2 {{ border-left: 5px solid #00ffcc; padding-left: 15px; margin-top: 40px; color: #fff; }}
                table {{ width: 100%; border-collapse: collapse; margin-top: 15px; background: #111; border-radius: 10px; overflow: hidden; }}
                th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #222; }}
                th {{ background: #1a1a1a; color: #00ffcc; font-size: 10px; text-transform: uppercase; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <div><h1>🚀 SCALPING AUDIT <span style='color:#555;'>v5.5</span></h1><p>🛡️ EVALUANDO EFICACIA DE IA EN TIEMPO REAL</p></div>
                    <div style="text-align:right"><div>Balance Est.</div><div class="stat-value">${current_balance:.2f}</div></div>
                </div>

                <h2>🧠 BITÁCORA DE IA Y AUDITORÍA DE FILTROS</h2>
                <p style="font-size:12px; color:#888; margin-left:20px;">
                    La columna <b>Efficacy</b> analiza si el filtrado fue correcto tras esperar 15 velas.<br>
                    ✅ CORRECT: El filtro evitó una pérdida. | ❌ FALSE: El filtro nos hizo perder una ganancia.
                </p>
                <table>
                    <thead><tr><th>Hora</th><th>Símbolo</th><th>Lado</th><th>Precio</th><th>IA Score</th><th>Veredicto</th><th>Eficacia (Audit)</th></tr></thead>
                    <tbody>{dec_rows if dec_rows else "<tr><td colspan='7' style='text-align:center;'>Esperando señales...</td></tr>"}</tbody>
                </table>

                <h2>📈 POSICIONES ABIERTAS</h2>
                <table>
                    <thead><tr><th>Inicio</th><th>Símbolo</th><th>Inversión</th><th>Entrada</th><th>SL</th><th>TP</th></tr></thead>
                    <tbody>
                        {''.join([f"<tr><td>{p['timestamp']}</td><td>{p['symbol']}</td><td>${p['usd_invested']:.2f}</td><td>${p['price']}</td><td>${p['sl']}</td><td>${p['tp']}</td></tr>" for p in open_pos]) if open_pos else "<tr><td colspan='6' style='text-align:center; padding:20px;'>Buscando confluencia...</td></tr>"}
                    </tbody>
                </table>
            </div>
        </body>
        </html>
        """
        with open(html_file, 'w', encoding='utf-8') as f: f.write(html_content)
    except Exception as e: print(f"Error HTML: {e}")

if __name__ == "__main__": generate_static_dashboard()
