import pandas as pd
import os

def create_comparison_report():
    # 1. Leer resultados de Python
    py_results_file = 'data/optimization_results.csv'
    if not os.path.exists(py_results_file):
        print("No se encontró el archivo de resultados de Python.")
        return

    df_py = pd.read_csv(py_results_file)
    
    # Seleccionar top 10 para el reporte
    df_top = df_py.sort_values(by='return_pct', ascending=False).head(10).copy()
    
    # Renombrar columnas para claridad
    df_top.rename(columns={
        'return_pct': 'Python Return %',
        'win_rate': 'Python WinRate %',
        'trades': 'Python Trades'
    }, inplace=True)

    # 2. Preparar columnas para TradingView (se llenarán después del análisis manual/auto)
    df_top['TV Return %'] = ""
    df_top['TV WinRate %'] = ""
    df_top['TV Drawdown %'] = ""
    df_top['TV Profit Factor'] = ""

    # 3. Guardar como Excel (CSV compatible con Excel)
    output_path = 'Resultados_Estrategia_Comparativa.csv'
    df_top.to_csv(output_path, index=False)
    print(f"Reporte creado en: {output_path}")

if __name__ == "__main__":
    create_comparison_report()
