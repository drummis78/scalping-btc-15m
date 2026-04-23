import sys
import os
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.collectors.context_collector import ContextCollector
from src.logic.ai_agent import AIAgent

def simulate_autonomous_decision(symbol):
    print(f"\n--- SIMULACIÓN DE DECISIÓN AUTÓNOMA: {symbol} ---")
    
    # 1. Simular Señal Técnica (Detectada por nuestro optimizador de 15m)
    print("[TEC] Señal Detectada: Breakout Alcista Donchian(20) detectado.")
    print("[TEC] Volumen: 1.8x (Confirmado).")
    
    # 2. Recolectar Contexto (Noticias)
    print("[CTX] Consultando NewsAPI y Ballenas...")
    collector = ContextCollector()
    news_text = collector.fetch_news(symbol)
    
    # 3. Consultar al Cerebro (Groq Llama 3)
    print("[AI] Analizando sentimiento con Llama 3 en Groq...")
    agent = AIAgent()
    sentiment_score = agent.analyze_sentiment(news_text)
    
    print(f"\n[AI] Resultado del Análisis: {sentiment_score}")
    
    # 4. Decisión Final
    if sentiment_score > 0.2:
        print(f"✅ VERDICTO: CONFLUENCIA POSITIVA. Enviando orden LONG a Binance.")
    elif sentiment_score < -0.2:
        print(f"❌ VERDICTO: FILTRADO POR IA. Sentimiento bajista detectado en noticias. No se opera.")
    else:
        print(f"⚠️ VERDICTO: SENTIMIENTO NEUTRAL. Esperando mejores condiciones.")

if __name__ == "__main__":
    # Probamos con AVAX que fue uno de nuestros top
    simulate_autonomous_decision('AVAX/USDT')
