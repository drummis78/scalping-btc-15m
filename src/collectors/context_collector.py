import requests
import pandas as pd
import os
import sys
from datetime import datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import NEWS_API_KEY

class ContextCollector:
    def __init__(self):
        self.api_key = NEWS_API_KEY
        self.base_url = "https://newsapi.org/v2/everything"

    def fetch_news(self, symbol, days=1):
        """
        Descarga noticias de News API sobre cripto y macro.
        """
        if not self.api_key or self.api_key == "pon_tu_clave_aqui":
            return "No API Key provided."

        query = f"{symbol.split('/')[0]} OR crypto OR FED OR inflation"
        from_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        params = {
            'q': query,
            'from': from_date,
            'sortBy': 'publishedAt',
            'apiKey': self.api_key,
            'language': 'en'
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            data = response.json()
            articles = data.get('articles', [])
            
            combined_text = ""
            for art in articles[:10]: # Tomar los 10 más recientes
                combined_text += f"- {art['title']}: {art['description']}\n"
            
            return combined_text
        except Exception as e:
            print(f"Error en NewsAPI: {e}")
            return ""

if __name__ == "__main__":
    collector = ContextCollector()
    news = collector.fetch_news('BTC/USDT')
    print(news)
