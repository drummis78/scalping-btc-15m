import requests
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import GROQ_API_KEY

class AIAgent:
    def __init__(self):
        self.api_key = GROQ_API_KEY
        self.url = "https://api.groq.com/openai/v1/chat/completions"

    def analyze_sentiment(self, news_text):
        if not self.api_key or self.api_key == "pon_tu_clave_aqui":
            return 0

        prompt = f"""
        Analiza el impacto de estas noticias en el mercado cripto (15-60 min).
        Responde SOLO con un número entre -1 y 1.
        NOTICIAS: {news_text[:2000]}
        """

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": "llama-3.3-70b-versatile", # Modelo actual de Groq
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0
        }

        try:
            response = requests.post(self.url, headers=headers, json=payload, timeout=10)
            result = response.json()
            
            if 'choices' in result:
                score_str = result['choices'][0]['message']['content'].strip()
                # Extraer solo el número si la IA se pone habladora
                import re
                match = re.search(r"[-+]?\d*\.?\d+", score_str)
                return float(match.group()) if match else 0.0
            else:
                print(f"❌ Error de API Groq: {result.get('error', {}).get('message', 'Desconocido')}")
                return 0
        except Exception as e:
            print(f"❌ Error de conexión AIAgent: {e}")
            return 0

if __name__ == "__main__":
    agent = AIAgent()
    print(f"Prueba IA: {agent.analyze_sentiment('Bitcoin reached 100k today!')}")
