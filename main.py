from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect, HTTPException, Path
from fastapi.middleware.cors import CORSMiddleware
from sqlmodel import select, Session
import ccxt
import pandas as pd
import numpy as np
import asyncio
from datetime import datetime, timedelta
from typing import List, Optional
import json
from sqlmodel import SQLModel, Field, create_engine
from contextlib import asynccontextmanager

# Datenbankmodell
class Watchlist(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: int
    symbol: str
    created_at: datetime = Field(default_factory=datetime.utcnow)

# Datenbank einrichten
DATABASE_URL = "sqlite:///./trading.db"
engine = create_engine(DATABASE_URL, echo=True)

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

# Lebenszyklus-Ereignisse
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Starten
    create_db_and_tables()
    yield
    # Beenden
    # Hier könnten wir Ressourcen aufräumen

app = FastAPI(title="TradingView Clone API", lifespan=lifespan)

# CORS für Frontend-Kommunikation erlauben
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# WebSocket-Verbindungsmanager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

manager = ConnectionManager()

# Marktdaten von einer Börse abrufen
def get_market_data(symbol="BTC/USDT", timeframe="1d", limit=100):
    exchange = ccxt.binance()
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Fehler beim Abrufen der Marktdaten: {e}")
    
    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df['date'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    return df

# Technische Indikatoren berechnen
def calculate_indicators(df):
    # Einfache gleitende Durchschnitte
    df['sma_20'] = df['close'].rolling(window=20).mean()
    df['sma_50'] = df['close'].rolling(window=50).mean()
    
    # RSI
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))
    
    # MACD
    exp12 = df['close'].ewm(span=12, adjust=False).mean()
    exp26 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd'] = exp12 - exp26
    df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
    df['macd_hist'] = df['macd'] - df['macd_signal']
    
    # Bollinger Bands
    df['bb_middle'] = df['close'].rolling(window=20).mean()
    bb_std = df['close'].rolling(window=20).std()
    df['bb_upper'] = df['bb_middle'] + (bb_std * 2)
    df['bb_lower'] = df['bb_middle'] - (bb_std * 2)
    
    # Stochastischer Oszillator
    high_14 = df['high'].rolling(window=14).max()
    low_14 = df['low'].rolling(window=14).min()
    df['stoch_k'] = 100 * ((df['close'] - low_14) / (high_14 - low_14))
    df['stoch_d'] = df['stoch_k'].rolling(window=3).mean()
    
    return df

@app.get("/")
def read_root():
    return {"message": "TradingView Clone Backend läuft!"}

@app.get("/api/market/{symbol}")
def get_market(
    symbol: str,
    timeframe: str = Query("1d", description="Zeitrahmen, z.B. 1d, 1h, 5m"),
    limit: int = Query(100, ge=10, le=500, description="Anzahl der Kerzen")
):
    try:
        data = get_market_data(symbol, timeframe, limit)
        return {"success": True, "data": data.to_dict('records')}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/indicators/{symbol}")
def get_technical_indicators(
    symbol: str,
    timeframe: str = Query("1d"),
    limit: int = Query(200, ge=20, le=500)
):
    try:
        data = get_market_data(symbol, timeframe, limit)
        df = calculate_indicators(data)
        return {"success": True, "data": df.dropna().to_dict('records')}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/candlestick/{symbol}")
def get_candlestick_data(
    symbol: str,
    timeframe: str = Query("1d"),
    limit: int = Query(100, ge=10, le=500)
):
    try:
        data = get_market_data(symbol, timeframe, limit)
        return {"success": True, "data": data.to_dict('records')}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.websocket("/ws/{symbol}")
async def websocket_endpoint(websocket: WebSocket, symbol: str):
    await manager.connect(websocket)
    try:
        exchange = ccxt.binance()
        while True:
            try:
                # Aktuelle Marktdaten abrufen
                ticker = exchange.fetch_ticker(symbol)
                price_data = {
                    "symbol": symbol,
                    "price": ticker['last'],
                    "timestamp": datetime.now().isoformat(),
                    "volume": ticker['baseVolume'],
                    "change": ticker['percentage']
                }
                
                # Daten an den Client senden
                await websocket.send_json(price_data)
                
                # 5 Sekunden warten
                await asyncio.sleep(5)
            except ccxt.NetworkError:
                print("Network error, retrying in 10 seconds...")
                await asyncio.sleep(10)
            except Exception as e:
                print(f"Error in WebSocket loop: {e}")
                break
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        print(f"WebSocket error: {e}")
        manager.disconnect(websocket)

# Watchlist-Endpunkte
@app.get("/api/watchlist/{user_id}")
def get_watchlist(user_id: int):
    with Session(engine) as session:
        statement = select(Watchlist).where(Watchlist.user_id == user_id)
        watchlist = session.exec(statement).all()
        return {"success": True, "data": watchlist}

@app.post("/api/watchlist/{user_id}")
def add_to_watchlist(user_id: int, symbol: str = Query(...)):
    with Session(engine) as session:
        # Prüfen, ob das Symbol bereits in der Watchlist ist
        statement = select(Watchlist).where(
            Watchlist.user_id == user_id, 
            Watchlist.symbol == symbol
        )
        existing = session.exec(statement).first()
        
        if existing:
            return {"success": False, "error": "Symbol bereits in der Watchlist"}
        
        watchlist_item = Watchlist(user_id=user_id, symbol=symbol)
        session.add(watchlist_item)
        session.commit()
        return {"success": True, "data": watchlist_item}

@app.delete("/api/watchlist/{user_id}/{symbol}")
def remove_from_watchlist(user_id: int, symbol: str):
    with Session(engine) as session:
        statement = select(Watchlist).where(
            Watchlist.user_id == user_id, 
            Watchlist.symbol == symbol
        )
        watchlist_item = session.exec(statement).first()
        
        if not watchlist_item:
            return {"success": False, "error": "Symbol nicht in der Watchlist"}
        
        session.delete(watchlist_item)
        session.commit()
        return {"success": True, "message": "Symbol aus Watchlist entfernt"}

# Symbol-Suche
@app.get("/api/search/{query}")
def search_symbols(query: str):
    try:
        exchange = ccxt.binance()
        markets = exchange.load_markets()
        
        # Filtere Märkte basierend auf der Suchanfrage
        results = []
        for symbol, market in markets.items():
            if query.upper() in symbol and market['active']:
                results.append({
                    'symbol': symbol,
                    'base': market['base'],
                    'quote': market['quote'],
                    'active': market['active']
                })
                
                # Begrenze die Anzahl der Ergebnisse
                if len(results) >= 20:
                    break
                    
        return {"success": True, "data": results}
    except Exception as e:
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)