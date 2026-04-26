import os
import json
import datetime
import requests
import pandas as pd
import yfinance as yf
import gspread
from google.oauth2.service_account import Credentials

# --- CONFIGURAZIONI INIZIALI ---
FMP_API_KEY = os.getenv("FMP_API_KEY") 
GCP_CREDENTIALS_JSON = os.getenv("GCP_CREDENTIALS")
SHEET_NAME = "NomeDelTuoFileGoogleSheet" # Sostituisci con il nome esatto del tuo file
TAB_NAME = "Dashboard"
CURRENT_YEAR = datetime.date.today().year

def get_nasdaq_stats():
    """Scarica i dati storici YTD del NASDAQ (tramite ETF QQQ) e calcola le statistiche."""
    print("Scaricando dati NASDAQ (QQQ)...")
    qqq = yf.Ticker("QQQ")
    df = qqq.history(start=f"{CURRENT_YEAR}-01-01")
    
    if df.empty:
        raise ValueError("Impossibile scaricare i dati da Yahoo Finance.")

    # Variabili Base
    total_days = len(df)
    first_close = df['Close'].iloc[0]
    last_close = df['Close'].iloc[-1]
    ytd_return = ((last_close / first_close) - 1) * 100

    # 1. January Barometer
    jan_data = df.loc[f"{CURRENT_YEAR}-01-01":f"{CURRENT_YEAR}-01-31"]
    if not jan_data.empty:
        jan_return = ((jan_data['Close'].iloc[-1] / jan_data['Close'].iloc[0]) - 1) * 100
        jan_status = "🟢 Valido" if jan_return > 0 else "🔴 Invalido"
        jan_signal = "Rialzista" if jan_return > 0 else "Neutro"
        jan_detail = f"Rendimento Gennaio: {jan_return:.2f}%"
    else:
        jan_status, jan_signal, jan_detail = ("N/A", "N/A", "N/A")

    # 2. First 5 Days
    if total_days >= 5:
        f5_return = ((df['Close'].iloc[4] / first_close) - 1) * 100
        f5_status = "🟢 Valido" if f5_return > 0 else "🔴 Invalido"
        f5_signal = "Rialzista" if f5_return > 0 else "Neutro"
        f5_detail = f"Rendimento Primi 5 gg: {f5_return:.2f}%"
    else:
        f5_status, f5_signal, f5_detail = ("Attesa", "Attesa", "Dati Insufficienti")

    # 3. First 100 Days
    if total_days >= 100:
        d100_return = ((df['Close'].iloc[99] / first_close) - 1) * 100
        d100_status = "🟢 Completo"
        d100_signal = "Forte Rialzo" if d100_return > 5 else "Neutro"
        d100_detail = f"Rendimento 100 gg: {d100_return:.2f}%"
    else:
        d100_status = f"🟡 In corso (Giorno {total_days})"
        d100_signal = "Forte YTD" if ytd_return > 0 else "Debole YTD"
        d100_detail = f"YTD Attuale: {ytd_return:.2f}%"

    # 4. Sell in May
    current_month = datetime.date.today().month
    if 5 <= current_month <= 10:
        sim_status = "🔴 Attivo (Maggio-Ottobre)"
        sim_signal = "Rischio Volatilità / Correzione"
    else:
        sim_status = "🟢 Inattivo (Novembre-Aprile)"
        sim_signal = "Stagionalità Favorevole"
    sim_detail = "Finestra attiva da Mag a Ott"

    # 5. Ciclo Presidenziale (2026 = Anno 2, Midterm)
    pres_cycle_status = "🟡 Anno 2 (Midterm)"
    pres_cycle_signal = "Volatilità estiva attesa"
    pres_cycle_detail = "Minimi tipici nel Q3"

    return [
        [jan_status, jan_signal, jan_detail],
        [f5_status, f5_signal, f5_detail],
        [d100_status, d100_signal, d100_detail],
        [sim_status, sim_signal, sim_detail],
        [pres_cycle_status, pres_cycle_signal, pres_cycle_detail]
    ]

def get_yield_curve():
    """Calcola l'inversione della curva dei rendimenti (10 Anni vs 3 Mesi)."""
    print("Calcolo Curva Rendimenti...")
    # Usiamo il 10y (^TNX) e il 3m (^IRX) perché il 2y a volte manca su YF
    tnx = yf.Ticker("^TNX").history(period="5d")
    irx = yf.Ticker("^IRX").history(period="5d")
    
    if not tnx.empty and not irx.empty:
        yield_10y = tnx['Close'].iloc[-1]
        yield_3m = irx['Close'].iloc[-1]
        spread = yield_10y - yield_3m
        
        status = "🔴 Invertita" if spread < 0 else "🟢 Normale"
        signal = "Rischio Recessione" if spread < 0 else "Espansione"
        detail = f"Spread 10Y-3M: {spread:.2f}%"
        return [status, signal, detail]
    return ["Errore", "Dati non disponibili", "N/A"]

def get_zbt():
    """Calcola lo Zweig Breadth Thrust usando l'API di FMP."""
    print("Calcolo ZBT da FMP...")
    if not FMP_API_KEY:
        return ["Errore", "API Key mancante", "Verifica GitHub Secrets"]

    url = f"https://financialmodelingprep.com/api/v4/historical-market-breadth?limit=30&apikey={FMP_API_KEY}"
    response = requests.get(url)
    
    if response.status_code != 200:
        return ["Errore API", response.status_code, "N/A"]
        
    dati = response.json()
    df = pd.DataFrame(dati)
    df = df.sort_values(by='date').reset_index(drop=True)
    
    df['A_D_Ratio'] = df['advancing'] / (df['advancing'] + df['declining'])
    df['ZBT_EMA'] = df['A_D_Ratio'].ewm(span=10, adjust=False).mean()
    
    ultimo_valore = df['ZBT_EMA'].iloc[-1]
    valore_precedente_10gg = df['ZBT_EMA'].iloc[-10]
    
    status = "⚫ Nessun Segnale"
    signal = "Neutro"
    if valore_precedente_10gg < 0.40 and ultimo_valore > 0.615:
        status = "🟢 INNESCATO"
        signal = "Forte Rialzo Long-Term"
    
    detail = f"ZBT EMA Attuale: {ultimo_valore:.3f}"
    return [status, signal, detail]

def update_google_sheets(dashboard_data):
    """Si connette a Google Sheets e inietta la matrice di dati."""
    print("Autenticazione su Google Sheets...")
    if not GCP_CREDENTIALS_JSON:
        raise ValueError("Credenziali GCP mancanti nei Secret.")

    creds_dict = json.loads(GCP_CREDENTIALS_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(credentials)
    
    # Apri il foglio
    sheet = gc.open(SHEET_NAME).worksheet(TAB_NAME)
    
    # Aggiorna il blocco B3:D9 in una singola chiamata API per evitare limiti
    # La sintassi range_name è aggiornata per gspread 6.0.0+
    sheet.update(values=dashboard_data, range_name='B3:D9')
    print("Dashboard aggiornata con successo!")

if __name__ == "__main__":
    try:
        # Raccogli tutti i dati
        nasdaq_data = get_nasdaq_stats()
        yield_data = get_yield_curve()
        zbt_data = get_zbt()
        
        # Assembla la matrice (Lista di liste corrispondenti a B3:D9)
        final_data = nasdaq_data + [yield_data] + [zbt_data]
        
        # Invia a Google Sheets
        update_google_sheets(final_data)
        
    except Exception as e:
        print(f"Errore critico durante l'esecuzione: {e}")
