import os
import json
import datetime
import requests
import pandas as pd
import yfinance as yf
import gspread
from google.oauth2.service_account import Credentials

# --- 1. CONFIGURAZIONI ---
FMP_API_KEY = os.getenv("FMP_API_KEY") 
GCP_CREDENTIALS_JSON = os.getenv("GCP_CREDENTIALS")
SHEET_NAME = "NASDAQ 100 Quant Dashboard"
TAB_NAME = "Dashboard"
CURRENT_YEAR = datetime.date.today().year

def get_nasdaq_stats():
    """Analisi Quantitativa su YTD e Stagionalità tramite ETF QQQ."""
    print("Scaricando dati NASDAQ (QQQ)...")
    
    # Usiamo yfinance in modo pulito, senza sessioni custom. L'anti-bot è integrato.
    qqq = yf.Ticker("QQQ")
    df = qqq.history(start=f"{CURRENT_YEAR}-01-01")
    
    if df.empty:
        return [["Errore", "Dati QQQ non disponibili", "N/A"]] * 5

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

    # 4. Sell in May (Finestra Stagionale)
    current_month = datetime.date.today().month
    if 5 <= current_month <= 10:
        sim_status = "🔴 Attivo (Maggio-Ottobre)"
        sim_signal = "Rischio Volatilità / Correzione"
    else:
        sim_status = "🟢 Inattivo (Novembre-Aprile)"
        sim_signal = "Stagionalità Favorevole"
    sim_detail = "Finestra debole attiva da Mag a Ott"

    # 5. Ciclo Elettorale Presidenziale
    cycle_year = (CURRENT_YEAR - 2024) % 4
    if cycle_year == 2:
        pres_cycle_status = "🟡 Anno 2 (Midterm)"
        pres_cycle_signal = "Volatilità estiva attesa"
        pres_cycle_detail = "Minimi tipici nel Q3"
    elif cycle_year == 3:
        pres_cycle_status = "🟢 Anno 3 (Pre-Election)"
        pres_cycle_signal = "Fortemente Rialzista"
        pres_cycle_detail = "L'anno migliore del ciclo"
    else:
        pres_cycle_status = f"⚪ Anno {cycle_year or 4}"
        pres_cycle_signal = "Neutro"
        pres_cycle_detail = "Nessun estremo statistico"

    return [
        [jan_status, jan_signal, jan_detail],
        [f5_status, f5_signal, f5_detail],
        [d100_status, d100_signal, d100_detail],
        [sim_status, sim_signal, sim_detail],
        [pres_cycle_status, pres_cycle_signal, pres_cycle_detail]
    ]

def get_yield_curve():
    """Verifica Inversione Curva dei Rendimenti (10Y vs 3M)."""
    print("Calcolando Curva dei Rendimenti...")
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
    return ["Errore", "Dati Tassi non disponibili", "N/A"]

def get_zbt():
    """Calcola uno ZBT Sintetico esatto sui 100 titoli del NASDAQ 100 tramite Yahoo Finance"""
    print("Calcolando ZBT Sintetico (NASDAQ 100)...")
    import io # <-- IL FIX È QUI: Importiamo la libreria per gestire i testi come file
    
    try:
        url = 'https://en.wikipedia.org/wiki/Nasdaq-100'
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        
        risposta_wiki = requests.get(url, headers=headers)
        risposta_wiki.raise_for_status() 
        
        # IL SECONDO FIX È QUI: Wrappiamo il testo in io.StringIO()
        tables = pd.read_html(io.StringIO(risposta_wiki.text))
        tickers = tables[4]['Ticker'].tolist()
        
        # Sostituiamo eventuali ticker che Yahoo legge diversamente
        tickers = [t.replace('.', '-') for t in tickers]
        
        # Scarichiamo le ultime 25 sedute
        data = yf.download(tickers, period="25d", progress=False)['Close']
        
        # Calcoliamo le variazioni
        returns = data.pct_change()
        
        # Contiamo azioni in rialzo e ribasso
        advancing = (returns > 0).sum(axis=1)
        declining = (returns < 0).sum(axis=1)
        
        # Calcoliamo lo ZBT
        ad_ratio = advancing / (advancing + declining)
        zbt_ema = ad_ratio.ewm(span=10, adjust=False).mean()
        
        # Rimuoviamo i valori nulli e prendiamo gli ultimi giorni
        zbt_ema = zbt_ema.dropna()
        ultimo_valore = zbt_ema.iloc[-1]
        valore_precedente_10gg = zbt_ema.iloc[-10]
        
        status = "⚫ Nessun Segnale"
        signal = "Neutro"
        if valore_precedente_10gg < 0.40 and ultimo_valore > 0.615:
            status = "🟢 INNESCATO"
            signal = "Forte Rialzo Long-Term"
            
        detail = f"ZBT Nasdaq-100: {ultimo_valore:.3f}"
        return [status, signal, detail]
        
    except Exception as e:
        return ["Errore", "Calcolo sintetico fallito", str(e)[:20]]
        
def update_google_sheets(dashboard_data):
    """Inietta la matrice dati nel blocco B3:D9 di Google Sheets."""
    print("Connessione a Google Sheets in corso...")
    if not GCP_CREDENTIALS_JSON:
        raise ValueError("Credenziali GCP mancanti nei Secret di GitHub.")

    creds_dict = json.loads(GCP_CREDENTIALS_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(credentials)
    
    sheet = gc.open(SHEET_NAME).worksheet(TAB_NAME)
    sheet.update(values=dashboard_data, range_name='B3:D9')
    print("✅ Dashboard aggiornata con successo!")

if __name__ == "__main__":
    try:
        nasdaq_data = get_nasdaq_stats()
        yield_data = get_yield_curve()
        zbt_data = get_zbt()
        
        final_data = nasdaq_data + [yield_data] + [zbt_data]
        update_google_sheets(final_data)
        
    except Exception as e:
        print(f"❌ Errore critico durante l'esecuzione: {e}")
