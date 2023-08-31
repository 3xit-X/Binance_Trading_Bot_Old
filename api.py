import requests
import time
import hashlib
import hmac
from binance.exceptions import BinanceAPIException
from data_connection import API_Key_test, Secret_Key_test

def get_current_multi_asset_mode():
    # Parametri della richiesta

    endpoint = '/fapi/v1/multiAssetsMargin'
    api_key = API_Key_test
    api_secret = Secret_Key_test

    # Creazione del timestamp UNIX in millisecondi
    timestamp = int(time.time() * 1000)

    # Creazione della stringa da firmare
    query_string = f'timestamp={timestamp}'
    signature = hmac.new(api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

    # Aggiunta della firma al payload
    query_string += f'&signature={signature}'

    # Creazione dell'header di autenticazione
    headers = {
        'X-MBX-APIKEY': api_key
    }

    # Esecuzione della richiesta GET
    response = requests.get(f'https://fapi.binance.com{endpoint}?{query_string}', headers=headers)

    # Visualizzazione della risposta
    return response.json()

def create_test_order(symbol, quantity, pip_size, side):
    try:
        # Parametri della richiesta
        endpoint = '/api/v3/order/test'
        api_key = API_Key_test
        api_secret = Secret_Key_test
        base_url = 'https://api.binance.com'
        timestamp = int(time.time() * 1000)

        # Creazione del payload
        payload = {
            'symbol': symbol,
            'side': 'BUY',
            'type': 'MARKET',
            'quantity': quantity,
            'timestamp': timestamp,
            'recvWindow': 5000  # Finestra temporale per ricevere la risposta in millisecondi
        }

        # Creazione della stringa da firmare
        query_string = '&'.join([f'{k}={v}' for k, v in payload.items()])
        signature = hmac.new(api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

        # Aggiunta della firma al payload
        payload['signature'] = signature

        # Creazione dell'header di autenticazione
        headers = {
            'X-MBX-APIKEY': api_key
        }

        # Esecuzione della richiesta POST
        response = requests.post(f'{base_url}{endpoint}?{query_string}&signature={signature}', headers=headers)

        # Controllo della risposta
        if response.status_code == 200:
            print('Test order created successfully.')
            return response

    except BinanceAPIException as e:
        print(e)









