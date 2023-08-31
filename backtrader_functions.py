import pandas as pd
from data_connection import API_Key_test, Secret_Key_test
from binance.exceptions import *
import mysql.connector
from data_connection import Hostname, Username, Password, Database_Name
from binance.client import Client, AsyncClient
import time
import re

tframe = '1m'
if tframe[-1] == 'm':
    tf1 = int(re.findall(r'\d+', tframe)[0])
    tme_frame = 1 * tf1
if tframe[-1] == 'h':
    tf1 = int(re.findall(r'\d+', tframe)[0])
    tme_frame = 60 * tf1
timeout = time.time() + (50 * tme_frame)


def get_price(symbol):
    client = Client(API_Key_test, Secret_Key_test, tld='com')
    ticker = client.get_symbol_ticker(symbol=symbol)
    price = ticker['price']
    return price

def change_position_mode():
    # Modalità di copertura (Hedge Mode): In questa modalità, un utente può aprire posizioni sia long che short sullo
    # stesso simbolo contemporaneamente. 'False' solo o long o short
    timestamp = int(time.time() * 1000)  # Converti il timestamp in millisecondi
    client = Client(API_Key_test, Secret_Key_test, tld='com')
    client.futures_change_position_mode(dualSidePosition='true', timestamp=timestamp)

def change_multi_asset_mode():
    # Modalità multi-asset: In questa modalità, un utente può aprire posizioni su più asset contemporaneamente. 'False' solo uno
    timestamp = int(time.time() * 1000)  # Converti il timestamp in millisecondi
    client = Client(API_Key_test, Secret_Key_test, tld='com')
    client.futures_change_position_mode(multiAssetsMargin='true', timestamp=timestamp)

def change_initial_leverage(symbol):
    timestamp = int(time.time() * 1000)  # Converti il timestamp in millisecondi
    client = Client(API_Key_test, Secret_Key_test, tld='com')
    leverage_values = [125, 100, 75, 60, 50, 45, 30, 25, 20, 10]

    for leverage in leverage_values:
        try:
            change = client.futures_change_leverage(symbol=symbol, leverage=leverage, timestamp=timestamp)
            leverage = change["leverage"]
            print(f"Cambio leva a x {leverage} completato con successo.")
        except BinanceAPIException as e:
            print(f"Errore durante il cambio leva: {e}")
            # Prova con il valore di leva successivo
        else:
            break  # Esci dal ciclo solo se la richiesta ha successo
    else:
        print("La leva non può essere aggiornata con i valori testati")



def reverse_order():
    client = Client(API_Key_test, Secret_Key_test, tld='com')
    client.futures_change_position_margin()


def open_long(symbol, quantity, pip_size, pip_profit: float, pip_stop: float):
    try:
        client = Client(API_Key_test, Secret_Key_test, tld='com')
        buy_limit_order = client.futures_create_order(
            symbol=symbol, side='BUY', type='LIMIT', timeInForce='GTC', quantity=quantity
        )
        order_id = buy_limit_order['orderId']
        order_status = buy_limit_order['status']

        while order_status != 'FILLED':
            time.sleep(5)
            order_status = client.futures_get_order(symbol=symbol, orderId=order_id)['status']
            print(order_status)

            if order_status == 'FILLED':
                price = get_price(symbol)
                stop_loss_price = price - pip_size * pip_stop
                take_profit_price = price + pip_size * pip_profit
                time.sleep(1)
                set_stop_loss = client.futures_create_order(symbol=symbol, side='SELL', type='STOP_MARKET',
                                                            quantity=quantity, stopPrice=stop_loss_price, closePosition=True)
                time.sleep(1)
                set_take_profit = client.futures_create_order(symbol=symbol, side='SELL', type='TAKE_PROFIT_MARKET',
                                                              quantity=quantity, stopPrice=take_profit_price, closePosition=True)
                order_side = 'LONG'
                return order_id, order_side

            if time.time() > timeout:
                order_status = client.futures_get_order(symbol=symbol, orderId=order_id)['status']

                if order_status == 'PARTIALLY_FILLED':
                    cancel_order = client.futures_cancel_order(symbol=symbol, orderId=order_id)
                    time.sleep(1)

                    pos_size = client.futures_position_information()
                    df = pd.DataFrame(pos_size)
                    symbols = client.futures_position_information()
                    df = pd.DataFrame(symbols)
                    symbol_loc = df.index[df.symbol == symbol]
                    symbol_pos = (symbol_loc[-1])
                    pos_amount = abs(float(df.loc[symbol_pos, 'positionAmt']))

                    price = get_price(symbol)
                    stop_loss_price = price - pip_size * 3.0
                    take_profit_price = price + pip_size * 5.0

                    time.sleep(1)
                    set_stop_loss = client.futures_create_order(symbol=symbol, side='SELL', type='STOP_MARKET',
                                                                quantity=pos_amount, stopPrice=stop_loss_price, closePosition=True)
                    time.sleep(1)
                    set_take_profit = client.futures_create_order(symbol=symbol, side='SELL',
                                                                  type='TAKE_PROFIT_MARKET', quantity=pos_amount,
                                                                  stopPrice=take_profit_price, closePosition=True)
                    order_side = 'LONG'
                    return order_id, order_side

                else:
                    cancel_order = client.futures_cancel_order(symbol=symbol, orderId=order_id)
                    order_side = None
                    order_id = None
                    return order_id, order_side
    except BinanceAPIException as e:
        # error handling goes here
        print(e)
    except BinanceOrderException as e:
        # error handling goes here
        print(e)

    else:
        print("Buy long signal is on but you are already in position..")


def open_short(symbol, quantity, pip_size, pip_profit: float, pip_stop: float):
    try:
        client = Client(API_Key_test, Secret_Key_test, tld='com')
        buy_limit_order = client.futures_create_order(
            symbol=symbol, side='SELL', type='LIMIT', timeInForce='GTC', quantity=quantity, price=get_price(symbol)
        )
        order_id = buy_limit_order['orderId']
        order_status = buy_limit_order['status']

        while order_status != 'FILLED':
            time.sleep(10)
            order_status = client.futures_get_order(symbol=symbol, orderId=order_id)['status']
            print(order_status)

            if order_status == 'FILLED':
                price = get_price(symbol)
                stop_loss_price = price + pip_size * pip_stop
                take_profit_price = price - pip_size * pip_profit
                time.sleep(1)
                set_stop_loss = client.futures_create_order(symbol=symbol, side='BUY', type='STOP_MARKET',
                                                            quantity=quantity, stopPrice=stop_loss_price, closePosition=True)
                time.sleep(1)
                set_take_profit = client.futures_create_order(symbol=symbol, side='BUY', type='TAKE_PROFIT_MARKET',
                                                              quantity=quantity, stopPrice=take_profit_price, closePosition=True)
                order_side = 'SHORT'
                return order_id, order_side

            if time.time() > timeout:
                order_status = client.futures_get_order(symbol=symbol, orderId=order_id)['status']

                if order_status == 'PARTIALLY_FILLED':
                    cancel_order = client.futures_cancel_order(symbol=symbol, orderId=order_id)
                    time.sleep(1)

                    pos_size = client.futures_position_information()
                    df = pd.DataFrame(pos_size)
                    symbols = client.futures_position_information()
                    df = pd.DataFrame(symbols)
                    symbol_loc = df.index[df.symbol == symbol]
                    symbol_pos = (symbol_loc[-1])
                    pos_amount = abs(float(df.loc[symbol_pos, 'positionAmt']))

                    price = get_price(symbol)
                    stop_loss_price = price + pip_size * 3.0
                    take_profit_price = price - pip_size * 5.0

                    time.sleep(1)
                    set_stop_loss = client.futures_create_order(symbol=symbol, side='BUY', type='STOP_MARKET',
                                                                quantity=pos_amount, stopPrice=stop_loss_price, closePosition=True)
                    time.sleep(1)
                    set_take_profit = client.futures_create_order(symbol=symbol, side='BUY',
                                                                  type='TAKE_PROFIT_MARKET', quantity=pos_amount,
                                                                  stopPrice=take_profit_price, closePosition=True)
                    order_side = 'SHORT'
                    return order_id, order_side

                else:
                    cancel_order = client.futures_cancel_order(symbol=symbol, orderId=order_id)
                    order_side = None
                    order_id = None
                    return order_id, order_side

    except BinanceAPIException as e:
        # error handling goes here
        print(e)
    except BinanceOrderException as e:
        # error handling goes here
        print(e)

    else:
        print("Sell short signal is on but you are already in position..")


def get_min_order_quantity(symbol):
    client = Client(API_Key_test, Secret_Key_test, tld='com')
    exchange_info = client.futures_exchange_info()
    for symbol_info in exchange_info['symbols']:
        if symbol_info['symbol'] == symbol:
            filters = symbol_info['filters']
            for f in filters:
                if f['filterType'] == 'LOT_SIZE':
                    return float(f['minQty'])

    return None


def get_usdt_price(symbol):
    client = Client(API_Key_test, Secret_Key_test, tld='com')
    ticker = client.get_symbol_ticker(symbol=symbol)
    usdt_price = ticker['price']
    return usdt_price


def calculate_min_order_quantity_in_usdt(symbol, min_quantity):
    usdt_price = get_usdt_price(symbol)
    if min_quantity and usdt_price:
        min_order_quantity_in_usdt = min_quantity * float(usdt_price)
        return min_order_quantity_in_usdt

    return None


def extract_symbol_info(symbol):
    client = Client(api_key=API_Key_test, api_secret=Secret_Key_test)
    symbol_info = client.get_symbol_info(symbol)
    print(symbol_info)
    pip_size = None
    min_qty = None
    price_precision = get_precision(symbol)
    for filter in symbol_info['filters']:
        filter_type = filter['filterType']
        if filter_type == 'PRICE_FILTER':
            pip_size = float(filter['tickSize'])
        elif filter_type == 'LOT_SIZE':
            min_qty = float(filter['minQty'])

    return pip_size, min_qty, price_precision


def get_precision(symbol):
    client = Client(api_key=API_Key_test,
                    api_secret=Secret_Key_test,
                    tld='com'
                    )
    info = client.futures_exchange_info()
    for x in info['symbols']:
        if x['symbol'] == symbol:
            return x['quantityPrecision']

def get_solde():
    client = Client(API_Key_test, Secret_Key_test)

    # Ottenere le informazioni sull'account
    account_info = client.get_account()

    # Ottenere il saldo disponibile per ogni asset
    balances = account_info['balances']

    # Calcolare il saldo totale
    total_balance = 0.0

    for balance in balances:
        asset = balance['asset']
        free = float(balance['free'])
        locked = float(balance['locked'])
        total_balance += free + locked

    return total_balance


def get_last_candle_from_db(symbol):
    cnx = mysql.connector.connect(host=Hostname,
                                  user=Username,
                                  password=Password,
                                  database=Database_Name)
    cursor = cnx.cursor()

    table_name = f"{symbol.lower()}_minutes"
    query = f"SELECT * FROM {table_name} ORDER BY id DESC LIMIT 1 ;"
    cursor.execute(query)
    row = cursor.fetchone()
    cnx.commit()
    cursor.close()
    cnx.close()
    if row:
        return row[10], row[11], row[12]
    else:
        return False, False, False


def save_last_candle_to_db(symbol, df, last_ema_50, last_ema_10, last_ema_5, last_k, last_d, bankroll, open_position, type_position, price_position):
    cnx = mysql.connector.connect(host=Hostname,
                                  user=Username,
                                  password=Password,
                                  database=Database_Name)
    cursor = cnx.cursor()
    # last_candle_time = df['timestamp']
    # print(last_candle_time)
    last_candle_high = float(df['high'].values)
    #print(last_candle_high)
    last_candle_low = float(df['low'].values)
    #print(last_candle_low)
    last_candle_close = float(df['close'].values)
    #print(last_candle_close)
    last_candle_volume = float(df['volume'].values)
    #print(last_candle_volume)

    table_name = f"{symbol.lower()}_minutes"
    query = f"INSERT INTO {table_name} (high, low, close, volume, ema50, ema10, ema5, k, d, opened, type_position, price_position, bankroll) VALUES (" \
            f"'{last_candle_high}', '{last_candle_low}', '{last_candle_close}', '{last_candle_volume}', '{last_ema_50}'," \
            f" '{last_ema_10}', '{last_ema_5}', '{last_k}', '{last_d}', '{open_position}', '{type_position}', '{price_position}', '{bankroll}');"
    cursor.execute(query)

    cnx.commit()
    cursor.close()
    cnx.close()


def create_tables_for_symbols(symbols):
    cnx = mysql.connector.connect(host=Hostname,
                                  user=Username,
                                  password=Password,
                                  database=Database_Name)
    cursor = cnx.cursor()
    for symbol in symbols:
        table_name = f"{symbol.lower()}_minutes"
        query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ( \
            `id` INT NOT NULL AUTO_INCREMENT, \
            `created_at` timestamp NOT NULL DEFAULT current_timestamp(), \
            `high` DECIMAL(20,10) NOT NULL, \
            `low` DECIMAL(20,10) NOT NULL, \
            `close` DECIMAL(20,10) NOT NULL, \
            `volume` DECIMAL(20,10) NULL DEFAULT NULL, \
            `ema50` DECIMAL(20,10) NULL DEFAULT NULL, \
            `ema10` DECIMAL(20,10) NULL DEFAULT NULL, \
            `ema5` DECIMAL(20,10) NULL DEFAULT NULL, \
            `k` DECIMAL(20,10) NULL DEFAULT NULL, \
            `d` DECIMAL(20,10) NULL DEFAULT NULL, \
            `opened` BOOLEAN NULL DEFAULT NULL, \
            `type_position` VARCHAR(15) NULL DEFAULT NULL, \
            `price_position` VARCHAR(15) NULL DEFAULT NULL, \
            `bankroll` DECIMAL(20,10) NULL DEFAULT NULL, \
             PRIMARY KEY (`id`) \
             ) ENGINE = InnoDB AUTO_INCREMENT = 2 DEFAULT CHARSET = utf8mb4;"
        cursor.execute(query)
        cnx.commit()
    cursor.close()
    cnx.close()


def create_dataframe(candles):
    headers = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'timestamp_ms', 'quote_asset_volume',
               'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
    df = pd.DataFrame(data=candles, columns=headers)

    new_df = df.loc[:, ['timestamp', 'close', 'high', 'low', 'volume']]
    new_df['timestamp'] = pd.to_datetime(new_df['timestamp'], unit='ms')
    new_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    return new_df


def calculate_ema_x(df, periods):
    close_prices = df['close'].tolist()

    # converto la lista in un oggetto pandas Series
    close_prices_series = pd.Series(close_prices)
    #print(close_prices_series)

    # imposto il parametro alpha
    alpha = 2 / (periods + 1)

    # calcolo l'EMA
    ema = close_prices_series.ewm(alpha=alpha).mean()
    last_ema = ema.iloc[-1]
    return last_ema


def calculate_stochastic_oscillator(df, k_period, d_period):
    # Calcola la finestra mobile di massimi e minimi

    df['low'] = df['low'].astype(float)
    df['high'] = df['high'].astype(float)
    df['close'] = df['close'].astype(float)

    high = df['high'].rolling(k_period).max()
    low = df['low'].rolling(k_period).min()

    # Calcola %K
    k = ((df['close']) - low) / (high - low) * 100

    # Calcola %D
    d = k.rolling(d_period).mean()

    return k, d


def scalping_strategy(df, k, d):
    client = AsyncClient(API_Key_test, Secret_Key_test, tld='com')
    # leggi il valore dell'EMA degli ultimi 50 e 200 periodi
    ema_50 = calculate_ema_x(df, 50)
    ema_200 = calculate_ema_x(df, 200)

    # leggi l'ultimo valore di EMA 50 e EMA 200
    last_ema_50 = ema_50.iloc[-1]
    last_ema_200 = ema_200.iloc[-1]

    # leggi l'ultimo valore di k e d dell'oscillatore stocastico
    last_k = k.iloc[-1]
    last_d = d.iloc[-1]

    # verifica se il prezzo corrente supera l'EMA 50 e l'EMA 200
    if last_ema_50 >= last_ema_200:
        # verifica se k < d
        if last_k < last_d:
            # acquista se entrambe le condizioni sono soddisfatte
            print("Buy")
            order = client.order_market_buy(
                symbol='BNBBTC',
                quantity=100)
            open_position = True
            # imposta il take-profit e lo stop-loss
        else:
            print("Do nothing")
    else:
        # verifica se k > d
        if last_k >= last_d:
            # vende se entrambe le condizioni NON sono soddisfatte
            print("Sell")
            open_position = False
            # imposta il take-profit e lo stop-loss
        else:
            print("Do nothing")

def calculate_qty_on_euro(euros, last_crypto_price):
    quantity = euros/last_crypto_price
    return quantity

