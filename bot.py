import asyncio
from data_connection import API_Key_test, Secret_Key_test
from binance.exceptions import BinanceAPIException
from binance.client import Client, AsyncClient
from api import get_current_multi_asset_mode
from backtrader_functions import (create_dataframe, calculate_ema_x, calculate_stochastic_oscillator,
                                  extract_symbol_info, create_tables_for_symbols, save_last_candle_to_db,
                                  get_solde, get_last_candle_from_db,calculate_min_order_quantity_in_usdt, open_long, open_short,
                                  change_initial_leverage, change_multi_asset_mode, change_position_mode)


class TradingBot:
    def __init__(self, symbol):
        self.symbol = symbol
        self.order_id = None
        self.order_side = None

    async def start(self):
        client = AsyncClient(API_Key_test, Secret_Key_test, tld='com')
        while True:
            try:
                candles = await client.get_klines(symbol=self.symbol, interval=Client.KLINE_INTERVAL_1MINUTE)
                # candles_stoch = await client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_1DAY)
                pos_mode = client.futures_get_position_mode()
                if not pos_mode:
                    change_position_mode()
                mul_mode = get_current_multi_asset_mode()
                if not mul_mode:
                    change_multi_asset_mode()
                bankroll = get_solde()
                print(bankroll)
                df = create_dataframe(candles)
                df_last = df.tail(1)
                print(f"Last df:{df_last}")
                last_price = df["close"].iloc[-1]
                last_price = last_price.replace(',', '.')
                last_price = float(last_price)
                print(f"{symbol} last price: {last_price}")
                # df_stoch = create_dataframe(candles_stoch)
                last_ema_5 = calculate_ema_x(df, 5)
                last_ema_50 = calculate_ema_x(df, 50)
                last_ema_10 = calculate_ema_x(df, 10)
                print(f"{symbol}, EMA 5:", last_ema_5)
                print(f"{symbol}, EMA 10:", last_ema_10)
                print(f"{symbol}, EMA 50:", last_ema_50)
                # k, d = calculate_stochastic_oscillator(df_stoch, 5, 3)
                # last_k = k.iloc[-1]
                # last_d = d.iloc[-1]
                #print(f"{symbol}, last %K value:", k.iloc[-1])
                #print(f"{symbol}, last %D value:", d.iloc[-1])
                pip_size, min_qty, quantity_precision = extract_symbol_info(symbol)
                quantity = calculate_min_order_quantity_in_usdt(symbol, min_qty)
                quantity = round(quantity, quantity_precision)
                change_initial_leverage(symbol)
                if not self.order_id:
                    if last_ema_5 >= last_ema_50 and last_ema_10 >= last_ema_50:
                        # if last_k <= last_d:
                        # se le condizioni sono rispettate apro posizione long (buy)
                        self.order_id, self.order_side = open_long(symbol, quantity, pip_size, 8, 5)
                        print("Opened long: ", last_price)
                    elif last_ema_5 <= last_ema_50 and last_ema_10 <= last_ema_50:
                        # if last_k >= last_d:
                        # se le condizioni sono rispettate apro posizione short (sell)
                        self.order_id, self.order_side = open_short(symbol, quantity, pip_size, 8, 5)
                        print("Opened short: ", last_price)
                elif self.order_id and self.order_side == 'LONG':
                    if last_ema_5 <= last_ema_50 and last_ema_10 <= last_ema_50:
                        # funzione per invertire la posizione
                        print("Reversed from long to short")
                elif self.order_id and self.order_side == 'SHORT':
                    if last_ema_5 >= last_ema_50 and last_ema_10 >= last_ema_50:
                        # funzione per invertire da short a long
                        print("Reversed from short to long")
            except BinanceAPIException as e:
                print(e)

                await asyncio.sleep(10)

if __name__ == "__main__":
    symbol = 'BTCUSDT'
    # create_tables_for_symbols(symbols)
    bot = TradingBot(symbol)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(bot.start())

# , 'DOGEUSDT', 'SHIBUSDT', 'WINUSDT', 'TRXUSDT', 'OOKIUSDT', 'BATUSDT', 'CHZUSDT'
