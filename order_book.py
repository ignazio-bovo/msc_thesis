import itertools
import numpy as np
import time
import pandas as pd
import cbpro
import datetime as dt
import sys

public_client = cbpro.PublicClient()


def main(sleep,directory):

    with open(directory+'/orderbook.csv', 'w') as f:
        f.write(','.join(['timestamp','bid','bid_size','bid_count','ask','ask_size','ask_count']) + '\n' )
    with open(directory+'/trades.csv', 'w') as f:
        f.write(','.join(['timestamp','sell_avg_price','sell_qty','sell_count',\
                'buy_avg_price','buy_qty','buy_count']) + '\n' )

    date = dt.datetime.now(tz=dt.timezone(dt.timedelta(hours=1)))
    while True:
        r_book = public_client.get_product_order_book('BTC-USD')
        r_trades_gen = public_client.get_product_trades(product_id ='BTC-USD')
        top5 = itertools.islice(r_trades_gen, 10)

        book = r_book['bids'][0] + r_book['asks'][0]

        trades = pd.DataFrame(top5).astype({'trade_id': int, 'price' :np.float64, 'size': np.float64})
        trades.time = pd.to_datetime(trades.time)
        trades = trades.loc[(trades.time >= date)]
        sells = trades[trades.side == 'sell']
        buys = trades[trades.side == 'buy']
        trades_l = [sells['price'].mean(), sells['size'].sum(), sells.shape[0],\
            buys['price'].mean(), buys['size'].sum(), buys.shape[0]]

        date = dt.datetime.now(tz=dt.timezone(dt.timedelta(hours=1)))

        # trades = pd.concat([pd.DataFrame([x]) for x in r_trades_gen])
        # print(trades.head(), dt.datetime.now().isoformat())
        # with open('trades.csv', 'a') as f:
            # f.write(','.join(map(str,t)) + '\n')
        with open(directory+'/orderbook.csv', 'a') as f:
            f.write(date.isoformat()+','+','.join(map(str,book)) + '\n' )

        with open(directory+'/trades.csv', 'a') as f:
            f.write(date.isoformat()+','+','.join(map(str,trades_l)) + '\n' )

        time.sleep(sleep)

if __name__ == '__main__':
    main(int(sys.argv[1]), sys.argv[2])

