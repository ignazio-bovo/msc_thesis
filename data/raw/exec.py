import pandas as pd
from requests import get
import time

if __name__ == '__main__':
    while(True):
        df = pd.DataFrame(get(f'https://www.bitmex.com/api/v1/quote?symbol=ETH&count=1000&reverse=true').json())
        df.to_csv('bitmex_level1_ob.txt',mode='a',index=False)
        time.sleep(1)
    

