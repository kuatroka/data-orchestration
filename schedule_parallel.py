import schedule
import time
import sys
sys.stderr = open('/dev/null', 'w')
import concurrent.futures
from stock_download_utils_nonasync import fetch_data_av, fetch_yahoo_prices, fetch_data_fmp, tickers


def run_parallel(funcs, args):
    start = time.perf_counter()
    print(f"\nALL THREADED DATA Download started at: {time.ctime()}\n")
    """Run each function in funcs with its corresponding argument in parallel"""
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(funcs)) as executor:
        futures = [executor.submit(func, arg) for func, arg in zip(funcs, args)]
    concurrent.futures.wait(futures)

    print(f"\nALL THREADED DATA Download finished at: {time.ctime()}\n")
    end = time.perf_counter()
    print(f"\nALL THREADED Download took: {(end-start)/60 :0.2f} min\n")
    return schedule.CancelJob

# schedule.every(10).seconds.do(run_parallel, [func1, func2, func3], ["John", "Hello", 42])
# schedule.every(1000).seconds.do(run_parallel, [
#     fetch_data_av, 
#     fetch_yahoo_prices,
#     fetch_data_fmp
#     ], [tickers, tickers, tickers])

schedule.every().wednesday.at("21:59").do(run_parallel, [
    fetch_data_av, 
    fetch_yahoo_prices,
    fetch_data_fmp
    ], [tickers, tickers, tickers])

if __name__ == "__main__":
    while 1:
        schedule.run_all()
        # schedule.run_pending()
        time.sleep(1)