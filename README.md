
# CLI

## Virtual Environment

Create environment:
```sh
python -m venv .wenv
```

Activate environment (Linux):
```sh
source .wenv/bin/activate
```

Activate environment (Windows):
```sh
source .wenv/Scripts/activate
# or
.wenv\Scripts\activate.bat
```

Install requirements
```sh
python -m pip install -r requirements.txt
```

## ByBit data downloader

[bybit](https://public.bybit.com/trading)

```sh
python downloader_bybit.py -t BTCUSDT ETHUSDT
python downloader_bybit.py --tickers BTCUSDT ETHUSDT
```

```sh
python downloader_bybit.py -t BTCUSDT ETHUSDT -b
python downloader_bybit.py -t BTCUSDT ETHUSDT --backfill
```

```sh
python downloader_bybit.py -t BTCUSDT ETHUSDT -o DATA/1-DOWNLOADS
python downloader_bybit.py -t BTCUSDT ETHUSDT --output_directory_path DATA/1-DOWNLOADS
```
