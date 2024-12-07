
# Project

Tick data downloader, format converter (CSV and Parquet), tick data joiner and preprocessor, tick data aggregator into OHLCV data for ByBit exchange written in Python.

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
python -m pip install --upgrade pip
python -m pip install -r bybit/requirements.txt
```

## ByBit data downloader

[bybit](https://public.bybit.com/trading)

```sh
python bybit/download_tick_data.py -s BTCUSDT ETHUSDT
python bybit/download_tick_data.py --symbols BTCUSDT ETHUSDT
```

```sh
python bybit/download_tick_data.py -s BTCUSDT ETHUSDT -b
python bybit/download_tick_data.py -s BTCUSDT ETHUSDT --backfill
```

```sh
python bybit/download_tick_data.py -s BTCUSDT ETHUSDT -o DATA/1-DOWNLOADS
python bybit/download_tick_data.py -s BTCUSDT ETHUSDT --output_directory_path DATA/1-DOWNLOADS
```


## ByBit tick data converter (from CSV to Parquet)

```sh
python bybit/convert_tick_data_csv2parquet.py -s BTCUSDT ETHUSDT
python bybit/convert_tick_data_csv2parquet.py --symbols BTCUSDT ETHUSDT
```

```sh
python bybit/convert_tick_data_csv2parquet.py -s BTCUSDT ETHUSDT -i DATA/1-DOWNLOADS -o DATA/2-CONVERTED
python bybit/convert_tick_data_csv2parquet.py -s BTCUSDT ETHUSDT --input_directory_path DATA/1-DOWNLOADS --output_directory_path DATA/2-CONVERTED
```

## ByBit tick data preprocessor

```sh
python bybit/join_and_format_tick_data.py -s BTCUSDT ETHUSDT
python bybit/join_and_format_tick_data.py --symbols BTCUSDT ETHUSDT
```

```sh
python bybit/join_and_format_tick_data.py -s BTCUSDT ETHUSDT -f csv
python bybit/join_and_format_tick_data.py -s BTCUSDT ETHUSDT -f parquet
python bybit/join_and_format_tick_data.py -s BTCUSDT ETHUSDT --formats csv
python bybit/join_and_format_tick_data.py -s BTCUSDT ETHUSDT --formats parquet
```

```sh
python bybit/join_and_format_tick_data.py -s BTCUSDT ETHUSDT -e csv
python bybit/join_and_format_tick_data.py -s BTCUSDT ETHUSDT -e parquet
python bybit/join_and_format_tick_data.py -s BTCUSDT ETHUSDT -e csv parquet
python bybit/join_and_format_tick_data.py -s BTCUSDT ETHUSDT --exports csv
python bybit/join_and_format_tick_data.py -s BTCUSDT ETHUSDT --exports parquet
python bybit/join_and_format_tick_data.py -s BTCUSDT ETHUSDT --exports csv parquet
```

```sh
python bybit/join_and_format_tick_data.py -s BTCUSDT ETHUSDT -i DATA/1-RAW_TICK -o DATA/2-PREPROCESSED
python bybit/join_and_format_tick_data.py -s BTCUSDT ETHUSDT --input_directory_path DATA/1-RAW_TICK --output_directory_path DATA/2-PREPROCESSED
```

## ByBit tick to OHLCV data aggregator

```sh
python bybit/aggregate_tick_to_ohlcv.py -s BTCUSDT ETHUSDT
python bybit/aggregate_tick_to_ohlcv.py --symbols BTCUSDT ETHUSDT
```

```sh
python bybit/aggregate_tick_to_ohlcv.py -s BTCUSDT ETHUSDT -t 1s 5s 10s 15s 20s 30s 1m 5m 10m 15m 20m 30m 1h 2h 3h 4h 6h 8h 12h 1d 1w
python bybit/aggregate_tick_to_ohlcv.py -s BTCUSDT ETHUSDT --timeframes 1s 5s 10s 15s 20s 30s 1m 5m 10m 15m 20m 30m 1h 2h 3h 4h 6h 8h 12h 1d 1w
```

```sh
python bybit/aggregate_tick_to_ohlcv.py -s BTCUSDT ETHUSDT -f csv
python bybit/aggregate_tick_to_ohlcv.py -s BTCUSDT ETHUSDT -f parquet
python bybit/aggregate_tick_to_ohlcv.py -s BTCUSDT ETHUSDT --formats csv
python bybit/aggregate_tick_to_ohlcv.py -s BTCUSDT ETHUSDT --formats parquet
```

```sh
python bybit/aggregate_tick_to_ohlcv.py -s BTCUSDT ETHUSDT -e csv
python bybit/aggregate_tick_to_ohlcv.py -s BTCUSDT ETHUSDT -e parquet
python bybit/aggregate_tick_to_ohlcv.py -s BTCUSDT ETHUSDT -e csv parquet
python bybit/aggregate_tick_to_ohlcv.py -s BTCUSDT ETHUSDT --exports csv
python bybit/aggregate_tick_to_ohlcv.py -s BTCUSDT ETHUSDT --exports parquet
python bybit/aggregate_tick_to_ohlcv.py -s BTCUSDT ETHUSDT --exports csv parquet
```

```sh
python bybit/aggregate_tick_to_ohlcv.py -s BTCUSDT ETHUSDT -i DATA/1-PREPROCESSED -o DATA/2-OHLCV
python bybit/aggregate_tick_to_ohlcv.py -s BTCUSDT ETHUSDT --input_directory_path DATA/1-PREPROCESSED --output_directory_path DATA/2-OHLCV
```
