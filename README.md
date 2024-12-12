
# Project

For ByBit exchange written in Python:
* Tick data downloader (CSV)
* Format converter (from CSV to Parquet)
* Tick data joiner and preprocessor
* Tick data aggregator into OHLCV data

# CLI

## Create virtual environment (Windows or *nix system)

```sh
python -m venv .venv
# or
python3 -m venv .venv
```

## Activate environment (Windows system & terminal)

```sh
.venv\Scripts\activate.bat
```

## Activate environment (Windows system / *nix terminal)

Create environment (Windows system) and Activate environment (*nix terminal):
```sh
source .venv/Scripts/activate
```

## Activate environment (*nix system & terminal)

```sh
source .venv/bin/activate
```

## Install requirements (PROD and DEV)

```sh
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -r requirements-dev.txt
```

## Jupyter Virtual Environment

Check environment:
```sh
which python
which pip

# if pip is missing or not in VENV

python -m ensurepip --upgrade
python -m pip install --upgrade pip
```

Install dependency if not installed:
```sh
pip install ipykernel
```

Install kernel into Jupyter:
```sh
python -m ipykernel install --user --name=exch_venv --display-name "Python (.exch_venv)"

jupyter kernelspec uninstall exch_venv
jupyter kernelspec list
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

## ByBit preprocessed tick to OHLCV file aggregator

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
python bybit/aggregate_tick_to_ohlcv.py -s BTCUSDT ETHUSDT -i DATA/2-PREPROCESSED -o DATA/3-OHLCV
python bybit/aggregate_tick_to_ohlcv.py -s BTCUSDT ETHUSDT --input_directory_path DATA/2-PREPROCESSED --output_directory_path DATA/3-OHLCV
```

## ByBit raw tick to OHLCV database aggregator

```sh
python bybit/aggregate_raw_tick_to_ohlcv_into_database.py -s BTCUSDT ETHUSDT
python bybit/aggregate_raw_tick_to_ohlcv_into_database.py --symbols BTCUSDT ETHUSDT
```

```sh
python bybit/aggregate_raw_tick_to_ohlcv_into_database.py -s BTCUSDT ETHUSDT -t 1s 5s 10s 15s 20s 30s 1m 5m 10m 15m 20m 30m 1h 2h 3h 4h 6h 8h 12h 1d
python bybit/aggregate_raw_tick_to_ohlcv_into_database.py -s BTCUSDT ETHUSDT --timeframes 1s 5s 10s 15s 20s 30s 1m 5m 10m 15m 20m 30m 1h 2h 3h 4h 6h 8h 12h 1d
```

```sh
python bybit/aggregate_raw_tick_to_ohlcv_into_database.py -s BTCUSDT ETHUSDT -f csv
python bybit/aggregate_raw_tick_to_ohlcv_into_database.py -s BTCUSDT ETHUSDT -f parquet
python bybit/aggregate_raw_tick_to_ohlcv_into_database.py -s BTCUSDT ETHUSDT --formats csv
python bybit/aggregate_raw_tick_to_ohlcv_into_database.py -s BTCUSDT ETHUSDT --formats parquet
```

```sh
python bybit/aggregate_raw_tick_to_ohlcv_into_database.py -s BTCUSDT ETHUSDT -b 2022-12-12 -e 2022-12-21
python bybit/aggregate_raw_tick_to_ohlcv_into_database.py -s BTCUSDT ETHUSDT --interval_begin 2022-12-12 --interval_end 2024-12-21
```

```sh
python bybit/aggregate_raw_tick_to_ohlcv_into_database.py -s BTCUSDT ETHUSDT -i DATA/1-RAW_TICK -o DATA/3-OHLCV_DATABASE
python bybit/aggregate_raw_tick_to_ohlcv_into_database.py -s BTCUSDT ETHUSDT --input_directory_path DATA/1-RAW_TICK --output_directory_path DATA/3-OHLCV_DATABASE
```
