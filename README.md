
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
python -m pip install -r bybit/requirements.downloader.txt
python -m pip install -r bybit/requirements.converter.txt
```

## ByBit data downloader

[bybit](https://public.bybit.com/trading)

```sh
python bybit/download_tick_data.py -t BTCUSDT ETHUSDT
python bybit/download_tick_data.py --tickers BTCUSDT ETHUSDT
```

```sh
python bybit/download_tick_data.py -t BTCUSDT ETHUSDT -b
python bybit/download_tick_data.py -t BTCUSDT ETHUSDT --backfill
```

```sh
python bybit/download_tick_data.py -t BTCUSDT ETHUSDT -o DATA/1-DOWNLOADS
python bybit/download_tick_data.py -t BTCUSDT ETHUSDT --output_directory_path DATA/1-DOWNLOADS
```


## ByBit tick data converter (from CSV to Parquet)

```sh
python bybit/convert_tick_data_csv2parquet.py -t BTCUSDT ETHUSDT
python bybit/convert_tick_data_csv2parquet.py --tickers BTCUSDT ETHUSDT
```

```sh
python bybit/convert_tick_data_csv2parquet.py -t BTCUSDT ETHUSDT -i DATA/1-DOWNLOADS -o DATA/2-CONVERTED
python bybit/convert_tick_data_csv2parquet.py -t BTCUSDT ETHUSDT --input_directory_path DATA/1-DOWNLOADS --output_directory_path DATA/2-CONVERTED
```

## ByBit tick data preprocessor

```sh
python bybit/join_and_format_tick_data.py -t BTCUSDT ETHUSDT -f csv
python bybit/join_and_format_tick_data.py -t BTCUSDT ETHUSDT -f parquet
python bybit/join_and_format_tick_data.py -t BTCUSDT ETHUSDT --filters csv
python bybit/join_and_format_tick_data.py -t BTCUSDT ETHUSDT --filters parquet
```

```sh
python bybit/join_and_format_tick_data.py -t BTCUSDT ETHUSDT -e csv
python bybit/join_and_format_tick_data.py -t BTCUSDT ETHUSDT -e parquet
python bybit/join_and_format_tick_data.py -t BTCUSDT ETHUSDT -e csv parquet
python bybit/join_and_format_tick_data.py -t BTCUSDT ETHUSDT --exports csv
python bybit/join_and_format_tick_data.py -t BTCUSDT ETHUSDT --exports parquet
python bybit/join_and_format_tick_data.py -t BTCUSDT ETHUSDT --exports csv parquet
```

```sh
python bybit/join_and_format_tick_data.py -t BTCUSDT ETHUSDT -i DATA/1-RAW_TICK -o DATA/2-PREPROCESSED
python bybit/join_and_format_tick_data.py -t BTCUSDT ETHUSDT --input_directory_path DATA/1-RAW_TICK --output_directory_path DATA/2-PREPROCESSED
```
