[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://share.streamlit.io/gerardrbentley/uber-nyc-pickups-duckdb/main/streamlit_app_duck.py)

# Streamlit + DuckDB Demo: Uber / Taxi Pickups in New York City

- `streamlit_app_duck.py`: Inspired / Copied from [streamlit demo repo](https://github.com/streamlit/demo-uber-nyc-pickups)
  - Analyzes a month of NYC Uber Pickup location data. The original is from the Streamlit [demo gallery](https://streamlit.io/gallery)
  - A [Streamlit](https://streamlit.io) demo converted to utilize [DuckDB](https://duckdb.org/docs/api/python) to run data analysis faster and on more data than raw pandas.
- `01_duck_streamlit.py`: Inspired / Copied from [duckdb](https://duckdb.org/2021/12/03/duck-arrow.html) and [arrow](https://arrow.apache.org/blog/2021/12/03/arrow-duckdb/) blog post
  - Analyze 10 years (1.5 Billion rows / 40 GB) of NYC Taxi Pickup location data and demo some other filter optimizations over Pandas
  - A blog post on the power of DuckDB + Arrow converted to an interacive demo in Streamlit

Read more in the accompanying [blog post](https://tech.gerardbentley.com/python/data/intermediate/2022/04/26/holy-duck.html) ✍🏻

## One Month Uber Dataset

Check out the speed up on loading data.
From left to right:

- `5.087 s`: [streamlit example (100,000 rows)](https://github.com/streamlit/demo-uber-nyc-pickups/blob/e714e117abe0a22fe159ce7b29980c566289b6d1/streamlit_app.py#L32)
- `54.306 s`: streamlit example (Full Dataset using `pd.read_csv`)
- `1.178 s`: this example (Full Dataset using `pyarrow` + `duckdb`)

![load data speedup compare](load_data_compare.png)

(*Note:* Profiled with [pyinstrument](https://pyinstrument.readthedocs.io/en/latest/how-it-works.html), see more on the caveats / how it works in [this post](http://joerick.me/posts/2017/12/15/pyinstrument-20/))

### Analysis

I wrote the `load_data` function above to match what the original code does, which is **load** the data into a `Dataframe`, not just load the schema.
After it's loaded then `pandas` and `numpy` are used for some additional filtering and computation.

The real point of `duckdb` is to do your filtering and computation **before** loading all of your data into memory.

For parity with the streamlit demo `load_data` originally would be:

```py
    data = duckdb.arrow(data)
    return data.arrow().to_pandas()
```

Just returning the duckdb instance will drop the time `load_data` takes to `~0.1 s`!
Then you have an in memory analysis object ready to go.

```py
    data = duckdb.arrow(data)
    return data
```

### Run this demo locally

```sh
git clone git@github.com:gerardrbentley/uber-nyc-pickups-duckdb.git duckdb-streamlit
cd duckdb-streamlit
python -m venv venv
. ./venv/bin/activate
python -m pip install -r requirements.txt

streamlit run streamlit_app_duck.py
```

## 10 Years of data

*NOTE:* The following will download 40 GB of data to your machine.
Not available on streamlit cloud due to storage limitations.

Going deeper into the DuckDB / Arrow power, we can filter and analyze even larger datasets.

We can select `304,851` interesting rows from all `1,547,741,381` in the 10 year dataset in < 3 seconds on a laptop!

The following will download necessary files and then run the app

```sh
# Setup
python -m pip install boto3
# Download datasets
wget https://github.com/cwida/duckdb-data/releases/download/v1.0/lineitemsf1.snappy.parquet
python 00_download_nyc_data.py
# Run the demo
streamlit run 01_duck_streamlit.py
```
