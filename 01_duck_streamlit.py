import streamlit as st

with st.echo():
    from time import perf_counter as timer

    import duckdb
    import pyarrow as pa
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq
    import pandas as pd
    
    import pydeck as pdk  # for visualization


home = "Duck DB NYC Taxi Analysis"
projection_pushdown = "Line Items Projection Pushdown"
filter_pushdown = "Line Items Filter Pushdown"
stream = "NYC Taxi Stream"
view = st.sidebar.radio("Page View", [home, projection_pushdown, filter_pushdown, stream])

if view == home:
    st.header(home)
    """\
See duckbd / Apache Arrow [blogpost](https://duckdb.org/2021/12/03/duck-arrow.html) for source and discussion.

Downloading the full dataset requires ~40 GB storage space

Final example requires ~250 GB of memory / swap...

Choose "NYC Taxi Stream" on the left to see DuckDB + Arrow speed through 10 years of data in ~3 seconds in Streamlit (laptop benchmark. duckdb benchmark ~0.05 seconds)
"""
elif view == projection_pushdown:
    st.header(projection_pushdown)
    "In this example we run a simple aggregation on two columns of our lineitem table."
    with st.echo():
        # DuckDB
        start_time = timer()
        lineitem = pq.read_table("lineitemsf1.snappy.parquet")
        con = duckdb.connect()

        # Transforms Query Result from DuckDB to Arrow Table
        result = con.execute(
            """SELECT sum(l_extendedprice * l_discount) AS revenue
                        FROM
                        lineitem;"""
        ).fetch_arrow_table()
        end_time = timer()
    duckdb_runtime = end_time - start_time
    st.write(f"Finished in {duckdb_runtime} seconds")
    st.write(result)
    with st.echo():
        start_time = timer()
        # Pandas
        arrow_table = pq.read_table("lineitemsf1.snappy.parquet")
        # Converts an Arrow table to a Dataframe
        df = arrow_table.to_pandas()

        # Runs aggregation
        res = pd.DataFrame({"sum": [(df.l_extendedprice * df.l_discount).sum()]})

        # Creates an Arrow Table from a Dataframe
        new_table = pa.Table.from_pandas(res)
        end_time = timer()
    pandas_runtime = end_time - start_time
    st.write(f"Finished in {pandas_runtime} seconds")
    st.write(new_table)

    st.metric("Duck DB Runtime", duckdb_runtime, pandas_runtime - duckdb_runtime)
elif view == filter_pushdown:
    st.header(filter_pushdown)
    "For our filter pushdown we repeat the same aggregation used in the previous section, but add filters on 4 more columns."
    with st.echo():
        # DuckDB
        start_time = timer()
        lineitem = pq.read_table("lineitemsf1.snappy.parquet")

        # Get database connection
        con = duckdb.connect()

        # Transforms Query Result from DuckDB to Arrow Table
        result = con.execute(
            """SELECT sum(l_extendedprice * l_discount) AS revenue
                FROM
                    lineitem
                WHERE
                    l_shipdate >= CAST('1994-01-01' AS date)
                    AND l_shipdate < CAST('1995-01-01' AS date)
                    AND l_discount BETWEEN 0.05
                    AND 0.07
                    AND l_quantity < 24; """
        ).fetch_arrow_table()
        end_time = timer()
    duckdb_runtime = end_time - start_time
    st.write(f"Finished in {duckdb_runtime} seconds")
    st.write(result)
    with st.echo():
        # Pandas
        start_time = timer()
        arrow_table = pq.read_table("lineitemsf1.snappy.parquet")

        df = arrow_table.to_pandas()
        filtered_df = df[
            (df.l_shipdate >= "1994-01-01")
            & (df.l_shipdate < "1995-01-01")
            & (df.l_discount >= 0.05)
            & (df.l_discount <= 0.07)
            & (df.l_quantity < 24)
        ]

        res = pd.DataFrame(
            {"sum": [(filtered_df.l_extendedprice * filtered_df.l_discount).sum()]}
        )
        new_table = pa.Table.from_pandas(res)
        end_time = timer()
    pandas_runtime = end_time - start_time
    st.write(f"Finished in {pandas_runtime} seconds")
    st.write(new_table)
    st.metric("Duck DB Runtime", duckdb_runtime, pandas_runtime - duckdb_runtime)
elif view == stream:
    st.header(stream)
    "As demonstrated before, DuckDB is capable of consuming and producing Arrow data in a streaming fashion. In this section we run a simple benchmark, to showcase the benefits in speed and memory usage when comparing it to full materialization and Pandas. This example uses the full NYC taxi dataset which you can download"
    with st.echo():
        # DuckDB
        # Open dataset using year,month folder partition
        start_time = timer()
        nyc = ds.dataset("nyc-taxi/", partitioning=["year", "month"])

        # Get database connection
        con = duckdb.connect()

        # Run query that selects part of the data
        query = con.execute(
            "SELECT total_amount, passenger_count,year,pickup_at, pickup_longitude as lon, pickup_latitude as lat FROM nyc where total_amount > 100 and year > 2014 and lat is not null and lon is not null"
        )

        # Create Record Batch Reader from Query Result.
        # "fetch_record_batch()" also accepts an extra parameter related to the desired produced chunk size.
        record_batch_reader = query.fetch_record_batch()

        # Retrieve all batch chunks
        all_chunks = []
        while True:
            try:
                # Process a single chunk here
                # pyarrow.lib.RecordBatch
                chunk = record_batch_reader.read_next_batch()
                all_chunks.append(chunk)
            except StopIteration:
                break
        end_time = timer()
        data = pa.Table.from_batches(all_chunks)
    duckdb_runtime = end_time - start_time
    st.write(f"Finished in {duckdb_runtime} seconds")
    st.write(data)

    st.write("The below pandas example uses ~250 GB of memory. Run it if you've got it!")
    st.code("""\
        # Pandas
        # We must exclude one of the columns of the NYC dataset due to an unimplemented cast in Arrow.
        start_time = timer()
        working_columns = [
            "vendor_id",
            "pickup_at",
            "dropoff_at",
            "passenger_count",
            "trip_distance",
            "pickup_longitude",
            "pickup_latitude",
            "store_and_fwd_flag",
            "dropoff_longitude",
            "dropoff_latitude",
            "payment_type",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "total_amount",
            "year",
            "month",
        ]

        # Open dataset using year,month folder partition
        nyc_dataset = ds.dataset("nyc-taxi/", partitioning=["year", "month"])

        # Generate a scanner to skip problematic column
        dataset_scanner = nyc_dataset.scanner(columns=working_columns)

        # Materialize dataset to an Arrow Table
        nyc_table = dataset_scanner.to_table()

        # Generate Dataframe from Arow Table
        nyc_df = nyc_table.to_pandas()

        # Apply Filter
        filtered_df = nyc_df[(nyc_df.total_amount > 100) & (nyc_df.year > 2014)]

        # Apply Projection
        res = filtered_df[["total_amount", "passenger_count", "year"]]

        # Transform Result back to an Arrow Table
        new_table = pa.Table.from_pandas(res)
        end_time = timer()
        """)
    # pandas_runtime = end_time - start_time
    # st.write(f"Finished in {pandas_runtime} seconds")
    # st.write(new_table)

    st.metric("Duck DB Runtime", duckdb_runtime)
    df = data.to_pandas()
    st.write(df)
    st.write(len(df))

    st.write(
        pdk.Deck(
            map_style="mapbox://styles/mapbox/light-v9",
            initial_view_state={
                "latitude": 40.7,
                "longitude": -73.9,
                "zoom": 12,
                "pitch": 50,
            },
            layers=[
                pdk.Layer(
                    "HexagonLayer",
                    data=df[['lon', 'lat']],
                    get_position=["lon", "lat"],
                    radius=100,
                    elevation_scale=4,
                    elevation_range=[0, 1000],
                    pickable=True,
                    extruded=True,
                ),
            ],
        )
    )