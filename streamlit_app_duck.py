# -*- coding: utf-8 -*-
# Copyright 2018-2022 Streamlit Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Modified by Gerard Bentley

"""An example of showing geographic data."""

from datetime import datetime
from pathlib import Path
import streamlit as st
import pandas as pd
import duckdb
from pyarrow import csv
import numpy as np
import altair as alt
import pydeck as pdk
# from pyinstrument import Profiler

# SETTING PAGE CONFIG TO WIDE MODE AND ADDING A TITLE AND FAVICON
st.set_page_config(layout="wide", page_title="NYC Ridesharing Demo", page_icon=":taxi:")

# profiler = Profiler(interval=0.0001)
# profiler.start()

# LOAD DUCKDB ONCE
@st.cache_resource
def load_data():
    data = csv.read_csv(
        "uber-raw-data-sep14.csv.gz",
        convert_options=csv.ConvertOptions(
            include_columns=["Date/Time", "Lat", "Lon"],
            timestamp_parsers=["%m/%d/%Y %H:%M:%S"],
        ),
    ).rename_columns(["date/time", "lat", "lon"])

    return duckdb.from_arrow(data)


# FUNCTION FOR AIRPORT MAPS
def map(data, lat, lon, zoom):
    st.write(
        pdk.Deck(
            map_style="mapbox://styles/mapbox/light-v9",
            initial_view_state={
                "latitude": lat,
                "longitude": lon,
                "zoom": zoom,
                "pitch": 50,
            },
            layers=[
                pdk.Layer(
                    "HexagonLayer",
                    data=data,
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


# FILTER DATA FOR A SPECIFIC HOUR, CACHE
@st.cache_data
def filterdata(hour_selected):
    data = load_data()
    return data.filter(f'hour("date/time") = {hour_selected}').to_df()


# CALCULATE MIDPOINT FOR GIVEN SET OF DATA
@st.cache_data
def mpoint():
    data = load_data()
    return tuple(data.query("data", "SELECT AVG(lat), AVG(lon) FROM data").fetchone())


# FILTER DATA BY HOUR
@st.cache_data
def histdata(hr):
    data = load_data()
    hist_query = f'SELECT histogram(minute("date/time")) FROM hist WHERE hour("date/time") >= {hr} and hour("date/time") < {hr + 1}'
    (results,) = data.query("hist", hist_query).fetchone()
    df = pd.DataFrame(results)
    df.columns = ["minute", "pickups"]
    return df


# STREAMLIT APP LAYOUT
# LAYING OUT THE TOP SECTION OF THE APP
row1_1, row1_2 = st.columns((2, 3))

# SEE IF THERE'S A QUERY PARAM IN THE URL (e.g. ?pickup_hour=2)
# THIS ALLOWS YOU TO PASS A STATEFUL URL TO SOMEONE WITH A SPECIFIC HOUR SELECTED,
# E.G. https://share.streamlit.io/streamlit/demo-uber-nyc-pickups/main?pickup_hour=2
if not st.session_state.get("url_synced", False):
    try:
        pickup_hour = int(st.experimental_get_query_params()["pickup_hour"][0])
        st.session_state["pickup_hour"] = pickup_hour
        st.session_state["url_synced"] = True
    except KeyError:
        pass


# IF THE SLIDER CHANGES, UPDATE THE QUERY PARAM
def update_query_params():
    hour_selected = st.session_state["pickup_hour"]
    st.experimental_set_query_params(pickup_hour=hour_selected)


with row1_1:
    st.title("NYC Uber Ridesharing Data")
    hour_selected = st.slider(
        "Select hour of pickup", 0, 23, key="pickup_hour", on_change=update_query_params
    )


with row1_2:
    st.write(
        """
    ##
    Examining how Uber pickups vary over time in New York City's and at its major regional airports.
    By sliding the slider on the left you can view different slices of time and explore different transportation trends.
    """
    )

# LAYING OUT THE MIDDLE SECTION OF THE APP WITH THE MAPS
row2_1, row2_2, row2_3, row2_4 = st.columns((2, 1, 1, 1))

# SETTING THE ZOOM LOCATIONS FOR THE AIRPORTS
la_guardia = [40.7900, -73.8700]
jfk = [40.6650, -73.7821]
newark = [40.7090, -74.1805]
zoom_level = 12
midpoint = mpoint()

with row2_1:
    st.write(
        f"""**All New York City from {hour_selected}:00 and {(hour_selected + 1) % 24}:00**"""
    )
    map(filterdata(hour_selected), midpoint[0], midpoint[1], 11)

with row2_2:
    st.write("**La Guardia Airport**")
    map(filterdata(hour_selected), la_guardia[0], la_guardia[1], zoom_level)

with row2_3:
    st.write("**JFK Airport**")
    map(filterdata(hour_selected), jfk[0], jfk[1], zoom_level)

with row2_4:
    st.write("**Newark Airport**")
    map(filterdata(hour_selected), newark[0], newark[1], zoom_level)

# CALCULATING DATA FOR THE HISTOGRAM
chart_data = histdata(hour_selected)
# LAYING OUT THE HISTOGRAM SECTION
st.write(
    f"""**Breakdown of rides per minute between {hour_selected}:00 and {(hour_selected + 1) % 24}:00**"""
)

st.altair_chart(
    alt.Chart(chart_data)
    .mark_area(
        interpolate="step-after",
    )
    .encode(
        x=alt.X("minute:Q", scale=alt.Scale(nice=False)),
        y=alt.Y("pickups:Q"),
        tooltip=["minute", "pickups"],
    )
    .configure_mark(opacity=0.2, color="red"),
    use_container_width=True,
)
# profiler.stop()

# profiler.print()
# (Path('profiles') / f"Duck_Load_{datetime.now().isoformat()}.html").write_text(profiler.output_html())
