from copy import deepcopy
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import mpld3
import datetime as date

st.markdown("""
# Exploring Related Same Day Brands

For this application, I will be exploring the Safegraph data that \
includes where people go before or after they visit a Chipotle. \
The data we have includes both monthly and daily data.
""")

st.markdown("""
I downloaded data from databricks after selecting what I needed.

```python
df.select('*', F.explode('related_same_day_brand'))\
.withColumnRenamed('key', 'brand_day')\
.withColumnRenamed('value', 'brand_day_visits')\
.drop('related_same_day_brand', 'related_same_month_brand')

df.select('*', F.explode('related_same_month_brand'))\
.withColumnRenamed('key', 'brand_month')\
.withColumnRenamed('value', 'brand_month_visits')\
.drop('related_same_month_brand', 'related_same_day_brand', 'visits_by_day')
```
# View the Data
""")

@st.cache
def day_df():
    df = pd.read_csv('data/day.csv')
    return df.sort_values(by='date_range_start')

@st.cache
def month_df():
    df = pd.read_csv('data/month.csv')
    return df.sort_values(by='date_range_start')

@st.cache
def state_df():
    df = gpd.read_file('data/state/cb_2021_us_state_20m.shp')
    return df

## displaying tables
option = st.selectbox(
     'Select to explore monthly or daily data',
     ('month', 'day'), key=1)

if option == 'day':
    st.dataframe(day_df(), height=250)
    df = day_df()
    brand = 'brand_day'
    visits = 'brand_day_visits'
elif option == 'month':
    st.dataframe(month_df(), height=250)
    brand = 'brand_month'
    visits = 'brand_month_visits'
    df = month_df()

## displaying state map
st.write("""

## States with most visits

We can view states individually to see their Chipotle locations. The first map is shows \
a heat map of Chipotle's with the most visits. The second allows you to zoom in an see the street.
""")

def plot_state(state, df):
    # day = day_df()
    d = df
    states = state_df()
    fig, ax = plt.subplots()
    d[d['region'] == state.lower()].plot(x='longitude', \
        y='latitude', kind='scatter', c='raw_visitor_counts', colormap='turbo', ax=ax)
    
    states[states["STUSPS"] == state].boundary.plot(ax=ax)
    
    fig_html = mpld3.fig_to_html(fig)
    return fig_html


state = st.selectbox(
    "Select a state",
    (state_df().STUSPS.sort_values().to_list())
)

components.html(plot_state(state, df), height=500)
st.map(df[df['region'] == state.lower()])

## table to with filter for most visits
st.write("""
After selecting a state, we can see which places that people visit the same day or month. \

We can now look at the amount of visits to other places.
""")

option = st.selectbox(
     'Select monthly or daily data',
     ('month', 'day'), key=2)

if option == 'day':
    df = day_df()
    grouped = df.groupby(['city', 'brand_day', 'region'], as_index=False).sum()
    brand = 'brand_day'
    visits = 'brand_day_visits'
elif option == 'month':
    df = month_df()
    grouped = df.groupby(['city', 'brand_month', 'region'], as_index=False).sum()
    brand = 'brand_month'
    visits = 'brand_month_visits'
    
# min, max = st.slider(
#     'Select a range of values', 
#     int(df[visits].min()), int(df[visits].max()), (int(df[visits].min()), int(df[visits].max()))
# )

@st.cache
def get_subset(state, visits):
    subset = grouped.query(f"region == '{state.lower()}'")
    return subset[visits]

@st.cache
def select_values(min, max, state, visits):
    subset = grouped.query(f"{visits} > {min} & {visits} < {max} & region == '{state.lower()}'")
    return subset[['city', brand, visits]].sort_values(by=visits, ascending=False)

try:
    min, max = st.slider(
        'Select a range of values',
        int(get_subset(state, visits).min()), int(get_subset(state, visits).max()), (int(get_subset(state, visits).min()), int(get_subset(state, visits).max()))
    )
    st.write(select_values(min, max, state, visits), height=250)
except:
    st.write("### Error, the state is empty")


## pick holidays and view places around those holidays
st.write("""
# What places do people visit after Chipotle around Holidays?

We will now look at a chart of places around holidays with the most visits nation wide.
""")

holiday = st.selectbox(
    'Select a holiday',
    ('New Years', "St. Patrick's Day", "Valentine's Day", "Father's Day", \
        "Halloween", "Fourth of July", "Easter", "Mother's Day", "Thanksgiving", "Christmas")
)

holiday_dict = {'New Years':(1,1), "St. Patrick's Day":(3, 1), "Valentine's Day":(2,1), "Father's Day":(6,1), \
        "Halloween":(10,1), "Fourth of July":(7,1), "Easter":(4,1), "Mother's Day":(5,1), "Thanksgiving":(11,1), "Christmas":(12,1)}

st.write("""
We also have 3 years of data so pick a year to gain more insight about.
""")

year = st.selectbox(
    'Select a year',
    (2019, 2020, 2021)
)

@st.cache
def new_df():
    df = deepcopy(month_df())
    df['date_range_start'] = pd.to_datetime(df['date_range_start'], utc=True)
    df['date_range_start'] = df.date_range_start.dt.date.astype('string')
    return df

@st.cache
def holiday_df(holiday, year):
    date_string = f"""{year}-{holiday_dict[holiday][0]}-{holiday_dict[holiday][1]}"""
    date = str(pd.to_datetime(date_string).date())

    df = new_df()
    subset = df.query(f'date_range_start == "{date}"')
    grouped = subset.groupby(['brand_month'], as_index=False).sum()
    ten = grouped.sort_values(by='brand_month_visits', ascending=False).head(10)
    return ten

def holiday_plot():
    ten = holiday_df(holiday, year)
    fig, ax = plt.subplots(figsize=(15,8))
    plt.bar(ten['brand_month'], ten['brand_month_visits'])
    plt.title(f"Top Ten Brands around {holiday} in {year}", fontsize=20)
    ax.set_ylabel("Brand Visits", fontsize=15)

    return fig

st.write(holiday_plot())