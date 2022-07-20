from copy import deepcopy
from turtle import width
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import altair as alt
import mpld3
import datetime as date

st.markdown("""
# Exploring Related Same Day Brands

For this application, I will be exploring the Safegraph data that \
includes where people go before or after they visit a Chipotle. \
The data we have includes both monthly and daily data.

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
    day = pd.read_csv('data/day.csv')
    return day.sort_values(by='date_range_start')

@st.cache
def month_df():
    month = pd.read_csv('data/month.csv')
    return month.sort_values(by='date_range_start')

@st.cache
def state_df():
    state = gpd.read_file('data/state/cb_2021_us_state_20m.shp')
    return state

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

We can view states individually to see their Chipotle locations. The first map shows \
a heat map of Chipotle's with the most visits. The second allows you to zoom in \
and get a better sense of where these locations are. Unfortunately, the data for the \
same day or month visits is not available.
""")

def plot_state(state, plot_df):
    # day = day_df()
    d = plot_df
    states = state_df()
    fig, ax = plt.subplots()
    plt.title(f"Chipotle Stores in {state}")

    d[d['region'] == state.lower()].plot(x='longitude', \
        y='latitude', kind='scatter', c='raw_visitor_counts', colormap='turbo', ax=ax)
    
    states[states["STUSPS"] == state].boundary.plot(ax=ax)

    fig_html = mpld3.fig_to_html(fig)
    return fig_html


state = st.selectbox(
    "Select a state",
    (state_df().STUSPS.sort_values().to_list()), key=1
)

components.html(plot_state(state, df), height=500)
st.map(df[df['region'] == state.lower()])

## table to with filter for most visits
st.write("""
After selecting a state, we can see which places that people visit before or after Chipotle \
the same day or month. This data is grouped by city and place to see how many visits that store \
received.
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

The holidays is a special time where people visits family and visit stores. \
Chipotle is also a popular place for people to visit around different holidays. \
This charts show the top 10 most popular places for people to visit after they visit Chipotle.
""")

holiday = st.selectbox(
    'Select a holiday',
    ('New Years', "St. Patrick's Day", "Valentine's Day", "Father's Day", \
        "Halloween", "Fourth of July", "Easter", "Mother's Day", "Thanksgiving", "Christmas")
)

holiday_dict = {'New Years':(1,1), "St. Patrick's Day":(3, 1), "Valentine's Day":(2,1), "Father's Day":(6,1), \
        "Halloween":(10,1), "Fourth of July":(7,1), "Easter":(4,1), "Mother's Day":(5,1),\
         "Thanksgiving":(11,1), "Christmas":(12,1)}

year = st.selectbox(
    'Select a year',
    (2019, 2020, 2021), key=1
)

@st.cache
def new_df():
    df = deepcopy(month_df())
    df['date_range_start'] = pd.to_datetime(df['date_range_start'], utc=True)
    df['month'] = df.date_range_start.dt.month.astype('string')
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
    # print(holiday, year)
    chart = alt.Chart(ten).encode(
        x=alt.X('brand_month', sort='-y', title='Brand Visits'),
        y=alt.Y('brand_month_visits', title='Brand')
    ).mark_bar().properties(title=f'Top Ten Brands around {holiday}', width=500)
    return chart

st.write(holiday_plot())

st.markdown("""
Walmart and McDonald's seems to always be a high contender. I can imagine families are not too different \
from my own. On Thanksgiving day, my cousins and I would leave the house where we just \
ate a huge meal. We would go to McDonald's to get a drink (maybe more food) and then go to Walmart \
to see the chaotic events of Black Friday.

# Comparing Brands over the Year

We can now compare brand in states over the year. You can select a state and then \
select a year to see the differences over the year. This will create a line chart \
based on the top visited brands per month.
""")

year = st.selectbox(
    'Select a year',
    (2019, 2020, 2021), key=2
)

state = st.selectbox(
    "Select a state",
    (state_df().STUSPS.sort_values().to_list()), key=2
)

def draw_chart(year, state):
    month = new_df()
    start_date = str(pd.to_datetime(f'{year}-01-01').date())
    end_date = str(pd.to_datetime(f'{year}-12-31').date())
    
    year_df = month.query(f"date_range_start >= '{start_date}' \
        & date_range_start <= '{end_date}' & region == '{state.lower()}'")

    year_df = year_df.groupby(['brand_month', 'date_range_start', 'month'], as_index=False).sum()

    yg = (year_df.sort_values(['date_range_start', 'month', 'brand_month_visits', 'brand_month'], 
                    ascending=[True, True, False, False])
    .groupby(['date_range_start', 'month'], 
                as_index = False, 
                sort = False)
    .nth([0,1,2,3,4,5])
    )

    chart = alt.Chart(yg).encode(
        x='date_range_start:T',
        y='brand_month_visits',
        color='brand_month'
    ).mark_line().properties(width=700, height=500)

    return chart

st.write(draw_chart(year, state))