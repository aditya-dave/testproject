import os
import zipfile
import plotly.graph_objects as go
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
import plotly.express as px
import chart_studio.plotly as py
import chart_studio
from pymongo import MongoClient
from flask import Flask, render_template, url_for

app = Flask(__name__)


#Downloading data

def get_data():
    os.environ['KAGGLE_USERNAME'] = "adipatel7828"
    os.environ['KAGGLE_KEY'] = "16f85841781e800a5759ed8d8f70858f"
    os.system("kaggle datasets download -d fireballbyedimyrnmom/us-counties-covid-19-dataset")
    zf = zipfile.ZipFile('us-counties-covid-19-dataset.zip')
    df = pd.read_csv(zf.open('us-counties.csv'))
    client = MongoClient("mongodb+srv://adi096:Qwertyuiop@cluster0-zuc0y.mongodb.net/test?retryWrites=true&w=majority")
    collection = client["testdb"]
    db = collection["testcollection"]

    db.drop()#Drop data before uploading again
    records_ = df.to_dict(orient="records")
    result = db.insert_many(records_)
    x = db.count_documents({})
    print("number of records", x)#Print the number of records to see how many are added
    headData = db.find()
    row_list = []
    for i in headData:
        row_list.append(i)

#Using apscheduler to refresh data every 24 hours

sched = BackgroundScheduler(daemon=True)
sched.add_job(get_data, 'interval', hours=24)
sched.start()



@app.route('/')
def index():
    return render_template('index.html')


@app.route('/casesbydate')
def casesbydate():
    return render_template('casesbydate.html')


@app.route('/activecasesbystates')
def activecasesbystates():
    return render_template('activecasesbystates.html')


@app.route('/dailycases')
def dailycases():
    return render_template('dailycases.html')


@app.route('/dailydeaths')
def dailydeaths():
    return render_template('dailydeaths.html')


if __name__ == '__main__':
    app.run()

#                <iframe id="igraph" scrolling="no" style="border:none;" seamless="seamless" src="https://plotly.com/~_adityadave/3.embed" height="525" width="100%"></iframe>


# Plotly API
username = '_adityadave'
api_key = 'qdGnYdWzyXqsjUQzTXMk'
chart_studio.tools.set_credentials_file(username=username, api_key=api_key)

# Importing data from MongoDB using pandas
client = MongoClient()

# point the client at mongo URI
client = MongoClient('mongodb+srv://adi096:Qwertyuiop@cluster0-zuc0y.mongodb.net/test?retryWrites=true&w=majority')

# select database
db = client['dataproc']

# select the collection within the database
data = db.CovidData

# convert entire collection to Pandas dataframe
data = pd.DataFrame(list(data.find()), columns=['_id', 'date', 'county', 'state', 'fips', 'cases', 'deaths'])
states = data['state']

# Group the data by state
data_statesGrouped = data.groupby(["state"])['cases', 'deaths'].sum().reset_index()

# Plot the bar graph
fig = px.bar(data_statesGrouped, x='state', y='deaths', title='NUMBER OF DEATHS IN US STATES')
fig.update_layout(
    xaxis_title="States",
    yaxis_title="Deaths",
)
fig.show()

# Uploading on Plotly account
py.plot(fig, filename='deaths_by_state', auto_open=True)
# print(tls.get_embed('https://plotly.com/~_adityadave/3/#/'))

# Converting string date into datetime
data['date'] = pd.to_datetime(data['date'])

# Grouping the data by months
data_datesGrouped = data.groupby(data['date'].dt.strftime('%B'))['cases'].sum().sort_values().reset_index()

# Plotting the line graph
fig = px.line(data_datesGrouped, x='date', y='cases')
#fig.update_layout(title='NUMBER OF CASES BY MONTH')
fig.update_layout(
    title='Total Cases in US States',
    xaxis_title="States",
    yaxis_title="Total Cases",
)
# fig.show()
#
#Uploading on Plotly account
py.plot(fig, filename='cases_by_date', auto_open=True)
#

##############################
#
# scope = ['USA']
# data_new = data[data['state'].isin(scope)]
# values = data_new['cases'].tolist()
# fips = data_new['fips'].tolist()
# # deaths = data_new['deaths'].tolist()
#

# data_geoGrouped = data.groupby(['date', 'state']).sum()

# fig = go.Figure(data=go.Choropleth(
#     locations=data_geoGrouped.index, # Spatial coordinates
#     z = data_geoGrouped['deaths'].astype(float), # Data to be color-coded
#     locationmode = 'USA-states', # set of locations match entries in `locations`
#     colorscale = 'Reds',
#     colorbar_title = "Millions USD",
# ))
#
# fig.update_layout(
#     title_text = '2011 US Agriculture Exports by State',
#     geo_scope='usa', # limite map scope to USA
# )
#
# fig.show()


# for series in data.itertuples():
#     for num, item in enumerate(series):
#         print(item)
#     print(series)
#     print("\n")

# using apply function to create a new column Active Cases
data_new = data_statesGrouped
data_new['ActiveCases'] = data_new.apply(lambda row: row.cases - row.deaths, axis=1)

# Print the DataFrame after addition
# of new column
# print(data_new)
# Plotting the scatter graph
fig = go.Figure(data=[go.Scatter(
    x=data_new['state'][0:36],
    y=data_new['ActiveCases'][0:36],
    mode='markers',
    marker=dict(
        size=[100, 90, 80, 70, 60, 50, 40, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35, 30, 28, 28, 25, 25, 20, 15, 15, 15,
              15, 10, 10, 10, 10, 10, 10, 10, 10, 10],

        showscale=True
    )
)])
fig.update_layout(
    title='Active Case in US States',
    xaxis_title="States",
    yaxis_title="Active Cases",
)

# #fig.show()
#
# #Uploading on Plotly account
#py.plot(fig, filename='Active_cases_by_states', auto_open=True)

#DAILY DEATHS IN USA
data_dailyGroupedDeaths = data.groupby(['date'])['deaths'].sum().reset_index().sort_values('deaths', ascending=True)
data_dailyGroupedDeaths['Death Toll'] = data_dailyGroupedDeaths['deaths'].sub(data_dailyGroupedDeaths['deaths'].shift())
data_dailyGroupedDeaths['Death Toll'].iloc[0] = data_dailyGroupedDeaths['deaths'].iloc[0]
data_dailyGroupedDeaths['Death Toll'] = data_dailyGroupedDeaths['Death Toll'].astype(int)
fig = px.bar(data_dailyGroupedDeaths, y='Death Toll', x='date', hover_data=['Death Toll'], color='Death Toll')
#fig.update_layout(title='DAILY DEATHS IN USA')
fig.update_layout(
    title='DAILY DEATHS IN US States',
    xaxis_title="States",
    yaxis_title="Daily Deaths",
)
# fig.show()
#
# #Uploading on Plotly account
#
py.plot(fig, filename='dates_by_deaths', auto_open=True)
#
# #Daily Cases in USA Datewise
data_dailyGroupedCases = data.groupby(['date'])['cases'].sum().reset_index().sort_values('cases', ascending=True)
data_dailyGroupedCases['Daily Cases'] = data_dailyGroupedCases['cases'].sub(data_dailyGroupedCases['cases'].shift())
data_dailyGroupedCases['Daily Cases'].iloc[0] = data_dailyGroupedCases['cases'].iloc[0]
data_dailyGroupedCases['Daily Cases'] = data_dailyGroupedCases['Daily Cases'].astype(int)
fig = px.bar(data_dailyGroupedCases, y='Daily Cases', x='date', hover_data=['Daily Cases'], color='Daily Cases',
             height=500)
#fig.update_layout(title='DAILY CASES IN USA')
fig.update_layout(
    title='DAILY CASES IN USA',
    xaxis_title="States",
    yaxis_title="Total Cases",
)
# #fig.show()
#
py.plot(fig, filename='dates_by_cases', auto_open=True)
