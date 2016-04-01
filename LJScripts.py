import csv

import datetime as dt
import pandas
from bson import Code
from functional import seq
from pymongo import MongoClient
import matplotlib.pyplot as plt


class User:
    def __init__(self, login, url, country):
        self.login = login
        self.url = url
        self.country = country

    def __str__(self):
        return "User{login = " + self.login + ", url = " + self.url + ", country = " + self.country + "}"


def get_users():
    db = MongoClient(host="192.168.13.133").get_database(name="SNCrawler")
    users_collection = db.get_collection("livejournal_users").find()
    count = users_collection.count()
    users = seq(users_collection).map(lambda dbo: User(dbo['login'], dbo['url'], dbo['country'])) \
        .group_by(lambda user: user.country) \
        .map(lambda country_and_users: [country_and_users[0], 100 * len(country_and_users[1]) / count]) \
        .sorted(lambda country_and_users: country_and_users[1], reverse=True)

    users_per_countries = pandas.DataFrame(data=users.to_list(),
                                           columns=["ISO 3166-1 2 Letter Code", "Percent of users"])
    users_collection.close()
    return users_per_countries


def draw_plot(df):
    # Learn about API authentication here: https://plot.ly/python/getting-started
    # Find your api_key here: https://plot.ly/settings/api
    import plotly.plotly as py
    import pandas as pd
    py.sign_in(username='djvipmax', api_key='mcjfxjdavg')
    # df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/2014_world_gdp_with_codes.csv')
    data = [dict(
        type='choropleth',
        locations=df['ISO 3166-1 3 Letter Code'],
        z=df['Percent of users'],
        text=df['Common Name'],
        colorscale=[[0, "rgb(5, 10, 172)"], [0.35, "rgb(40, 60, 190)"], [0.5, "rgb(70, 100, 245)"],
                    [0.6, "rgb(90, 120, 245)"], [0.7, "rgb(106, 137, 247)"], [1, "rgb(220, 220, 220)"]],
        autocolorscale=False,
        reversescale=True,
        marker=dict(
            line=dict(
                color='rgb(180,180,180)',
                width=0.5
            )
        ),
        colorbar=dict(
            autotick=False,
            title='Percent of users'
        ),
    )]
    layout = dict(
        title='LiveJournal user distribution',
        geo=dict(
            showframe=False,
            showcoastlines=False,
            projection=dict(
                type='Mercator'
            )
        )
    )
    fig = dict(data=data, layout=layout)
    url = py.plot(fig, validate=False, filename='d3-world-map')


def group_by_user(db):
    map = Code("""
                function() {
                    emit(this.username, this.eventTimestamp);
                }""")
    reduce = Code("""
                    function(key, values) {
                        return Math.min.apply( Math, (values));
                    }""")
    db.get_collection("livejournal_posts").map_reduce(map, reduce, out="temp")


def group_by_date(db):
    global map, reduce
    map = Code("""
            function() {
                var date = new Date(parseFloat(this.value)*1000);
                var month = date.getMonth() + 1;
                var year = date.getFullYear();
                var key = year.toString() + " " + month.toString();
                emit(key, 1);
            }""")
    reduce = Code("""
                function(key, values) {
                    return Array.sum(values);
                }""")
    db.get_collection("livejournal_users_and_first_post_date").map_reduce(map, reduce, out="temp")


# users = get_users()
# print(users)

# merge = pandas.merge(users, countries, on='ISO 3166-1 2 Letter Code', how='left')
# print(merge)

# draw_plot(merge)


db = MongoClient(host="192.168.13.133").get_database(name="SNCrawler")

# group_by_user(db)
# group_by_date(db)
#
n_users_per_month_and_year = db.get_collection("livejournal_number_of_users_per_month_and_year").find()

x = []
y = []
for iterable in list(n_users_per_month_and_year):
    if str(iterable['_id']).replace(" ", " ") > "20001":
        time = str(iterable['_id'])
        x.append(dt.datetime.strptime(time,'%Y %m').date())
        y.append(iterable['value'])

# x = map(lambda dbo: dbo['_id'], iterables)
# y = map(lambda dbo: dbo['value'], iterables)

print(x)
print(y)


plt.figure(figsize=(20,10))
plt.title('First post activity')
plt.xlabel('First post date')
plt.ylabel('Number of users')

import matplotlib.dates as mdates

plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y %m'))
plt.gca().xaxis.set_major_locator(mdates.YearLocator())

plt.bar(x, y)
plt.legend()
# plt.show()
plt.savefig("First post activity pic.jpg", dpi=1000, format='jpg')

