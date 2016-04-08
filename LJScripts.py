import datetime as dt
import pandas
from bson import Code
# pip install pyfunctional
from functional import seq
from pymongo import MongoClient
import matplotlib

matplotlib.use('TkAgg')
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
    py.plot(fig, validate=False, filename='d3-world-map')


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


def draw_first_post_plot(db):
    n_users_per_month_and_year = db.get_collection("livejournal_number_of_users_per_month_and_year").find()
    x = []
    y = []
    for iterable in list(n_users_per_month_and_year):
        if str(iterable['_id']).replace(" ", " ") > "20001":
            time = str(iterable['_id'])
            x.append(dt.datetime.strptime(time, '%Y %m').date())
            y.append(iterable['value'])

    # x = map(lambda dbo: dbo['_id'], iterables)
    # y = map(lambda dbo: dbo['value'], iterables)
    print(x)
    print(y)
    plt.figure(figsize=(20, 10))
    plt.title('First post activity')
    plt.xlabel('First post date')
    plt.ylabel('Number of users')

    import matplotlib.dates as mdates
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y %m'))
    plt.gca().xaxis.set_major_locator(mdates.YearLocator())
    plt.bar(x, y, label="D")
    plt.legend()
    plt.show()
    plt.savefig("First_post_activity_pic.jpg", dpi=1000, format='jpg')


# countries = pandas.read_csv("iso_3166_2_countries.csv", delimiter=',')[["Common Name", "ISO 3166-1 2 Letter Code", "ISO 3166-1 3 Letter Code"]]
# print(countries)

# users = get_users()
# print(users)

# merge = pandas.merge(users, countries, on='ISO 3166-1 2 Letter Code', how='left')
# print(merge)

# draw_plot(merge)

db = MongoClient(host="localhost").get_database(name="SNCrawler")
# draw_first_post_plot(db)
#
# cursor = db.get_collection("livejournal_users").find(projection={'_id': 1, 'country': 1})
# with open('D:\\users.csv', "a") as myfile:
#     for entry in cursor:
#         try:
#             myfile.write("{},{}\n".format(entry['_id'], entry['country']))
#         except:
#             None
#
# cursor.close()

# cursor = db.get_collection("livejournal_posts").find(projection={'username': 1, 'eventTimestamp': 1})
# with open("D:\\posts.csv", "a") as myfile:
#     count = 0
#     for entry in cursor:
#         count += 1
#         if count % 10000 == 0:
#             sys.stdout.write('\r' + str(count))
#         myfile.write("{},{}\n".format(entry['username'], entry['eventTimestamp']))
#
# cursor.close()


# posts = pandas.read_csv("D:\\posts.csv", delimiter=',', names=["username", "eventTimestamp"])
# # posts = posts.groupby(["username"])
# # for group in posts_groupby:
# #     print(group)
# #
# #     postsEvents = [x[1] for x in group[1].values]
# #     username = group[1].values[0][0]
# #     print({"username": username, "eventTimestamps": postsEvents})
#
# users = pandas.read_csv("D:\\users.csv", delimiter=',', names=["username", "country"])
# merge = pandas.merge(left=users, right=posts, on='username', how='left')
#
# print(merge)
# collection = db.get_collection("livejournal_temp")
#
# merge = merge.groupby(["username"])
# for group in merge:
#     postsEvents = [x[2] for x in group[1].values]
#     username = group[1].values[0][0]
#     country = group[1].values[0][1]
#     doc = {"username": username, "eventTimestamps": postsEvents, 'country': country}
#     # print(doc)
#     collection.insert(doc)


# collection = db.get_collection("livejournal_temp").find({'country': 'RU'}, {'country': 1, 'eventTimestamps': 1, 'username': 1})[:]
# ru_collection = db.get_collection("livejournal_temp_ru")
#
# for entry in collection:
#     if len(entry['eventTimestamps']) > 1:
#         print(entry)
#         ru_collection.insert(entry)

#
# ru_collection = db.get_collection("user_and_post_ru").find().limit(100)
ru_collection = list(db.get_collection("users_and_posts_time_ru").find().limit(1)[:])


def toDate(timestamp):
    time = dt.datetime.fromtimestamp(float(timestamp))
    if time.year < 2000: return None
    return "{}-{}-{} 00:00:00".format(time.year, time.month, time.day)

# y = list(map(lambda x: x['_id'], ru_collection))
# print(x)
#

from itertools import groupby
#
# def f(timestamps):
#     localtimestamps = list(map(lambda t: map1(t), timestamps))
#     localtimestamps = map(lambda x: x ,list(groupby(localtimestamps,key=lambda x: x[0])))
#     print(localtimestamps)
#     return localtimestamps


x = []
y = []
z = []
n = 0
for entry in ru_collection:
    if n >25: break

    print(entry)
    dates = []
    for timestamp in entry['eventTimestamps']:
        timestamp = toDate(timestamp)
        dates.append((timestamp, 1))

    # print(dates)

    # dates = list(dates.sort(key=lambda xxx: xxx[0]))

    for group in groupby(dates, key=lambda x: x[0]):
        username = entry['_id']
        date = group[0]
        posts_count = len(list(group[1]))

        if date is not None:
            print("username" + username + "date = " + date + ", count = " + str(posts_count))
            y.append(username)
            x.append(date)
            z.append(posts_count)

    n += 1

    # print(entry['_id'] + " " + entry['country'lambda x: x[0]] + " " + entry['eventTimestamps'])
#
#
# y = list(map(f, ru_collection))
# for yy in y:
#     print(yy)

# y = []

# for entry in list(ru_collection):
#     time = dt.datetime.fromtimestamp(float(entry['eventTimestamp']))
#     print("{} {} {}".format(time.year, time.month, time.day))


# x.append(time.date())
# y.append(entry['value'])

# # x = map(lambda dbo: dbo['_id'], iterables)
# # y = map(lambda dbo: dbo['value'], iterables)
# print(x)
# print(y)
# plt.figure(figsize=(20, 10))
# plt.title('First post activity')
# plt.xlabel('First post date')
# plt.ylabel('Number of users')
#
# import matplotlib.dates as mdates
#
# # plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y %m'))
# # plt.gca().xaxis.set_major_locator(mdates.YearLocator())
# plt.plot(x, y, label="D")
# plt.legend()
# plt.show()


import plotly.graph_objs as go
import plotly.plotly as py

import numpy as np

trace1 = go.Scatter(
    x = x,
    y = y,
    mode='markers',
    marker=dict(
        color = z, #set color equal to a variable
        colorscale='Viridis',
        showscale=True
    )
)
data = [trace1]
py.sign_in(username='djvipmax', api_key='mcjfxjdavg')
py.plot(data, filename='scatter-plot-with-colorscale')
