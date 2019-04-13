# !/usr/bin/env python3
import psycopg2
"""
Problem 2:

"""


def remove_extra(str):
    newlist = list(str)
    for idx in range(len(newlist)):
        if newlist[idx] == '-':
            newlist[idx] = ' '
    return "".join(newlist)


def most_popular_three():
    sql = ('select log.path , count(log.path) as read_time '
           'from log, articles '
           "where log.path = '/article/' || articles.slug "
           'group by log.path '
           'order by read_time DESC '
           'LIMIT 3 '
           )
    conn = psycopg2.connect("dbname=news")
    cursor = conn.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    for x in result:
        row = x
        string1 = str(row[0])
        string1 = remove_extra(string1)
        print \
            ' "{article}" - {count} views'.format(article=row[0], count=row[1])
    conn.close()


def match_count(str, dict1):
    str1 = str[0:10].lower()
    if str1 in dict1:
        return dict1[str1]
    return 0


def most_popular_author():
    view_dict = {}  # title of articles, number of views
    auth_dict = {}  # name of the author, total number of views
    sql = ('select log.path , count(log.path) as read_time '
           'from log '
           "where log.path LIKE '%article%' "
           'group by log.path '
           'order by read_time DESC '
           'LIMIT 10 '
           )
    conn = psycopg2.connect("dbname=news")
    cursor = conn.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    for row in result:
        string1 = remove_extra(str(row[0]))[9:19]
        # print(string1 + " in log")
        views = row[1]
        view_dict.update({string1: views})
    sql = ('select a.name, b.title '
           'from articles b inner join authors a ON a.id = b.author '
           )
    cursor.execute(sql)
    result = cursor.fetchall()
    for row in result:
        string1 = str(row[0])  # name
        string2 = str(row[1])  # title
        count = match_count(string2, view_dict)
        if string1 in auth_dict:
            count += auth_dict[string1]
        # print string1, string2, count
        auth_dict.update({string1: count})
    sorted_keys = sorted(auth_dict, key=auth_dict.get, reverse=True)
    for auth in sorted_keys:
        print auth, " -- ", auth_dict[auth]


def error_analysis():
    """
    select distinct date from db
    for each date count column, and count column of error
    report date with error rate if more than 1%
    """
    sql = ('create view logs as '
           "select to_char(time, 'YYYY MM DD') as date, count(*) as logCount "
           'from log '
           'group by date; '
           'create view errorlogs as '
           "select to_char(time, 'YYYY MM DD') as date, "
           "count(*) as errorCount "
           'from log '
           "where status like '404%' "
           'group by date; '
           'select distinct errorlogs.date, errorCount, logCount '
           'from logs INNER JOIN errorlogs ON logs.date = errorlogs.date '
           'where (errorCount * 1000 / logCount) >= 10 '
           )
    conn = psycopg2.connect("dbname=news")
    cursor = conn.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    for row in result:
        date = row[0]
        errorCount = row[1]
        logCount = row[2]
        print date, " -- ", \
            format(float(errorCount) * 100 / logCount, '.2f'), "% errors"


most_popular_three()

most_popular_author()

error_analysis()
