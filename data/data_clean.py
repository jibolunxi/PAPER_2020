from util import mysql_pdbc


MIN_STAR = 10


# sql获取每年项目id和对应的Star数
def get_star_num(db_object, year):
    sql = "select repo_id id, count(repo_id) count from watchers " \
          "where unix_timestamp(created_at) < unix_timestamp('next_year-01-01 00:00:00') " \
          "and unix_timestamp(created_at) > unix_timestamp('year-01-01 00:00:00')" \
          "group by repo_id"
    sql = sql.replace("next_year", str(year + 1))
    sql = sql.replace("year", str(year))

    star_count_projects = db_object.execute(sql)

    return star_count_projects


# 获取star数筛选后的项目id
def filter_repo(star_count, year):
    res = []
    for repo in star_count:
        try:
            if repo['count'] >= MIN_STAR:
                res.append(repo)
        except:
            print("error")

    return res


def get_filtered_repos(dbObject, year):
    star_count = get_star_num(dbObject, year)
    filtered_star_count = filter_repo(star_count, year)
    filtered_star_count_order = sorted(filtered_star_count, key=lambda x: x['count'], reverse=True)
    return filtered_star_count_order
