from util import mysql_pdbc
import csv
from util import util


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
def get_filtered_repo(star_count, year):
    for repo in star_count:
        try:
            if repo['count'] >= 5:
                star_count.remove(repo)
        except:
            star_count.remove(repo)

    return star_count


# sql获取每年项目id和对应的Star人员
def get_star_user_by_id(db_object, year, id):
    sql = "select user_id from watchers " \
          "where unix_timestamp(watchers.created_at) < unix_timestamp('year-01-01 00:00:00') and repo_id = " + str(id)
    new_sql = sql.replace("year", str(year))
    star_users = db_object.execute(new_sql)

    headers = ['user_id']
    file_name = 'F:\\Code\\Python\\ASE_2020_04\\star_2009\\' + str(id) + '_' + str(year) + '.csv'
    with open(file_name, 'w', newline='')as f:
        f_csv = csv.DictWriter(f, headers)
        f_csv.writeheader()
        f_csv.writerows(star_users)


# sql获取每年项目id和对应的issue数
def get_is_num(db_object, year):
    sql = "select projects.id id, count(issues.id) count from projects, issues " \
          "where projects.id = issues.repo_id" \
          "and unix_timestamp(issues.created_at) < unix_timestamp('next_year-01-01 00:00:00') " \
          "and unix_timestamp(issues.created_at) > unix_timestamp('year-01-01 00:00:00')" \
          "group by projects.id"
    sql = sql.replace("next_year", str(year + 1))
    sql = sql.replace("year", str(year))
    is_rank_1000_projects = db_object.execute(sql)

    if len(is_rank_1000_projects) > 0:
        headers = ['id', 'count']
        file_name = 'repo_issue_count_year.csv'
        new_file_name = file_name.replace("year", str(year))
        with open(new_file_name, 'w', newline='')as f:
            f_csv = csv.DictWriter(f, headers)
            f_csv.writeheader()
            f_csv.writerows(is_rank_1000_projects)


if __name__ == '__main__':
    dbObject = mysql_pdbc.SingletonModel()

    for year in range(2010, 2011):
        star_count = get_star_num(dbObject, year)
        filtered_star_count = get_filtered_repo(star_count, year)
        filtered_star_count_order = sorted(filtered_star_count.items(), key=lambda x: x[1], reverse=True)
