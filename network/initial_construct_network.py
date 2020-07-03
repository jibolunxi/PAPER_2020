from util import mysql_pdbc
import csv
import numpy as np
from util.ProcessBar import ProcessBar
from data import data_clean


FORK_WEIGHT = 100
SAME_OWNER_WEIGHT = 50
SAME_STAR_WEIGHT = 0.5
SAME_CODER_WEIGHT = 10
# SAME_LANGUAGE_WEIGHT = 1


# 将节点与边输出到csv
def print_to_csv(filename, data, headers):
    with open(filename, 'w', newline='')as f:
        f_csv = csv.DictWriter(f, headers)
        f_csv.writeheader()
        f_csv.writerows(data)


# csv获取项目id和star数
def get_repo_star(year):
    filename = "repo_star_count_year.csv"
    new_filename = filename.replace("year", str(year))
    res = {}
    with open(new_filename, 'r', encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)  # 获取数据的第一列，作为后续要转为字典的键名 生成器，next方法获取
        csv_reader = csv.DictReader(f, fieldnames=header)
        for row in csv_reader:
            d = {}
            for k, v in row.items():
                d[k] = int(v)          # 类型改为int
            res[d['id']] = d['count']
    return res


# csv获取项目id和issue数
def get_repo_issue(year):
    filename = "repo_issue_count_year.csv"
    new_filename = filename.replace("year", str(year))
    res = {}
    with open(new_filename, 'r', encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)  # 获取数据的第一列，作为后续要转为字典的键名 生成器，next方法获取
        csv_reader = csv.DictReader(f, fieldnames=header)
        for row in csv_reader:
            d = {}
            for k, v in row.items():
                d[k] = int(v)          # 类型改为int
            res[d['id']] = d['count']
    return res


# 获取star数排名前1000，且star数大于5、issue大于10的项目集合
def get_star_1000_repo_by_year(year):
    all_repo_list = get_repo_star(year)
    all_issue_list = get_repo_issue(year)
    all_repo_list_order = sorted(all_repo_list.items(), key=lambda x: x[1], reverse=True)
    print(len(all_repo_list))
    repos_id = []
    for repo in all_repo_list_order:
        try:
            if repo[1] > 5 and all_issue_list[repo[0]] > 5:
                repos_id.append(repo[0])
        except :
            continue
    return repos_id


# 通过id获取代码库名称
def get_name_by_id(db_object, repo_id):
    sql = "select * from projects where id = " + str(repo_id)
    repo = db_object.execute(sql)[0]
    repo_name = repo['url'][29:]
    return repo_name


# 检测repo1和repo2是否为fork关系、所有者是否相同、语言是否相同，返回总权值
def fork_or_owner_relation(db_object, repo1_id, repo2_id):
    weight = 0
    sql = "select * from projects where id = " + str(repo1_id)
    repo1 = db_object.execute(sql)[0]
    sql = "select * from projects where id = " + str(repo2_id)
    repo2 = db_object.execute(sql)[0]

    if repo1['forked_from'] is not None and repo1['forked_from'] == repo2_id:
        weight += FORK_WEIGHT
    if repo2['forked_from'] is not None and repo2['forked_from'] == repo1_id:
        weight += FORK_WEIGHT

    # if repo1['language'] is not None and repo2['language'] is not None and repo1['language'] == repo2['language']:
    #     weight += SAME_LANGUAGE_WEIGHT

    if repo1['owner_id'] == repo2['owner_id']:
        weight += SAME_OWNER_WEIGHT

    return weight


# sql获取每年项目id和对应的Star人员
def get_star_user_by_id(db_object, year, id):
    sql = "select user_id from watchers " \
          "where unix_timestamp(watchers.created_at) < unix_timestamp('next_year-01-01 00:00:00') and repo_id = " + str(id)
    new_sql = sql.replace("next_year", str(year + 1))
    star_users = db_object.execute(new_sql)
    users = []
    for user in star_users:
        users.append(user['user_id'])

    return users


# sql获取每年项目id和对应的Star人员
def get_members_by_id(db_object, year, id):
    sql = "select user_id from project_members " \
          "where unix_timestamp(created_at) < unix_timestamp('next_year-01-01 00:00:00') and repo_id = " + str(id)
    sql_no_time = "select user_id from project_members where repo_id = " + str(id)
    new_sql = sql.replace("next_year", str(year + 1))
    members_users = db_object.execute(new_sql)
    members = []
    for user in members_users:
        members.append(user['user_id'])
    return members


# 统计repo1和repo2 Star和watch中相同的人数
def count_same_star_watch(db_object, repo1_id, repo2_id, year):
    repo1_star_list = []
    repo2_star_list = []

    sql = "select user_id from watchers where repo_id = " + str(repo1_id) + \
          " and unix_timestamp(created_at) < unix_timestamp('next_year-01-01 00:00:00')"
    new_sql = sql.replace("next_year", str(year + 1))
    repo1_star_dict_list = db_object.execute(new_sql)
    for user in repo1_star_dict_list:
        repo1_star_list.append(user['user_id'])

    sql = "select user_id from watchers where repo_id = " + str(repo2_id) + \
          " and unix_timestamp(created_at) < unix_timestamp('next_year-01-01 00:00:00')"
    new_sql = sql.replace("next_year", str(year + 1))
    repo2_star_dict_list = db_object.execute(new_sql)
    for user in repo2_star_dict_list:
        repo2_star_list.append(user['user_id'])

    weight = len(set(repo1_star_list) & set(repo2_star_list))
    return weight


# 统计repo1和repo2 PR中相同的人数
def count_same_pr(db_object, repo1_id, repo2_id, year):
    r1_pr_actor = []
    r2_pr_actor = []

    sql = "select id from pull_requests where head_repo_id = " + str(repo1_id) + " or base_repo_id = " + str(repo1_id)
    repo1_pr_dict_list = db_object.execute(sql)
    for pr in repo1_pr_dict_list:
        sql = "select actor_id from pull_request_history " \
              "where unix_timestamp(created_at) < unix_timestamp('next_year-01-01 00:00:00') " \
              "and pull_request_id = " + str(pr['id'])
        new_sql = sql.replace("next_year", str(year + 1))
        users = db_object.execute(new_sql)
        for user in users:
            r1_pr_actor.append(user['actor_id'])

    sql = "select id from pull_requests where head_repo_id = " + str(repo2_id) + " or base_repo_id = " + str(repo2_id)
    repo2_pr_dict_list = db_object.execute(sql)
    for pr in repo2_pr_dict_list:
        sql = "select actor_id from pull_request_history " \
              "where unix_timestamp(created_at) < unix_timestamp('next_year-01-01 00:00:00') " \
              "and pull_request_id = " + str(pr['id'])
        new_sql = sql.replace("next_year", str(year + 1))
        users = db_object.execute(new_sql)
        for user in users:
            r2_pr_actor.append(user['actor_id'])

    weight = len(set(r1_pr_actor) & set(r2_pr_actor))
    return weight


# 统计repo1和repo2 issue中相同的人数
def count_same_issue(db_object, repo1_id, repo2_id, year):
    r1_issue_actor = []
    r2_issue_actor = []

    sql = "select id from issues where repo_id = " + str(repo1_id) + \
          " and unix_timestamp(created_at) < unix_timestamp('next_year-01-01 00:00:00')"
    new_sql = sql.replace("next_year", str(year + 1))
    repo1_issue_dict_list = db_object.execute(new_sql)
    for issue in repo1_issue_dict_list:
        sql = "select actor_id from issue_events where issue_id = " + str(issue['id'])
        users = db_object.execute(sql)
        for user in users:
            r1_issue_actor.append(user['actor_id'])

    sql = "select id from issues where repo_id = " + str(repo2_id) + \
          " and unix_timestamp(created_at) < unix_timestamp('next_year-01-01 00:00:00')"
    new_sql = sql.replace("next_year", str(year + 1))
    repo2_issue_dict_list = db_object.execute(new_sql)
    for issue in repo2_issue_dict_list:
        sql = "select actor_id from issue_events where issue_id = " + str(issue['id'])
        users = db_object.execute(sql)
        for user in users:
            r2_issue_actor.append(user['actor_id'])

    weight = len(set(r1_issue_actor) & set(r2_issue_actor))
    return weight


if __name__ == '__main__':
    dbObject = mysql_pdbc.SingletonModel()

    for year in range(2009, 2010):
        # 获取star排名前1000，且star数大于5、issue大于10的项目id
        all_repos = data_clean.get_filtered_repos(dbObject, year)
        repos = all_repos[0:1000] if len(all_repos) > 1000 else all_repos

        # 进度条
        size = len(repos)
        pb = ProcessBar(size)

        # 构建size * size的矩阵
        repos_network_matrix = np.zeros([size, size])
        repos_star_user_list = []
        repos_member_list = []
        for repo in repos:
            repos_star_user_list.append(get_star_user_by_id(dbObject, year, repo['id']))
            repos_member_list.append(get_members_by_id(dbObject, year, repo['id']))

        # 权值确定
        for repo_i in range(0, size):
            for repo_j in range(repo_i + 1, size):
                count_weight = 0
                count_weight += fork_or_owner_relation(dbObject, repos[repo_i]['id'], repos[repo_j]['id'])
                count_weight += len(set(repos_member_list[repo_i]) & set(repos_member_list[repo_j])) * SAME_CODER_WEIGHT
                count_weight += len(set(repos_star_user_list[repo_i]) & set(repos_star_user_list[repo_j])) * SAME_STAR_WEIGHT

                # count_weight += count_same_pr(dbObject, repos[repo_i], repos[repo_j], year)
                # count_weight += count_same_issue(dbObject, repos[repo_i], repos[repo_j], year)

                repos_network_matrix[repo_i][repo_j] += count_weight
                repos_network_matrix[repo_j][repo_i] += count_weight
            pb.print_next()

        # 网络扩展
        size = len(all_repos)
        pb = ProcessBar(size)
        for repo in all_repos:
            pb.print_next()
            if repo not in repos:
                join_it = False
                size = len(repos)
                weight_list = list(np.zeros(size))
                for repo_i in range(0, size):
                    count_weight = 0
                    count_weight += fork_or_owner_relation(dbObject, repos[repo_i], repo)
                    count_weight += len(set(repos_member_list[repo_i]) & set(get_members_by_id(dbObject, year, repo))) * SAME_CODER_WEIGHT
                    count_weight += len(set(repos_star_user_list[repo_i]) & set(get_star_user_by_id(dbObject, year, repo))) * SAME_STAR_WEIGHT

                    weight_list[repo_i] = count_weight

                    if count_weight >= 100:
                        join_it = True

                if join_it:
                    repos.append(repo)
                    repos_member_list.append(get_members_by_id(dbObject, year, repo))
                    repos_star_user_list.append(get_star_user_by_id(dbObject, year, repo))
                    row = np.array([weight_list])
                    repos_network_matrix = np.row_stack((repos_network_matrix, row))
                    weight_list.append(0)
                    col = np.array([weight_list])
                    repos_network_matrix = np.column_stack((repos_network_matrix, col.T))

        node_link_res = []
        res_size = len(repos)
        for res_i in range(res_size):
            for res_j in range(res_i + 1, res_size):
                node1_id = repos[res_i]
                node1_name = get_name_by_id(dbObject, node1_id)
                node2_id = repos[res_j]
                node2_name = get_name_by_id(dbObject, node2_id)
                weight = repos_network_matrix[res_i][res_j]
                if weight > 100:
                    node_link_res.append({'Source': node1_name, 'Target': node2_name, 'Weight': weight, 'Type': 'undirected'})
        filename = "node_link_year.csv"
        new_filename = filename.replace("year", str(year))
        print_to_csv(new_filename, node_link_res, ['Source', 'Target', 'Weight', 'Type'])
