from util import mysql_pdbc
import numpy as np
from util.ProcessBar import ProcessBar
from data import data_clean
from util import util


FORK_WEIGHT = 100
SAME_OWNER_WEIGHT = 30
SAME_STAR_WEIGHT = 0.5
SAME_CODER_WEIGHT = 10
WEIGHT_THRESHOLD = 20


# 通过id获取代码库名称
def get_name_by_id(db_object, repo_id):
    sql = "select * from projects where id = " + str(repo_id)
    repo = db_object.execute(sql)[0]
    repo_name = repo['url'][29:]
    return repo_name


# 检测repo1和repo2是否为fork关系、所有者是否相同，返回总权值
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


# sql获取每年项目id和对应的开发人员
def get_members_by_id(db_object, year, id):
    sql = "select user_id from project_members " \
          "where unix_timestamp(created_at) < unix_timestamp('next_year-01-01 00:00:00') and repo_id = " + str(id)
    sql_no_time = "select user_id from project_members where repo_id = " + str(id)
    new_sql = sql.replace("next_year", str(year + 1))
    members_users = db_object.execute(new_sql)
    members = []
    for user in members_users:
        members.append(user['user_id'])
    sql = "select * from projects where id = " + str(id)
    repo = db_object.execute(sql)[0]
    members.append(repo['owner_id'])
    return members


# 初始网络权值确定
def calculate_weight(dbObject, repos, repos_member_list, link_filename, node_filename):
    # 进度条
    size = len(repos)
    pb = ProcessBar(size)

    for repo_i in range(0, size):
        node_data = [repos[repo_i]['id'], repos[repo_i]['id']]
        util.print_list_row_to_csv(node_filename, node_data, 'a')
        for repo_j in range(repo_i + 1, size):
            count_weight = 0
            count_weight += fork_or_owner_relation(dbObject, repos[repo_i]['id'], repos[repo_j]['id'])
            count_weight += len(set(repos_member_list[repo_i]) & set(repos_member_list[repo_j])) * SAME_CODER_WEIGHT

            if count_weight >= WEIGHT_THRESHOLD:
                link_data = [repos[repo_i]['id'], repos[repo_j]['id'], count_weight, 'undirected']
                util.print_list_row_to_csv(link_filename, link_data, 'a')

        pb.print_next()


# 网络扩展
def network_expansion(dbObject, year, all_repos, repos, repos_member_list, link_filename, node_filename):
    size = len(all_repos)
    pb = ProcessBar(size)
    for repo in all_repos:
        pb.print_next()
        if repo not in repos:
            join_it = False
            members = get_members_by_id(dbObject, year, repo['id'])
            size = len(repos)
            weight_list = list(np.zeros(size))
            for repo_i in range(0, size):
                count_weight = 0
                count_weight += fork_or_owner_relation(dbObject, repos[repo_i]['id'], repo['id'])
                count_weight += len(set(repos_member_list[repo_i]) & set(members)) * SAME_CODER_WEIGHT
                # count_weight += len(set(repos_star_user_list[repo_i]) & set(get_star_user_by_id(dbObject, year, repo['id']))) * SAME_STAR_WEIGHT

                weight_list[repo_i] = count_weight

                if count_weight >= WEIGHT_THRESHOLD:
                    join_it = True

            if join_it:
                node_data = [repo['id'], repo['id']]
                util.print_list_row_to_csv(node_filename, node_data, 'a')
                repos.append(repo)
                repos_member_list.append(members)
                # repos_star_user_list.append(get_star_user_by_id(dbObject, year, repo['id']))
                for value_i in range(0, size):
                    if weight_list[value_i] >= WEIGHT_THRESHOLD:
                        link_data = [repos[value_i]['id'], repo['id'], weight_list[value_i], 'undirected']
                        util.print_list_row_to_csv(link_filename, link_data, 'a')


if __name__ == '__main__':
    dbObject = mysql_pdbc.SingletonModel()

    for year in range(2011, 2016):
        # 结果输出文件初始化
        link_filename = "links_year.csv"
        link_filename = link_filename.replace("year", str(year))
        util.print_list_row_to_csv(link_filename, ['Source', 'Target', 'Weight', 'Type'], 'w')
        node_filename = "nodes_year.csv"
        node_filename = node_filename.replace("year", str(year))
        util.print_list_row_to_csv(node_filename, ['id', 'label'], 'w')

        # 获取star排名前1000，且star数大于5、issue大于10的项目id
        all_repos = data_clean.get_filtered_repos(dbObject, year)
        print(len(all_repos))
        for sub_i in range(20):
            if len(all_repos) < 10000 * sub_i:
                break
            else:
                print(sub_i * 10000, all_repos[sub_i * 10000])

        # 获取初始矩阵所包含项目
        all_repos = all_repos[:10000]
        repos = all_repos[0:3000] if len(all_repos) > 3000 else all_repos
        size = len(repos)

        # 项目关注人员列表
        # repos_star_user_list = []
        # 项目成员列表
        repos_member_list = []

        # 获取项目关注人员和成员列表
        for repo in repos:
            # repos_star_user_list.append(get_star_user_by_id(dbObject, year, repo['id']))
            repos_member_list.append(get_members_by_id(dbObject, year, repo['id']))

        # 初始网络权值确定
        calculate_weight(dbObject, repos, repos_member_list, link_filename, node_filename)

        # 网络扩展
        network_expansion(dbObject, year, all_repos, repos, repos_member_list, link_filename, node_filename)

