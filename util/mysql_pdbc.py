import pymysql


class SingletonModel:
    # 数据库连接对象
    __db = None
    # 游标对象
    __cursor = None

    def __new__(self, *args, **kwargs):
        if not hasattr(self, '_instance'):
            self._instance = super().__new__(self)
            # 主机
            host = 'host' in kwargs and kwargs['host'] or 'localhost'
            # 端口
            port = 'port' in kwargs and kwargs['port'] or '3306'
            # 用户名
            user = 'user' in kwargs and kwargs['user'] or 'root'
            # 密码
            passwd = 'passwd' in kwargs and kwargs['passwd'] or 'Mypassword0'
            # 数据库
            db = 'db' in kwargs and kwargs['db'] or 'ghtorrent'
            # 编码
            charset = 'charset' in kwargs and kwargs['charset'] or 'utf8'
            # 打开数据库连接  
            print('连接数据库')
            self.__db = pymysql.connect(host=host, port=int(port), user=user, passwd=passwd, db=db, charset=charset)
            # 创建一个游标对象 cursor
            # self.__cursor = self.__db.cursor()
            self.__cursor = self.__db.cursor(cursor=pymysql.cursors.DictCursor)
        return self._instance

    # 返回执行execute()方法后影响的行数 
    def execute(self, sql):
        self.__cursor.execute(sql)
        rowcount = self.__cursor.rowcount
        all_message = self.__cursor.fetchall()
        return all_message

    # 增->返回新增ID
    def insert(self, **kwargs):
        table = kwargs['table']
        del kwargs['table']
        sql = 'insert into %s set ' % table
        for k, v in kwargs.items():
            sql += "`%s`='%s'," % (k, v)
        sql = sql.rstrip(',')
        print(sql)
        try:
            # 执行SQL语句
            self.__cursor.execute(sql)
            # 提交到数据库执行
            self.__db.commit()
            # 获取自增id
            res = self.__cursor.lastrowid
        except:
            # 发生错误时回滚
            self.__db.rollback()
        return res

    # 删->返回影响的行数
    def delete(self, **kwargs):
        table = kwargs['table']
        where = kwargs['where']
        sql = 'DELETE FROM %s where %s' % (table, where)
        print(sql)
        try:
            # 执行SQL语句
            self.__cursor.execute(sql)
            # 提交到数据库执行
            self.__db.commit()
            # 影响的行数
            rowcount = self.__cursor.rowcount
        except:
            # 发生错误时回滚
            self.__db.rollback()
        return rowcount

    # 改->返回影响的行数
    def update(self, **kwargs):
        table = kwargs['table']
        # del kwargs['table']
        kwargs.pop('table')
        where = kwargs['where']
        kwargs.pop('where')
        sql = 'update %s set ' % table
        for k, v in kwargs.items():
            sql += "`%s`='%s'," % (k, v)
        sql = sql.rstrip(',')
        sql += ' where %s' % where
        print(sql)
        try:
            # 执行SQL语句
            self.__cursor.execute(sql)
            # 提交到数据库执行
            self.__db.commit()
            # 影响的行数
            rowcount = self.__cursor.rowcount
        except:
            # 发生错误时回滚
            self.__db.rollback()
        return rowcount

    # 查->单条数据
    def fetchone(self, **kwargs):
        table = kwargs['table']
        # 字段
        field = 'field' in kwargs and kwargs['field'] or '*'
        # where
        where = 'where' in kwargs and 'where ' + kwargs['where'] or ''
        # order
        order = 'order' in kwargs and 'order by ' + kwargs['order'] or ''
        sql = 'select %s from %s %s %s limit 1' % (field, table, where, order)
        print(sql)
        try:
            # 执行SQL语句
            self.__cursor.execute(sql)
            # 使用 fetchone() 方法获取单条数据.
            data = self.__cursor.fetchone()
        except:
            # 发生错误时回滚
            self.__db.rollback()
        return data

    # 查->多条数据
    def fetchall(self, **kwargs):
        table = kwargs['table']
        # 字段
        field = 'field' in kwargs and kwargs['field'] or '*'
        # where
        where = 'where' in kwargs and 'where ' + kwargs['where'] or ''
        # order
        order = 'order' in kwargs and 'order by ' + kwargs['order'] or ''
        # limit
        limit = 'limit' in kwargs and 'limit ' + kwargs['limit'] or ''
        sql = 'select %s from %s %s %s %s' % (field, table, where, order, limit)
        print(sql)
        try:
            # 执行SQL语句
            self.__cursor.execute(sql)
            # 使用 fetchone() 方法获取单条数据.
            data = self.__cursor.fetchall()
        except:
            # 发生错误时回滚
            self.__db.rollback()
        return data

    # 析构函数，释放对象时使用
    def __del__(self):
        # 释放游标
        self.__cursor.close()
        print('释放游标')
        # 关闭数据库连接
        self.__db.close()
        print('关闭数据库连接')


