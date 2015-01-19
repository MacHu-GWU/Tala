##encoding=utf8

from __future__ import print_function
from sqlalchemy import create_engine
from sqlalchemy.sql import select
from collections import OrderedDict
from .util.iterable import grouper
import sqlite3
import time

##################################################
#                                                #
#                 Query class                    #
#                                                #
##################################################

class QueryEqual():
    def __init__(self, field_name, value):
        self.field_name = field_name
        self.value = value
        self.issql = True
        
    def __str__(self):
        return "%s = %s" % (self.field_name,
                            repr(self.value))
        
class QueryGreater():
    def __init__(self, field_name, lowerbound):
        self.field_name = field_name
        self.lowerbound = lowerbound
        self.issql = True
        
    def __str__(self):
        return "%s >= %s" % (self.field_name,
                             repr(self.lowerbound),)
        
class QuerySmaller():
    def __init__(self, field_name, upperbound):
        self.field_name = field_name
        self.upperbound = upperbound
        self.issql = True
    
    def __str__(self):
        return "%s <= %s" % (self.field_name,
                             repr(self.upperbound),)
            
class QueryBetween():
    def __init__(self, field_name, lowerbound, upperbound):
        self.field_name = field_name
        self.lowerbound = lowerbound
        self.upperbound = upperbound
        self.issql = True
        
    def __str__(self):
        return "%s BETWEEN %s AND %s" % (self.field_name,
                                         repr(self.lowerbound),
                                         repr(self.upperbound),)

class QueryStartwith():
    def __init__(self, field_name, prefix):
        self.field_name = field_name
        self.prefix = prefix
        self.issql = True
    
    def __str__(self):
        return "{0} LIKE '{1}%'".format(self.field_name,
                                        self.prefix)
        
class QueryEndwith():
    def __init__(self, field_name, surfix):
        self.field_name = field_name
        self.surfix = surfix
        self.issql = True
    
    def __str__(self):
        return "{0} LIKE '%{1}'".format(self.field_name,
                                        self.surfix)

class QueryLike():
    def __init__(self, field_name, piece):
        self.field_name = field_name
        self.piece = piece
        self.issql = True
    
    def __str__(self):
        return "{0} LIKE '%{1}%'".format(self.field_name,
                                         self.piece)

class QueryContains():
    def __init__(self, field_name, *keywords):
        self.field_name = field_name
        self.subset = {keyword for keyword in keywords}
        self.issql = False

class Query():
    """抽象查询对象, 用于储存用户自定义的查询条件
    讨论:
    
    在同时用sql语句以及倒排索引筛选时有两种生成result的做法:
    1. 先用倒排索引筛选出所有的uuid_set, 然后和用sql语句对主表进行查询所生成的uuid_set求交集。然后利用对
    主键查询的复杂度为1的特性, 按照uuid_set输出结果。
    2. 先用倒排索引筛选出所有的uuid_set, 然后依次检查主表查询的行结果, 如果行中的主键在uuid_set集合中, 
    则输出。
    经过试验证明, 无论什么情况, 方法1的速度都要远远优于方法2。
    """
    def __init__(self, schema):
        self.schema = schema
        self.criterions = list()
        
        self.query_equal = QueryEqual
        self.query_greater = QueryGreater
        self.query_smaller = QuerySmaller
        self.query_between = QueryBetween
        self.query_startwith = QueryStartwith
        self.query_endwith = QueryEndwith
        self.query_like = QueryLike
        self.query_contains = QueryContains
        
    def add_criterion(self, criterion):
        """add a searching criterion 
        all criterions will be joint with AND operation by default.
        """
        if criterion.field_name in self.schema.fields:
            self.criterions.append(criterion)
        else:
            raise Exception("""criterion has to be applied to one of the searchable fields.
            yours is = {0}
            all valid fields = {1}""".format(repr(criterion.field_name),
                                             list(self.schema.fields) ) )
        
    def add(self, criterion):
        """just a wrapper for Query.add_criterion(criterion)
        """
        self.add_criterion(criterion)
        
    def _split_SqlCriterions_and_KeywordCriterions(self):
        """分离对主表查询的criterion和对倒排索引表查询的criterion
        returns
        -------
            sql_criterions: criterion that apply to the main table
            keyword_criterions: criterion that apply to the invert index 
        """
        sql_criterions = list()
        keyword_criterions = list()
        for criterion in self.criterions:
            if criterion.issql:
                sql_criterions.append(criterion)
            else:
                keyword_criterions.append(criterion)
        return sql_criterions, keyword_criterions
 
    def create_sql(self):
        """生成对主表进行查询的SQL语句和若干个对倒排索引表查询的SQL语句
        create one SQL command for the main_table and create several SQL command for invert index
        table
        """
        sql_criterions, keyword_criterions = self._split_SqlCriterions_and_KeywordCriterions()
        
        ### SQL command for the main table (which table_name = Engine.schema_name)
        select_clause = "SELECT\t{0}\n".format(self.schema.uuid)
        select_clause_all = "SELECT\t*\n"
        
        from_clause = "FROM\t{0}\n".format(self.schema.schema_name)
        if len(sql_criterions) >= 1:
            where_clause = "WHERE\t" + "\n\tAND ".join([str(criterion) for criterion in sql_criterions])
        else:
            where_clause = ""
        main_sqlcmd = select_clause + from_clause + where_clause
        main_sqlcmd_select_all = select_clause_all + from_clause + where_clause
        ### SQL command for the invert index table (which table_name = Engine.keyword_fields)
        keyword_sqlcmd_list = list()
        for criterion in keyword_criterions:
            for keyword in criterion.subset:
                select_clause = "SELECT\t{0}\n".format("uuid_set")
                from_clause = "FROM\t{0}\n".format(criterion.field_name)
                where_clause = "WHERE\t{0} = {1}".format("keyword", repr(keyword))
                keyword_sqlcmd_list.append(select_clause + from_clause + where_clause)
        
        return main_sqlcmd, main_sqlcmd_select_all, keyword_sqlcmd_list

##################################################
#                                                #
#              SearchEngine class                #
#                                                #
##################################################

class SearchEngine():
    """
    """
    def __init__(self, database, schema):
        """
        Engine.database
            数据库的文件路径
        
        Engine.schema
            文档的Schema
        
        Engine.connect
            原生的sqlite3.connect(database)的连接
            
        Engine.cursor
            原生的sqlite3.connect(database).cursor()的游标
        
        Engine.engine
            SqlAlchemy database engine
        
        Engine._SAconn
            SqlAlchemy database engine.connect()
            
        Engine.fields
            wrapper for Engine.schema.fields
            
        Engine.tables
            wrapper for Engine.schema.tables
            
        Engine.schema_name
            wrapper for Engine.schema.schema_name
            
        Engine.keyword_fields
            wrapper for Engine.schema.keyword_fields
            
        Engine.uuid
            wrapper for Engine.schema.uuid
        """
        self.database = database
        self.schema = schema
        
        self.connect = sqlite3.connect(self.database)
        self.cursor = self.connect.cursor()
        
        self._create_all()
        
    def _create_all(self):
        """根据schema, 在database中创建所有需要的表格
        """
        engine = create_engine("sqlite:///%s" % self.database, echo=False)
        self.engine = engine
        self.schema.metadata.create_all(self.engine)
        self._SAconn = self.engine.connect()
        
        # add some name space wrapper to access table
        self.fields = self.schema.fields
        self.tables = self.schema.tables
        self.schema_name = self.schema.schema_name
        self.keyword_fields = self.schema.keyword_fields
        self.uuid = self.schema.uuid
        
    def get_table(self, table_name):
        """根据table得到一个表对象
        """
        return self.tables[table_name]
        
    def add_all(self, documents):
        """往表格中填充数据
        
        step 1. 往主表格填充数据
            主表格名称是 self.schema.schema_name
            主表格对象是 self.schema.tables[self.schema.schema_name]
            
        step 2. 针对每一个Keyword_field对应的表, 制作倒排索引, 并往内填充数据
        
            invert_index = {keyword: set_of_uuid}
        """
        st = time.clock()
        print("正在往主表格 %s 中填充数据..." % self.schema_name)
        ins = self.tables[self.schema_name].insert()
        self._SAconn.execute(ins, documents)
        print("\t填充完毕, 一共插入了 %s 条数据" % len(documents))
        
        print("正在为 %s 创建倒排索引..." % self.keyword_fields)
        all_inv_dict = dict() # {keyword_field_name: invert_index}
        for field_name in self.schema.keyword_fields: # initialize a empty dict for invert index
            all_inv_dict[field_name] = dict()
        
        ### 为了制作倒排索引, 我们只需要从主表中选择 uuid 和 keyword field 对应的列即可, 其他列可不管
        
        main_table = self.get_table(self.schema_name) # 得到主表
        selected_columns = list() # 待选的列
        selected_columns.append(main_table.c[self.schema.uuid]) # 先选择 uuid 列
        for field_name in self.schema.keyword_fields: # 再选择所有的 keyword field 对应的列
            selected_columns.append(main_table.c[field_name])
        
        ### 从主表中选择数据, 构建倒排索引
        for row in self._SAconn.execute( select(selected_columns) ):
            dict_row = dict(row) # 将行记录字典化, 这样就可以通过 dict[field_name] 来访问值
            for field_name, inv_dict in all_inv_dict.items():
                uuid = dict_row[self.uuid]
                for key in dict_row[field_name].split("&"):
                    if key in inv_dict:
                        inv_dict[key].add(uuid)
                    else:
                        inv_dict[key] = set([uuid])
        
        ### 将制作好的倒排索引中的数据填入对应的表
        for field_name, inv_dict in all_inv_dict.items():
            ins = self.schema.tables[field_name].insert()
            ### 将所有的数据放入列表, 以进行bulk insert
            records = list()
            for key, value in inv_dict.items():
                records.append( {"keyword": key, "uuid_set": "&".join(value)} ) 
            self._SAconn.execute(ins, records) # 存入数据库
        
        print("\t数据库准备完毕, 可以进行搜索了! 一共耗时 %s 秒" % (time.clock() - st,) )
        
    def create_query(self):
        """生成一个Query对象, 并把引擎所绑定的Schema传给Query
        使得Query能够自行找到Schema中的各个Fields
        """
        return Query(self.schema)
                   
    def search(self, query):
        """根据query进行单元搜索, 返回row
        """
        main_sqlcmd, main_sqlcmd_select_all, keyword_sqlcmd_list = query.create_sql()

        ### 情况1, 主表和倒排索引表都要被查询
        if (len(keyword_sqlcmd_list) >= 1) and ("WHERE" in main_sqlcmd):
            # 得到查询主表所筛选出的 result_uuid_set
            result_uuid_set = {row[0] for row in self.cursor.execute(main_sqlcmd) }
            # 得到使用倒排索引所筛选出的 keyword_uuid_set
            keyword_uuid_set = set.intersection(
                *[set( self.cursor.execute(sqlcmd).fetchone()[0].split("&") ) \
                  for sqlcmd in keyword_sqlcmd_list] )
            # 对两者求交集
            result_uuid_set.intersection_update(keyword_uuid_set)
            # 根据结果中的uuid, 去主表中取数据
            for uuid in result_uuid_set:
                row = self.cursor.execute("SELECT * FROM {0} WHERE {1} = {2}".format(self.schema_name,
                                                                                     self.uuid,
                                                                                     repr(uuid),) ).fetchone()
                yield row
        
        ### 情况2, 只对倒排索引表查询
        elif (len(keyword_sqlcmd_list) >= 1) and ("WHERE" not in main_sqlcmd):
            keyword_uuid_set = set.intersection(
                *[set( self.cursor.execute(sqlcmd).fetchone()[0].split("&") ) \
                  for sqlcmd in keyword_sqlcmd_list] )
            for uuid in keyword_uuid_set:
                row = self.cursor.execute("SELECT * FROM {0} WHERE {1} = {2}".format(self.schema_name,
                                                                                     self.uuid,
                                                                                     repr(uuid),) ).fetchone()
                yield row
        
        ### 情况3, 只对主表查询
        elif (len(keyword_sqlcmd_list) == 0) and ("WHERE" in main_sqlcmd):
            for row in self.cursor.execute(main_sqlcmd_select_all):
                yield row
        
        ### 情况4, 空查询
        else:
            pass
    
    def search_document(self, query):
        """根据query进行单元搜索, 返回 document = dict({field_name: field_value})
        """
        document = OrderedDict()
        for row in self.search(query):
            for key, value in zip(self.fields, row):
                document[key] = value
            yield document
        
    ### ==================== 下面的函数用于打印帮助信息 ====================
    def display_searchable_fields(self):
        """打印所有能被搜索到的单元名和具体类定义"""
        print("\n{:=^100}".format("All searchable fields"))
        for field_name, field in self.fields.items():
            print("\t%s <---> %s" % (field_name, field) )
    
    def display_criterion(self):
        """打印所有引擎支持的筛选条件"""
        query = self.create_query()
        print("\n{:=^100}".format("All supported criterion"))
        print("\t%s" % query.query_equal.__name__)
        print("\t%s" % query.query_greater.__name__)
        print("\t%s" % query.query_smaller.__name__)
        print("\t%s" % query.query_between.__name__)
        print("\t%s" % query.query_startwith.__name__)
        print("\t%s" % query.query_endwith.__name__)
        print("\t%s" % query.query_like.__name__)
        print("\t%s" % query.query_contains.__name__)
    
    def display_keyword_fields(self):
        """打印所有支持倒排索引的单元名和具体类定义"""
        print("\n{:=^100}".format("All keyword fields"))
        for field_name, field in self.fields.items():
            if "Searchable_KEYWORD" in field.search_types:
                print("\t%s <---> %s" % (field_name, field) )
                

    def _get_all_valid_keyword(self, field_name):
        """私有函数, 用于支持Engine.display_valid_keyword, Engine.search_valid_keyword功能
        根据field_name得到该field下所有出现过的keyword
        """
        if field_name in self.keyword_fields:
            all_keywords = [row[0] for row in self.cursor.execute("SELECT keyword FROM %s" % field_name)]
            return all_keywords
        else:
            raise Exception("ERROR! field_name has to be in %s, yours is %s" % (self.keyword_fields, 
                                                                                field_name))

    def display_valid_keyword(self, field_name):
        """打印某个单元下所有有效的keyword的集合"""
        print("\n{:=^100}".format("All valid keyword in %s" % field_name))
        if field_name in self.keyword_fields:
            all_keywords = self._get_all_valid_keyword(field_name)
            all_keywords.sort()
            for chunk in grouper(all_keywords, 5, ""):
                print("\t{0[0]:<20}\t{0[1]:<20}\t{0[2]:<20}\t{0[3]:<20}\t{0[4]:<20}".format(chunk) )
            print("Found %s valid keywords with in %s" % (len(all_keywords), 
                                                                     field_name) )
        else:
            print("ERROR! field_name has to be in %s, yours is %s" % (self.keyword_fields, 
                                                                      field_name) )
           
    def search_valid_keyword(self, field_name, pattern):
        """根据pattern, 搜索在单元名为field_name中可供选择的keyword"""
        print("\n{:=^100}".format("All valid keyword with pattern %s in %s" % (pattern,
                                                                               field_name) ) )
        result = [keyword for keyword in self._get_all_valid_keyword(field_name) if pattern in keyword]
        result.sort()
        for chunk in grouper(result, 5, ""):
            print("\t{0[0]:<20}\t{0[1]:<20}\t{0[2]:<20}\t{0[3]:<20}\t{0[4]:<20}".format(chunk) )
        print("Found %s valid keywords with pattern %s in %s" % (len(result), 
                                                                 pattern, 
                                                                 field_name))
        
    def help(self):
        """print help information"""
        text = """Use the following command to help your query:
        Engine.display_searchable_fields()
        Engine.display_criterion()
        Engine.display_keyword_fields()
        Engine.display_valid_keyword(field_name)
        Engine.search_valid_keyword(field_name, pattern)"""
        print(text)
        
def create_search_engine(database, schema):
    search_engine = SearchEngine(database, schema)
    return search_engine


if __name__ == "__main__":
    from fields import *
    from datetime import date
    
    def unittest_criterion():
        q = QueryGreater("value", 10)
        print(q)
        q = QuerySmaller("value", 20)
        print(q)
        q = QueryBetween("value", 10, 20)
        print(q)

        q = QueryGreater("create_date", "2010-01-01")
        print(q)
        q = QuerySmaller("create_date", "2010-12-31")
        print(q)
        q = QueryBetween("create_date", "2010-01-01", "2010-12-31")
        print(q)
        
        q = QueryStartwith("title", "ABC")
        print(q)
        q = QueryEndwith("title", "XYZ")
        print(q)
        q = QueryLike("title", "US")
        print(q)
        
#     unittest_criterion()

    def unittest_Query():
        movie_schema = Schema("movie",
            Field("movie_id", Searchable_UUID, Searchable_TEXT, primary_key=True),
            Field("title", Searchable_TEXT),
            Field("genres", Searchable_KEYWORD),
            Field("release_date", Searchable_DATE),
            )
        
        query = Query(movie_schema)
        query.add(query.query_between("release_date", "2000-01-01", "2014-12-31"))
        query.add(query.query_between("release_date", 
                                      str(date(1900,1,1)), 
                                      str(date(1950,12,31)), ) )
        query.add(query.query_contains("genres", "drama"))
        
        print("\n{:=^100}".format("Query._split_SqlCriterions_and_KeywordCriterions()"))
        print(query._split_SqlCriterions_and_KeywordCriterions())
        
        main_sqlcmd, main_sqlcmd_select_all, keyword_sqlcmd_list = query.create_sql()
        print("\n{:=^100}".format("main_sqlcmd"))
        print(main_sqlcmd)
        print("\n{:=^100}".format("main_sqlcmd_select_all"))
        print(main_sqlcmd_select_all)
        for sqlcmd in keyword_sqlcmd_list:
            print("\n{:=^100}".format("keyword_sqlcmd"))
            print(sqlcmd)
            
#     unittest_Query()