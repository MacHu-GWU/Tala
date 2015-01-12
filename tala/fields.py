##encoding=utf8

from __future__ import print_function
from sqlalchemy import Table, Column, MetaData
from sqlalchemy import Integer, REAL, TEXT, DateTime, Date, PickleType
from collections import OrderedDict

class SEARCHABLE_TYPE():
    def __repr__(self):
        return self.name

class _Searchable_UUID(SEARCHABLE_TYPE):
    """整个Schema中有且只能有一个Field属于此类; 字符串精确匹配"""
    name = "Searchable_UUID"
    sqlite_dtype = TEXT
    sqlite_dtype_name = "TEXT"
    
class _Searchable_ID(SEARCHABLE_TYPE):
    """字符串精确匹配"""
    name = "Searchable_ID"
    sqlite_dtype = TEXT
    sqlite_dtype_name = "TEXT"
    
class _Searchable_TEXT(SEARCHABLE_TYPE):
    """模糊字符串匹配"""
    name = "Searchable_TEXT"
    sqlite_dtype = TEXT
    sqlite_dtype_name = "TEXT"
    
class _Searchable_KEYWORD(SEARCHABLE_TYPE):
    """字符串集合匹配"""
    name = "Searchable_KEYWORD"
    sqlite_dtype = TEXT
    sqlite_dtype_name = "TEXT"
    
class _Searchable_DATE(SEARCHABLE_TYPE):
    """日期逻辑匹配"""
    name = "Searchable_DATE"
    sqlite_dtype = Date
    sqlite_dtype_name = "Date"
    
class _Searchable_DATETIME(SEARCHABLE_TYPE):
    """日期时间逻辑匹配"""
    name = "Searchable_DATETIME"
    sqlite_dtype = DateTime
    sqlite_dtype_name = "DateTime"
    
class _Searchable_INTEGER(SEARCHABLE_TYPE):
    """整数逻辑匹配"""
    name = "Searchable_INTEGER"
    sqlite_dtype = Integer
    sqlite_dtype_name = "Integer"
    
class _Searchable_REAL(SEARCHABLE_TYPE):
    """实数逻辑匹配"""
    name = "Searchable_REAL"
    sqlite_dtype = REAL
    sqlite_dtype_name = "REAL"
    

class _Unsearchable_OBJECT(SEARCHABLE_TYPE):
    """无法被搜索到, 可以储存任意python对象"""
    name = "Unsearchable_OBJECT"
    sqlite_dtype = PickleType
    sqlite_dtype_name = "PickleType"

Searchable_UUID = _Searchable_UUID()
Searchable_ID = _Searchable_ID()
Searchable_TEXT = _Searchable_TEXT()
Searchable_KEYWORD = _Searchable_KEYWORD()
Searchable_DATE = _Searchable_DATE()
Searchable_DATETIME = _Searchable_DATETIME()
Searchable_INTEGER = _Searchable_INTEGER()
Searchable_REAL = _Searchable_REAL()
Unsearchable_OBJECT = _Unsearchable_OBJECT()

##################################################
#                                                #
#                 Field class                    #
#                                                #
##################################################

class Field():
    """Field可能有多种SEARCHABLE_TYPE, 但是只能有一种sqlite_dtype
    所以必须保证Field.search_types的sqlite_dtype都相同。
    """
    def __init__(self, field_name, *list_of_searchable_type, primary_key=False, nullable=True):
        """
        Field.field_name
            string type, 单元名称, 对应数据表中的column_name 
        
        Field.search_types
            dict type, {SEARCHABLE_TYPE.name: SEARCHABLE_TYPE} 同一列可以以多种方式被搜索到
        
        Field.primary_key
            boolean type, 该列在数据库知否是PRIMARY KEY
        
        Field.nullable
            boolean type, 该列是否允许为NULL
        """
        self.field_name = field_name
        self.search_types = {searchable_type.name: searchable_type for searchable_type in list_of_searchable_type}
        self.primary_key = primary_key
        self.nullable = nullable
        
        self.self_validate()
    
    def self_validate(self):
        sqlite_dtype_set = {searchable_type.sqlite_dtype_name for searchable_type in self.search_types.values()}
        if len(sqlite_dtype_set) != 1:
            raise Exception("%s have different sqlite dtype!" % list(self.search_types.keys()))
        
    def __str__(self):
        return "Field(field_name='%s', search_types=%s, primary_key=%s, nullable=%s)" % (self.field_name,
                                                                                         self.search_types,
                                                                                         self.primary_key,
                                                                                         self.nullable)
    
    def __repr__(self):
        return "Field('%s', %s, primary_key=%s, nullable=%s)" % (self.field_name,
                                                                 ", ".join(self.search_types.keys()),
                                                                 self.primary_key,
                                                                 self.nullable)

##################################################
#                                                #
#                 Schema class                   #
#                                                #
##################################################

class Schema():
    """
    有且只有一个Field的search_types中包含Searchable_ID, 而且该Field必须是PRIMARY KEY
    """
    def __init__(self, schema_name, *Fields):
        """
        Schema.schema_name 
            string type, 储存主表的名称
            
        Schema.fields
            OrderedDict type, 储存 {Field名称: Field对象} 的字典
            
        Schema.uuid
            string type, 储存全局主键的 单元/列 的名称
            
        Schema.keyword_fields
            list type, 储存关键字类的 单元/列 的名称
            
        Schema.tables
            dict type, 储存 {Table名称: Table对象}
            
        Schema.metadata
            储存 metadata
        """
        # get self.schema_name
        self.schema_name = schema_name
        # get self.fields
        self.fields = OrderedDict()
        for field in Fields: # OrderedDict({field.name: field object})
            self.fields[field.field_name] = field
        # get self.uuid and self.keyword_fields
        self.uuid = None
        self.keyword_fields = list()
        for field in self.fields.values():
            if "Searchable_UUID" in field.search_types:
                self.uuid = field.field_name
            if "Searchable_KEYWORD" in field.search_types:
                self.keyword_fields.append(field.field_name)
                
        self._create_all_tables()
        self.self_validate()
        
    def _create_SqlAlchemy_columns(self):
        """由Schema中定义的Fields创建主表格所需要的, SqlAlchemy表格中对应的Columns
        Private method
        """
        all_columns = list()
        for field in self.fields.values():
            for searchable_type in field.search_types.values():
                break
            column = Column(field.field_name, # column.name = field.name
                            searchable_type.sqlite_dtype, # column.dtype =
                            primary_key=field.primary_key, # column.primary_key = field.primary_key
                            nullable=field.nullable) # column.nullable = field.nullable
            all_columns.append(column)
        return all_columns
    
    def _create_all_tables(self):
        """创建Schema中所要用到的表。以 {Table.name: Table} 的形式存储在 self.tables中
        1. 存储文档数据的主表
        2. 若干个针对Field.search_types中包含Searchable_KEYWORD所涉及的倒排索引表
        """
        metadata = MetaData()
        self.tables = dict()
        # 创建主表
        all_columns = self._create_SqlAlchemy_columns()
        self.tables[self.schema_name] = Table(self.schema_name, metadata, *all_columns )
        
        # 为KEYWORD属性的列创建索引表
        for field_name in self.keyword_fields:
            self.tables[field_name] = Table(field_name, metadata,
                Column("keyword", TEXT, primary_key=True),
                Column("uuid_set", TEXT),
                )
        
        self.metadata = metadata

    def self_validate(self):
        """检查这个Schema的设置是否合理
        """
        if not self.uuid:
            raise Exception("One and only one Fields can have only one Searchable_UUID!")
        
    def displayFields(self):
        pass
        
if __name__ == "__main__":
    from pprint import pprint as ppt
    
    def unittest_DTYPE():
        """class DTYPE unit test"""
        print(Searchable_UUID)
        print([Searchable_ID, Searchable_TEXT, Searchable_INTEGER])
        
#     unittest_DTYPE()

    def unittest_Field():
        """class Field unit test"""
        movie_id = Field("movie_id", Searchable_UUID, Searchable_TEXT, primary_key=True)
        title = Field("title", Searchable_TEXT)
        genres = Field("genres", Searchable_KEYWORD)
        release_date = Field("release_date", Searchable_DATE)
        print(movie_id)
        print(title)
        print(genres)
        print(release_date)
        
        print(repr(movie_id))
        
        invalid_field = Field("invalid_field", Searchable_UUID, Searchable_DATE)
        
#     unittest_Field()

    def unittest_Schema():
        movie_schema = Schema("movie",
            Field("movie_id", Searchable_UUID, Searchable_TEXT, primary_key=True),
            Field("title", Searchable_TEXT),
            Field("genres", Searchable_KEYWORD),
            Field("release_date", Searchable_DATE),
            )
        
        ppt(movie_schema.__dict__)
            
#     unittest_Schema()