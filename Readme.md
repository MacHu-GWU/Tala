#基于Sqlite3的单元搜索引擎
------

##什么叫单元搜索(Field Search)

我们拿IMDB(International Movie DataBase)为例。 每一部电影应可能具有但不限于如下的元素:

1. **movie_id**, 是数字或者字符串。 每一部电影具有唯一的movie_id
2. **title**, 字符串。 每一部电影的名称
3. **director**, 字符串。 电影主导演名字 
4. **cast**, 字符串的集合。 电影参演明星的名字的集合
5. **release_date**, 日期。 电影首映的日期
6. **release_year**, 年份, 数字。 电影首映的年份
7. **genres**, 字符串的集合。 电影的类别, 例如: action, adventure, drama...

我们在搜索电影时候, 很可能希望通过**电影名称**模糊搜索, 或者根据**演员阵容**搜索有某些影星出演的电影, 又或是根据电影的**类别**搜索某一类或者某几类电影, 再也许最后加上**上映时间**的限制, 筛选出在某一定时间区间内上映的电影。 我们对这一类文档搜索, 主要是根据文档的各个单元的进行逻辑与, 或, 非的排列组合进行筛选。 这就叫**单元搜索**。

##单元搜索中的抽象定义

###文档(Document)
一条数据被称作一个文档。 例如之前提到的一部电影的所有数据就叫一个文档。

对应于关系型数据库中每一行(row)的概念。 也对应于MongoDB中的[Document](http://www.w3cschool.cc/mongodb/mongodb-databases-documents-collections.html "MongoDB Document")概念。

每一条文档对应数据库中的一行。每一列对应文档的一个元素, 其中有些是能够被搜索到的, 有些是不能被搜索到的。

###单元(Field)
文档中不同的文档元素被称作单元。 例如前面提到的movie_id, title, cast这都是单元。

对应着关系型数据库中列(column)的概念。 也对应于NonSql中的Key-Value对的概念。

其中有些单元是能被搜索到的, 有些则是不能被搜索到的。 虽然有些单元不能被搜索到, 但是在调用该文档时会随着其他元素一同被显示出来。

###可搜索数据类型(Searchable Dtype)
在数据库中的每一列都有一个数据类型, 例如Interger, Real, Text。 但这些是数据库中的数据类型。 可搜索数据类型定义了文档的每一个单元能以何种方式被搜索到。 例如同样是TEXT, 我们定义movie_id必须**完全匹配**上才能被搜索到; 而title可以被**部分匹配**搜索到。 每一个单元都需要绑定一种可搜索数据类型。 对于不可被搜索到的单元, 我们可以绑定"不可搜索数据类型"(其本身也是一种Searchable Dtype)

###轮廓(Schema)
在**轮廓**中, 我们定义了文档有多少个单元, 每一个单元绑定了何种可搜索数据类型。

    为了方便说明, 以下提及上面的定义时, 都使用英文名。

##单元搜索中的算法

为了方便还是以IMDB movie为例子进行解说。 

- 对于movie\_id, 我们使用完全匹配, 对应Sql语句中的 where movie_id = 'movie\_id'
- 对于title, 我们使用部分匹配, 对应于Sql语句中的 where title LIKE 'pattern'
- 对于时间, 日期和其他数值型单元, 我们可以用Sql中的 where release_date >= "2010-01-01"

而对于集合型单元, 为了避免每次对整表进行扫描, 我们针对集合型单元制作了倒排索引([Invert Index](http://en.wikipedia.org/wiki/Inverted_index "Invert Index")), 并为每一个集合型单元创建一个新表用于保存倒排索引。

所以对于用户的搜索请求, 其他单元可以通过Sql Select语句进行筛选, 而对于集合型单元可以通过Invert Index进行查询, 然后求结果的交集即可。

##实战案例

我们试着用网络上的数据自定义一个搜索引擎。 还是以电影数据为例, 我们可以在这儿[**下载**](http://had.co.nz/data/movies/)到电影数据。(全部的代码可以参考 example_project.py)

###定义每一个Field

	Searchable_UUID # 整个Schema中有且只能有一个Field属于此类; 字符串精确匹配
	Searchable_ID # 字符串精确匹配
	Searchable_TEXT # 模糊字符串匹配
	Searchable_KEYWORD # 字符串集合匹配
	Searchable_DATE # 日期逻辑匹配
	Searchable_DATETIME # 日期时间逻辑匹配
	Searchable_INTEGER # 整数逻辑匹配
	Searchable_REAL # 实数逻辑匹配
	Unsearchable_OBJECT # 无法被搜索到, 但随着文档一起显示, 可以储存任意python对象

	Field(field_name, field_type1, field_type2, ..., primary_key=True/False) # 定义一个单元

###定义Schema

	"""以定义一个电影数据库的schema为例 """
	movie_schema = Schema("movie",
	    Field("movie_id", Searchable_UUID, primary_key=True),
	    Field("title", Searchable_TEXT),
	    Field("year", Searchable_INTEGER),
	    Field("length", Searchable_INTEGER),
	    Field("rating", Searchable_REAL),
	    Field("votes", Searchable_INTEGER),
	    Field("genres", Searchable_KEYWORD),
	    )

###绑定数据库
	
	engine = SearchEngine("movies.db", movie_schema)

###将下载下来的tab数据处理成一条条的document

	def add_data():
	    """extract documents from raw data, and add documents to engine
	    """
	    df = pd.read_csv("movies.tab", sep="\t")
	    documents = list()
	    for index, row in df.iterrows():
	        document = dict()
	        document["movie_id"] = str(index)
	        document["title"] = row["title"]
	        document["year"] = row["year"]
	        document["length"] = row["length"]
	        document["votes"] = row["votes"]
	        genres = list()
	        if row["Action"]:
	            genres.append("Action")
	        if row["Animation"]:
	            genres.append("Animation")
	        if row["Comedy"]:
	            genres.append("Comedy")
	        if row["Drama"]:
	            genres.append("Drama")
	        if row["Documentary"]:
	            genres.append("Documentary")
	        if row["Romance"]:
	            genres.append("Romance")
	        if row["Short"]:
	            genres.append("Short")
	        document["genres"] = "&".join(genres)
	        documents.append(document)
	    
	    engine.add_all(documents) # add data to engine
	
	add_data()
	## 数据看起来是这样的: [('movie_id', '37089'), ('title', 'Now and Then'), ('year', 1995), ('length', 100), ('rating', None), ('votes', 4394), ('genres', 'Drama')]

### 定义一个查询

	def search():
	    query = engine.create_query()
	    query.add(query.query_between("year", 1993, 2000)) # year between 1993, 2000
	    query.add(query.query_contains("genres", "Drama")) # genres contains Drama
	    for document in engine.search_document(query):
	        print(document)

	search()

看到了么? 如果刨去处理rawdata的部分, 整个应用的搭建代码不到20行。 科技本应如此简单。