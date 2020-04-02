# Overview Control flow tasks

## Database and logging specific tasks

ControlFlow Tasks are an easy way to run database independent sql code or to avoid the boilerplate code when you just want to execute
a simple statement. Control Flow task can be split in general tasks and logging tasks. General tasks allow you to create or drop objects 
or to run some particular predefined statements that will execute on any database. Logging tasks are useful helper when you need
to create some tables for logging or to retrieve the whole log as Json. 

Control Flow Tasks reside in the `ALE.ETLBox.ControlFlow` namespace - tasks for logging in the `ALE.ETLBox.Logging` namespace.

This article will go into the details about the general ControlFlow Tasks - [see the article about logging to learn more about logging
specific tasks](overview_logging.md).

## General Idea behind Control Flow Task

Control Flow Tasks are a set of tasks to manage, alter or query a database. With one single line of code you will be able to create 
a table or fire some sql on your database - you write your code in C#, and in the background database code for your specific 
database is generated. 

ControlFlow Tasks do *not* strive to be an replacement for an ORM (Object relation mapper), nor is it a complete set of tasks for all the operations
you actually can do on the database. They were created as useful helper to create Tests for ETL pipelines - they can help you to (re)create 
databases, drop tables (if they exists) or to create a test environment on your database without writing to much database specific sql code. 

### Avoiding boilerplate code

If you have ever wrote some ADO.NET code to simply execute some Sql, you probably found that there is some boilerplate code 
you have to write over and over again. The idea behind some of the Control Flow Tasks is that you don't have to write the same 
code again and again, e.g. just for doing something trivial like opening up a connection 
and counting the rows in table. This should be doable with only one line of .NET/C# code, and it should work independently on every database.

#### RowCount with ADO.NET

For instance, the code for establishing a connection and doing a simple row count on a table with a classic ADO.NET connection 
would look like this:

```C#
string connectionString = "Data Source=.; Database=Sample; Integrated Security=SSPI";
using (SqlConnection con = new SqlConnection(connectionString))
{
   SqlCommand cmd = new SqlCommand("select count(*) from demotable", con);
   con.Open();
   int numrows = (int)cmd.ExecuteScalar();   
}
```

#### RowCount with Control Flow Tasks

Now let's have a look how to do a row count with the Control Flow Tasks library. 
First, we need to setup a connection manager in order to connect with database.
This is the same connection manager as used within the data flow part. 

```C#
SqlConnectionManager connectionManager = new SqlConnectionManager("Data Source=.; Database=Sample; Integrated Security=SSPI"");
```

Now you can use a `RowCountTask` to query the number of rows within a table with only one line.

```C#
int count = RowCountTask.Count(connectionManager, "demotable");
```

Optionally, you can set up a default connection that is used every time you don't provide a connection manager. 
Simple set the property `DefaultDbConnection` on the static `ControlFlow` class.

```C#
ControlFlow.DefaultDbConnection = new SqlConnectionManager(new SqlonnectionString("Data Source=.; Database=Sample; Integrated Security=SSPI""));
```

Now a RowCount is as simple as this:

```C#
int count = RowCountTask.Count("demotable");
```

Internally, an ADO.NET connection is opened up (the default ADO.NET connection pooling is used) 
and a `select count(*) from demotable` is executed on the database. The result is returned from the RowCountTask. 

## Create, Drop and IfExists tasks

There are a lot of tasks that can help you to create, drop or check the existence of database objects. 
In the following there will be example how to create tables, views, procedures, indexes or databases, how to drop them and
how to check for their existence.

### ConnectionManager 

All example have `connectionManager` object passed - this is the actual connection manager for your database.
E.g. for Sql Server you would create it with 

```C#
SqlConnectionManager connectionManager = new SqlConnectionManager("Data Source=.; Database=Sample; Integrated Security=SSPI");
```

or for MySql with

```C#
MySqlConnectionManager connectionManager = new MySqlConnectionManager("Server=10.37.128.2;Database=ETLBox_DataFlow;User Id=postgres;Password=etlboxpassword;");
```

### Tables

#### CreateTables Task

CreateTableTask will help you to create a table on the database. You can pass either a `TableDefinition` object
or a table name and a list of table colums.

Here is an example with passing the table name and a list of table columns

```C#
List<TableColumn> columns = new List<TableColumn>() {
    new TableColumn("Id", "INT",allowNulls:false,isPrimaryKey:true, isIdentity: true),
    new TableColumn("value2", "DATE", allowNulls:true),
    new TableColumn("value3", "DECIMAL(10,2)",allowNulls:false) { DefaultValue = "3.12" },
    new TableColumn("compValue", "BIGINT",allowNulls:true) { ComputedColumn = "(value1 * value2)" }
};            
           
CreateTableTask.Create(connectionManager, "tablename", columns);
```

A table column describe the column in the database with the most used attributes: Name, Data type (use the data type
which you are most familiar with, there will be an attempt to convert the database type into the right database specific format),
if the column is nullable, if the column is a primary key and if the column is used as identity column. (Serial for Postgres or
auto increment for MySql). Additionaly, you could specify if the column is a computed column or if it has a default value. 

Here is an example for creating a `TableDefinition` and pass it to the CreateTableTask:

```C#
TableDefinition CustomerTableDef = new TableDefinition("customer",
    new List<TableColumn>() {
        new TableColumn("CustomerKey", "int",allowNulls: false, isPrimaryKey:true, isIdentity:true),
        new TableColumn("Name","nvarchar(200)", allowNulls: false),
    });
CreateTableTask.Create(connectionManager, CustomerTableDef);
```

#### DropTableTask

This task simple drops a table (and optionally checks if the table exists):
```C#
DropTableTask.Drop(connectionManager, "DropTableTest");
DropTableTask.DropIfExists(connectionManager, "DropTableTest");
```

#### IfExistsTableOrViewTask

This task checks if a table or view exists and returns true or false.

```C#
bool exists = IfTableOrViewExistsTask.IsExisting(connectionManager, "tablename");
```

#### Table Definition

If you are interesting in retrieving a TableDefinition object from an existing database table, use can use 
the static method `GetDefinitionFromTableName` on the `TableDefinition` class:

```C#
TableDefinition.GetDefinitionFromTableName("demoTable", connectionManager);
```

### Views

#### CreateViewTask

Creates a view on the database. If the view already exists, it will alter (or replace) the existing view.

```C#
 CreateViewTask.CreateOrAlter(connectionManager, "View1", "SELECT 1 AS Test");
```

#### DropViewTask

Drops a view (and optionally checks if the view exists).

```C#
DropViewTask.Drop(connectionManager, "viewname");
DropViewTask.DropIfExists(connectionManager, "viewname");
```

### Indexes

Similar to views and tables, you can (re)create, drop or check the existence as well on indexes.

```C#
//Create an index
CreateIndexTask.CreateOrRecreate(connection, "indexname", "tablename",
    new List<string>() { "index_column_1", "index_column_1" });

//Drop an index
DropIndexTask.DropIfExists(connectionManager, "indexname", "tablename");

//Check if an index exists
bool exists = IfIndexExistsTask.IsExisting(connectionManager, "indexname", "tablename");
```

### Procedures

```C#
//Create a procedure
List<ProcedureParameter> pars = new List<ProcedureParameter>() {
    new ProcedureParameter("Par1", "VARCHAR(10)"),
    new ProcedureParameter("Par2", "INT", "7"),
};
CreateProcedureTask.CreateOrAlter(connectionManager, "ProcedureName", "SELECT 1;", pars);

//Drop a procedure
DropProcedureTask.DropIfExists(connectionManager, "ProcedureName");

//Check if a procedure exists
bool exists = IfProcedureExistsTask.IsExisting(connectionManager, "ProcedureName");
```

### Schema

Schema are only available for Sql Server and Postgres databases. For MySql, use Create/Drop/Exists Database instead. 

```C#
//Create a schema
CreateSchemaTask.CreateOrAlter(connectionManager, "SchemaName");

//Drop a schema
DropSchemaTask.DropIfExists(connectionManager, "SchemaName");

//Check if a schema exists
bool exists = IfSchemaExistsTask.IsExisting(connectionManager, "SchemaName");
```

### Databases

This is not supported with SQLite. 

```C#
//Create a database
CreateDatabaseTask.CreateOrAlter(connectionManager, "DBName");

//Drop a database
DropDatabaseTask.DropIfExists(connectionManager, "DBName");

//Check if a database exists
bool exists = IfDatabaseExistsTask.IsExisting(connectionManager, "DBName");
```

#### Retrieving the connection without catalog or database

In some cases, you might want to get a connection string without a catalog, e.g. because you need to create the database first.
This is where you could use the ConnectionString-Wrapper for you database. E.g., for Postgres you could run the following code:

```C#
PostgresConnectionString conStringWrapper = new PostgresConnectionString("Server=10.37.128.2;Database=ETLBox_DataFlow;User Id=postgres;Password=etlboxpassword;");
PostgresConnectionString connectionWithoutCatalog = conStringWrapper.CloneWithMasterDbName();
PostgresConnectionManager connectionManager = new PostgresConnectionManager(connectionWithoutCatalog);
```

Your connection manager would now connect to the "postgres" database (which would be the "master" database in Sql Server and "mysql" database
in MySql).

## RowCount

## Truncate a table

Truncating a table is as simple as

```C#
TruncateTableTask.Truncate(connectionManager, "demo.table1");
```

## SqlTask

This is the swiss-army knife for running sql on your database. It will use the underlying ADO.NET connection manager, 
which allows you to do almost everything on the database, without the "overhead" and boilerplate code that ADO.NET brings with it. 

SqlTask always expects a descriptive name when you use it - this name is used for logging purposes. 

```C#
SqlTask.ExecuteNonQuery(connectionManager, "Insert data",
   $@"insert into demo.table1 (value) select * from (values ('Ein Text'), ('Noch mehr Text')) as data(v)");
```

ExecuteNonQuery will just execute the sql code without reading any results from the database. 

### Using parameters

You can pass parameterized sql code to have the database reuse existing plans in the cache. 

```C#
var parameter = new List<QueryParameter>
{
    new QueryParameter("value1", "INT", 1),
    new QueryParameter("value2", "NVARCHAR(100)", "Test1")
};
SqlTask.ExecuteNonQuery(connectionManager, "Test insert with parameter",
    $"INSERT INTO ParameterTest VALUES (@value1, @value2)", parameter);
```

### Reading result sets

#### Scalar values

If you result set contains only one row with one column, you can use the `ExecuteScalar` methods to retrieve that value.

```C#
//without type conversion
object result = SqlTask.ExecuteScalar(connectionManager, "Execute scalar",
    $@"SELECT 'Hallo Welt' AS ScalarResult");

//with type conversion
double? result = SqlTask.ExecuteScalar<double>(connectionManager,"Execute scalar with datatype",
    $@"SELECT CAST(1.343 AS NUMERIC(4,3)) AS ScalarResult"));
```

#### Result sets

Use the following code to read a result set from your database and store it in a List object.
The table to read from would have two columns (ColumnA and ColumnB), and the object `MyRow` would have two properties (Col1 and Col2).

```C#
List<MyRow> result = new List<MyRow>();
MyRow CurRow = new MyRow();

SqlTask.ExecuteReader(connectionManager,
    "Test execute reader",
    $"SELECT ColumnA, ColumnB FROM ResultTable"
    , () => CurRow = new MyRow()                    //this is executed before each row
    , () => result.Add(CurRow)                      //this is execute after each row
    , colA => CurRow.Col1 = int.Parse(colA.ToString())
    , colB => CurRow.Col2 = (string)colB
    );
```


### Bulk Inserts

Bulk inserts in ADO.NET normally need an object which implement `IDataReader`. Normally, you use a `DataTable` for this purpose.
But as the implementation of the ADO.NET DataTable has a large overhead and comes with some performance downside, ETLBox
provides it's own object that implements IDataReader: `TableData` can be used to be passed to a bulk insert operation.

Here is an example for a bulk insert:

```C#
TableData<string[]> data = new TableData<string[]>(destTable.TableDefinition);
string[] values = { "1", "Test1" };
data.Rows.Add(values);
string[] values2 = { "2", "Test2" };
data.Rows.Add(values2);
string[] values3 = { "3", "Test3" };
data.Rows.Add(values3);

//Act
SqlTask.BulkInsert(connection, "Bulk insert demo data", data, "TableName");
```

## Using the instances

For every Control Flow Tasks, there are static accessors to simplify the use of the tasks. In order to have 
access to all functionalities of a task, sometime you have to create a new instance of the task object.

If you want to do a row count with an instance instead of the static accessors, it would look like this:
```C#
RowCountTask task = new RowCountTask("demotable");
int count = task.Count().Rows;
```

So whenever you don't find a static accessor satisfying your needs, try to create an instance and check the 
properties and methods of the object.

### Configure a task

But there is more. Let's assume you want to count the rows on a pretty big table, a "normal" row count perhaps would take some time. 
So RowCount has a property called `QuickQueryMode`. If set to true, a sql statement that queries the partition tables is then executed. 

```C#
RowCountTask task = new RowCountTask("demotable") 
	{ QuickQueryMode = true };
int count = task.Count().Rows;
```

This would give you the same result, but instead of doing a `select count(*) from dbo.tbl` the following sql is fired on the database
```sql
select cast(sum([rows]) as int) from sys.partitions where [object_id] = object_id(N'dbo.tbl') and index_id in (0,1)
```

**Note**: The QuickQueryMode will create sql code that only works on SqlServer. For other databases, this won't work.

## Why not Entitiy Framework

ETLBox was designed to be used as an ETL object library. Therefore, the user normally deals with big data, some kind of datawarehouse structures and is used to
have full control over the database. With the underlying power of ADO.NET - which is used by ETLBox - you have full access to the database and basically can do anything 
you are used to do with sql on the server. As EF (Entity Framework) is a high sophisticated ORM tool, it comes with the downside that you can only do things on a database that
EF allows you to do. But as EF does not incorporate all the possibilities that you can do with SQL and ADO.NET on a Sql Server, Entitity Framework normally isn't a 
good choice for creating ETL jobs. This is also true for other ORM tools.
