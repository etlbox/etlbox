# Database integration

There a numerous database sources and destinations that come with ETLBox. In short, you can extract data 
from and load data into the following databases: Sql Server, MySql, Postgres, SQLite and Microsoft Access.

## DbSource

The DbSource is the most common data source for a data flow. It basically connects to a database via ADO.NET and executes a SELECT-statement to start reading the data. 
While data is read from the source, it is simultaneously posted into the dataflow pipe. This enables the DbSource also to handle big amount of data - it constantly can 
read data from a big table while already read data can be processed by the connected componentens. 

To initialize a DbSource, you can simply pass a table (or view) name or a SQL-statement. The DbSource also accepts a connection manager. 

The following code would read all data from the table `SourceTable` and use the default connection manager:

```C#
DbSource source = new DbSource("SourceTable");
```

For the `DbSource`, you can also specify some Sql code to retrieve your data:

```C#
DbSource source = new DbSource() {
    Sql = "SELECT * FROM SourceTable"
};
```

### Working with types

In the examples above we used a  object without a type.
This will let ETLBox work internally with an `ExpandoObject` which is a dynamic .NET object type.
Let's assume that SouceTable has two columns:

ColumnName|Data Type
----------|---------
Id|INTEGER
Value|VARCHAR

Reading from this table using the DbSource without type will internally create a dynamic object with two properties: Id of type int and Value of type string.

Working with dynamic objects has some drawbacks, as .NET is a strongly typed language. Of course you can also use a generic object 
to type the DbSource.

Let's assume we create a POCO (Plain old component object) `MySimpleRow` that looks like this:

```C#
public class MySimpleRow {
    public int Id { get; set;}
    public string Value { get; set;}
}
```

Now we can read the data from the source with a generic object:

```C#
DbSource<MySimpleRow> source = new DbSource<MySimpleRow>("SourceTable");
```

ETLBox will autamtically extract missing meta information during runtime from the source table and the involved types. In our example, the source table has
the exact same columns as the object - ETLBox will check this and write data from the Id column into the Id property, and data from the column Value into the Value property.
Each record in the source will be a new object that is created and then passed to the connected components. 

Of course the properties in the object and the columsn can differ - ETLBox will only load columns from a source where it can find the right property. If the data type is different,
ETLBox will try to automatically convert the data. If the names are different, you can use the attribute ColumnMap to define the matching columns name for a property. 
In our example, we could replace the property Id with a property Key - in order to still read data from the Id column, we add the ColumnMap attribute. Also, if we change
the data type to string, ETLBox will automatically convert the integer values into a string. 

```C#
[ColumnMap("Id")]
public string Key { get;set; }
```

## DbDestination

Like the `DbSource`, the `DbDestination` is the common component for sending data into a database. It is initialized with a table name.
Unlike other Destinations, the DbDestination inserts data into the database in batches. The default batch size is 1000 rows - the DbDestination waits
until it's input buffer has reached the batch size before it bulk inserts the data into the database. 

The following example would transfer data from the destination to the source:

```C#
DbSource source = new DbSource("SourceTable");
DbDestination dest = new DbDestination("DestinationTable");
//Link everything together
source.LinkTo(dest);
//Start the data flow
source.Execute();
dest.Wait()
```

## Connection manager

### Connection strings

To connect to your database of choice, you will need a string that contains all information needed to connect
to your database (e.g., the network address of the database, user name and password). The specific connection string syntax 
for each provider is defined by the ADO.NET framework. If you need assistance
to create such a connection string, <a href="https://www.connectionstrings.com" target="_blank">have a look at this website that 
provide example strings for almost every database</a>.

### Database Connections

The `DbSource` and `DbDestination` can be used to connect via ADO.NET to a database server. 
To do so, it will need the correct connection manager and either a raw connection string or a `ConnectionString` object. 
The easiest way is to directly pass a raw connection string and create with it a connection manager.  

Here is an example creating a connection manager for Sql Server and pass it to a DbSource:

```C#
DbSource source = DbSource (
    new SqlConnectionManager("Data Source=.;Integrated Security=SSPI;Initial Catalog=ETLBox;")
    , "SourceTable"
);
```

For other databases the code looks very similar. Please be aware that the connection string might look different.

This is how you create a connection manager for MySql:

```C#
MySqlConnectionManager connectionManager = new MySqlConnectionManager("Server=10.37.128.2;Database=ETLBox_ControlFlow;Uid=etlbox;Pwd=etlboxpassword;";
```

Here the example code for creating a connection manager for Postgres:

```C#
PostgresConnectionManager connectionManager = new PostgresConnectionManager("Server=10.37.128.2;Database=ETLBox_DataFlow;User Id=postgres;Password=etlboxpassword;");
```

Creation of a connection manager for SQLite:

```C#
SQLiteConnectionManager connectionManager = new SQLiteConnectionManager("Data Source=.\\db\\SQLiteControlFlow.db;Version=3;");
```

### Default ConnectionManager

Every component or task related to a database operation needs to have a connection managers set in order
to connect to the right database. Sometimes it can be cumbersome to pass the same connection manager over and over
again. To avoid this, there is a static `ControlFlow` class that contains the property `DefaultDbConnection`.
If you define a connection manager here, this will always be used as a fallback value if no other connection manager property was defined.

```
ControlFlow.DefaultDbConnection = new SqlConnectionManager("Data Source=.;Integrated Security=SSPI;Initial Catalog=ETLBox;");
//Now you can just create a DbSource like this
var source = new DbSource("SourceTable");
```

### Connection String wrapper

When you create a new connection manager, you have the choice to either pass the connection string directly or you
 create an adequate ConnectionString object from the connection string before you pass it to the connection manager.
 The ConnectionString object does exist for every database type (e.g. for MySql it is `MySqlConnectionString`). The ConnectionString
 wraps the raw database connection string into the appropriate ConnectionStringBuilder object and also offers some more
 functionalities, e.g. like getting a connection string for the database storing system information. 

```C#
SqlConnectionString etlboxConnString = new SqlConnectionString("Data Source=.;Integrated Security=SSPI;Initial Catalog=ETLBox;");
SqlConnectionString masterConnString = etlboxConnString.CloneWithMasterDbName();

//masterConnString is equal to "Data Source=.;Integrated Security=SSPI;"
SqlConnectionManager conectionToMaster = new SqlConnectionManager(masterConnString); 
```

#### ODBC Connections

The `DbSource` and `DbDestination` also works with ODBC connection. Currently ODBC connections with Sql Server and Access are supported. 
You will still use the underlying ADO.NET, but it allows you to connect to SQL Server or Access databases via ODBC. 

Here is how you can connect via ODBC:
  
```C#
DbSource source = DbSource (
    new SqlODBCConnectionManager("Driver={SQL Server};Server=.;Database=ETLBox_ControlFlow;Trusted_Connection=Yes"),
    "SourceTable"
);
```

*Warning*: ODBC does not support bulk inserts like in "native" connections.
The `DbDestination` will do a bulk insert by creating a sql insert statement that
has multiple values: INSERT INTO (..) VALUES (..),(..),(..)


#### Access DB Connections

The ODBC connection to Microsoft Access databases have more restrictions. ETLBox is based .NET Core and will run in your application as dependency.
It now depends if you compile your application with 32bit or 64bit (some version of .NET Core only support 64bit). You will need
the right Microsoft Access driver installed - either 32bit or 64bit. You can only install the 32bit driver
if you have a 32bit Access installed, and vice versa. Also, make sure to set up the correct ODBC connection (again, there is 
64bit ODBC connection manager tool in windows and a 32bit). 

The corresponding 64bit ODBC driver for Access can be download 
Microsoft: [Microsoft Access Database Engine 2010 Redistributable](https://www.microsoft.com/en-us/download/details.aspx?id=13255)

To create a connection to an Access Database, use the `AccessOdbcConnectionManager` and an `OdbcConnectionString`.

```C#
DbDestination dest = DbDestination (
    new AccessOdbcConnectionManager(new OdbcConnectionString("Driver={Microsoft Access Driver (*.mdb, *.accdb)}DBQ=C:\DB\Test.mdb")),
    "DestinationTable"
);
```
*Warning*: The `DbDestination` will do a bulk insert by creating a sql statement using a sql query that Access understands. The number of rows per batch is 
very limited - if it too high, you will the error message 'Query to complex'. Try to reduce the batch size to solve this.

*Note*: Please note that the AccessOdbcConnectionManager will create a "temporary" dummy table containing one record in your database when doing the bulk insert. After completion it will delete the table again. 
This is necessary to simulate a bulk insert with Access-like Sql. 


### Connection Pooling

The implementation of all connection managers is based on Microsoft ADO.NET and makes use of the underlying 
connection pooling. [Please see here for more details of connection pooling.](https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/sql-server-connection-pooling)
This means that this actually can increase your performance, and in most scenarios you never have more 
connections open that you actually need for your application.

You don't need to explicitly open a connection. ETLBox will call the `Open()` method on a connection manager whenever
needed - where it relies on the underlying ADO.NET connection pooling that either creates a new connection 
or re-uses an existing one. Whenever the work of a component or task is done, the connection manager will return the connection back to 
the pool so that it can be reused by other components or tasks when needed.

Please note that the connection pooling only works for the same connection strings. For every connection string that differs there
is going to be a separate pool 

This behavior - returning connections back to the pool when the work is done - does work very well in a scenario 
with concurrent tasks. There may be a use-case where you don't won't to query your database in parallel and you 
want to leave the connection open, avoiding the pooling. [For this scenario you can use the `LeaveOpen` property
on the connection managers.](https://github.com/roadrunnerlenny/etlbox/issues/39)


### Table Definitions

If you pass a table name to a `DBsource` or `DbDestination` or a Sql statement to a `DbSource`, the meta data
of the table is automatically derived from that table or sql statement by ETLBox. For table or views this is done via a Sql statement that queries
system information, and for the Sql statement this is done via parsing the statement. 
If you don't want ETLBox to read this information, or if you want to provide your own meta information, 
you can pass a `TableDefinition` instead.

This could look like this:

```
var TableDefinition = new TableDefinition("tableName"
    , new List<TableColumn>() {
    new TableColumn("Id", "BIGINT", allowNulls:false,  isPrimaryKey: true, isIdentity:true)),
    new TableColumn("OtherCol", "NVARCHAR(100)", allowNulls: true)
});

var DbSource<type> = new DbSource<type>() {  
  SourceTableDefinition = TableDefinition
}
```

ETLBox will use this meta data instead to get the right column names. 