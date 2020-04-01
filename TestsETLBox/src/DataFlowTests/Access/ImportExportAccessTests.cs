using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class ImportExportAccessTests : IDisposable
    {
        public AccessOdbcConnectionManager AccessOdbcConnection => Config.AccessOdbcConnection.ConnectionManager("DataFlow");
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");

        public ImportExportAccessTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }

        private TableDefinition RecreateAccessTestTable()
        {
            try
            {
                SqlTask.ExecuteNonQuery(AccessOdbcConnection, "Try to drop table",
                    @"DROP TABLE TestTable;");
            }
            catch { }
            TableDefinition testTable = new TableDefinition("TestTable", new List<TableColumn>() {
                new TableColumn("Field1", "NUMBER", allowNulls: true),
                new TableColumn("Field2", "CHAR", allowNulls: true)
            });
            new CreateTableTask(testTable)
            {
                ThrowErrorIfTableExists = true,
                ConnectionManager = AccessOdbcConnection
            }.Execute();
            return testTable;
        }

        //Download and configure Odbc driver for access first! This test points to access file on local path
        //Odbc driver needs to be 64bit if using 64bit .NET core and 32bit if using 32bit version of .NET Core!
        //(Visual Studio 2019 16.4 changed default behvaiour for xunit Tests - they now run with .NET Core 32bit versions
        //Driver Access >2016 https://www.microsoft.com/en-us/download/details.aspx?id=54920
        //Driver Access >2010 https://www.microsoft.com/en-us/download/details.aspx?id=13255
        [Fact]
        public void CSVIntoAccess()
        {
            //Arrange
            TableDefinition testTable = RecreateAccessTestTable();

            //Act
            CsvSource<string[]> source = new CsvSource<string[]>("res/UseCases/AccessData.csv");
            DbDestination<string[]> dest = new DbDestination<string[]>(testTable, AccessOdbcConnection, batchSize: 2);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(AccessOdbcConnection, testTable.Name));
        }

        public class Data
        {
            [ColumnMap("Col1")]
            public Double Field1 { get; set; }
            public string Field2 { get; set; }
            [ColumnMap("Col2")]
            public string Field2Trimmed => Field2.Trim();
        }

        [Fact]
        public void AccessIntoDBWithTableDefinition()
        {
            //Arrange
            TableDefinition testTable = RecreateAccessTestTable();
            InsertTestData();
            TwoColumnsTableFixture destTable = new TwoColumnsTableFixture(SqlConnection, "dbo.AccessTargetTableWTD");

            //Act
            DbSource<Data> source = new DbSource<Data>(testTable, AccessOdbcConnection);
            DbDestination<Data> dest = new DbDestination<Data>("dbo.AccessTargetTableWTD", SqlConnection);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            destTable.AssertTestData();
        }

        private void InsertTestData()
        {
            SqlTask.ExecuteNonQuery(AccessOdbcConnection, "Insert test data",
                "INSERT INTO TestTable (Field1, Field2) VALUES (1,'Test1');");
            SqlTask.ExecuteNonQuery(AccessOdbcConnection, "Insert test data",
                "INSERT INTO TestTable (Field1, Field2) VALUES (2,'Test2');");
            SqlTask.ExecuteNonQuery(AccessOdbcConnection, "Insert test data",
                "INSERT INTO TestTable (Field1, Field2) VALUES (3,'Test3');");
        }

        [Fact]
        public void AccessIntoDB()
        {
            //Arrange
            TableDefinition testTable = RecreateAccessTestTable();
            InsertTestData();
            TwoColumnsTableFixture destTable = new TwoColumnsTableFixture(SqlConnection, "dbo.AccessTargetTable");

            //Act
            DbSource<Data> source = new DbSource<Data>("TestTable", AccessOdbcConnection);
            DbDestination<Data> dest = new DbDestination<Data>("dbo.AccessTargetTable", SqlConnection);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            destTable.AssertTestData();
        }


    }
}
