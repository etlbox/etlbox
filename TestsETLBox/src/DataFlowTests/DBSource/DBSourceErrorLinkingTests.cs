using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DbSourceErrorLinkingTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public DbSourceErrorLinkingTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
            [ColumnMap("Col3")]
            public int AddCol { get; set; }
        }

        [Theory, MemberData(nameof(Connections))]
        public void RedirectErrorWithObject(IConnectionManager connection)
        {
            if (connection.GetType() == typeof(SQLiteConnectionManager))
                Task.Delay(100).Wait(); //Database was locked and needs to recover after exception

            //Arrange
            CreateSourceTable(connection, "DbSourceErrorLinking");
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(connection, "DbDestinationErrorLinking");

            //Act
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>("DbSourceErrorLinking", connection);
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>("DbDestinationErrorLinking", connection);
            MemoryDestination<ETLBoxError> errorDest = new MemoryDestination<ETLBoxError>();
            source.LinkTo(dest);
            source.LinkErrorTo(errorDest);
            source.Execute();
            dest.Wait();
            errorDest.Wait();

            //Assert
            dest2Columns.AssertTestData();
            Assert.Collection<ETLBoxError>(errorDest.Data,
                d => Assert.True(!string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)),
                d => Assert.True(!string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)),
                 d => Assert.True(!string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText))
            );
        }

        [Theory, MemberData(nameof(Connections))]
        public void WithoutErrorLinking(IConnectionManager connection)
        {
            //Arrange
            CreateSourceTable(connection, "DbSourceNoErrorLinking");

            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(connection, "DbDestinationNoErrorLinking");

            //Act
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>("DbSourceNoErrorLinking", connection);
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>("DbDestinationNoErrorLinking", connection);
            source.LinkTo(dest);

            //Assert
            Assert.Throws<System.FormatException>(() =>
               {
                   source.Execute();
                   dest.Wait();
               });
        }

        private static void CreateSourceTable(IConnectionManager connection, string tableName)
        {
            DropTableTask.DropIfExists(connection, tableName);

            var TableDefinition = new TableDefinition(tableName
                , new List<TableColumn>() {
                new TableColumn("Col1", "VARCHAR(100)", allowNulls: true),
                new TableColumn("Col2", "VARCHAR(100)", allowNulls: true),
                new TableColumn("Col3", "VARCHAR(100)", allowNulls: true)
            });
            TableDefinition.CreateTable(connection);
            ObjectNameDescriptor TN = new ObjectNameDescriptor(tableName, connection);
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
              , $@"INSERT INTO {TN.QuotatedFullName} VALUES('1','Test1','1')");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES('1.35','TestX','X')");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES('2','Test2', NULL)");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES('X',NULL, NULL)");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES('3','Test3', '3')");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES('4','Test4', 'X')");
        }




    }
}
