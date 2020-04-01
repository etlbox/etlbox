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
    public class LookupErrorLinkingTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public LookupErrorLinkingTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MyLookupRow
        {
            [ColumnMap("Col1")]
            public int Key { get; set; }
            [ColumnMap("Col2")]
            public string LookupValue { get; set; }
        }

        public class MyInputDataRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void NoErrorLinking()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(SqlConnection, "LookupErrorLinkingDest");
            CreateSourceTable(SqlConnection, "LookupErrorLinkingSource");
            DbSource<MyLookupRow> lookupSource = new DbSource<MyLookupRow>("LookupErrorLinkingSource", SqlConnection);

            MemorySource<MyInputDataRow> source = new MemorySource<MyInputDataRow>();
            source.Data = new List<MyInputDataRow>() {
                new MyInputDataRow() { Col1 = 1 },
                 new MyInputDataRow() { Col1 = 2 },
                  new MyInputDataRow() { Col1 = 3 }
            };

            //Act & Assert
            Assert.ThrowsAny<Exception>(() =>
            {
                List<MyLookupRow> LookupTableData = new List<MyLookupRow>();
                LookupTransformation<MyInputDataRow, MyLookupRow> lookup = new LookupTransformation<MyInputDataRow, MyLookupRow>(
                    lookupSource,
                    row =>
                    {
                        row.Col2 = LookupTableData.Where(ld => ld.Key == row.Col1).Select(ld => ld.LookupValue).FirstOrDefault();
                        return row;
                    }
                    , LookupTableData
                );
                DbDestination<MyInputDataRow> dest = new DbDestination<MyInputDataRow>("LookupErrorLinkingDest", SqlConnection);
                source.LinkTo(lookup);
                lookup.LinkTo(dest);
                source.Execute();
                dest.Wait();
            });
        }

        [Fact]
        public void WithObject()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(SqlConnection, "LookupErrorLinkingDest");
            CreateSourceTable(SqlConnection, "LookupErrorLinkingSource");
            DbSource<MyLookupRow> lookupSource = new DbSource<MyLookupRow>("LookupErrorLinkingSource", SqlConnection);

            MemorySource<MyInputDataRow> source = new MemorySource<MyInputDataRow>();
            source.Data = new List<MyInputDataRow>() {
                new MyInputDataRow() { Col1 = 1 },
                 new MyInputDataRow() { Col1 = 2 },
                  new MyInputDataRow() { Col1 = 3 },
                  new MyInputDataRow() { Col1 = 4 }
            };
            MemoryDestination<ETLBoxError> errorDest = new MemoryDestination<ETLBoxError>();

            //Act
            List<MyLookupRow> LookupTableData = new List<MyLookupRow>();
            LookupTransformation<MyInputDataRow, MyLookupRow> lookup = new LookupTransformation<MyInputDataRow, MyLookupRow>(
                 lookupSource,
                row =>
                {
                    row.Col2 = LookupTableData.Where(ld => ld.Key == row.Col1).Select(ld => ld.LookupValue).FirstOrDefault();
                    if (row.Col1 == 4) throw new Exception("Error record");
                    return row;
                }
                , LookupTableData
            );
            DbDestination<MyInputDataRow> dest = new DbDestination<MyInputDataRow>("LookupErrorLinkingDest", SqlConnection);
            source.LinkTo(lookup);
            lookup.LinkTo(dest);
            lookup.LinkLookupSourceErrorTo(errorDest);
            lookup.LinkLookupTransformationErrorTo(errorDest);
            source.Execute();
            dest.Wait();
            errorDest.Wait();

            //Assert
            dest2Columns.AssertTestData();
            Assert.Collection<ETLBoxError>(errorDest.Data,
                d => Assert.True(!string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)),
                d => Assert.True(!string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText))
            );
        }

        private static void CreateSourceTable(IConnectionManager connection, string tableName)
        {
            DropTableTask.DropIfExists(connection, tableName);

            var TableDefinition = new TableDefinition(tableName
                , new List<TableColumn>() {
                new TableColumn("Col1", "VARCHAR(100)", allowNulls: true),
                new TableColumn("Col2", "VARCHAR(100)", allowNulls: true)
            });
            TableDefinition.CreateTable(connection);
            ObjectNameDescriptor TN = new ObjectNameDescriptor(tableName, connection);
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
              , $@"INSERT INTO {TN.QuotatedFullName} VALUES('1','Test1')");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES('2','Test2')");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES('X','Test3')");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES('3','Test3')");
        }


    }
}
