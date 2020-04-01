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
    public class DbMergeDeltaTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public DbMergeDeltaTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MyMergeRow : MergeableRow
        {
            [IdColumn]
            [ColumnMap("Col1")]
            public long Key { get; set; }

            [CompareColumn]
            [ColumnMap("Col2")]
            public string Value { get; set; }

            [DeleteColumn(true)]
            public bool DeleteThisRow { get; set; }
        }

        [Theory, MemberData(nameof(Connections))]
        public void DeltaLoadWithDeletion(IConnectionManager connection)
        {
            //Arrange
            MemorySource<MyMergeRow> source = new MemorySource<MyMergeRow>();
            source.Data.Add(new MyMergeRow() { Key = 2, Value = "Test2" });
            source.Data.Add(new MyMergeRow() { Key = 3, Value = "Test3" });
            source.Data.Add(new MyMergeRow() { Key = 4, DeleteThisRow = true });
            source.Data.Add(new MyMergeRow() { Key = 10, DeleteThisRow = true });
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(connection, "DBMergeDeltaDestination");
            d2c.InsertTestDataSet3();

            //Act
            DbMerge<MyMergeRow> dest = new DbMerge<MyMergeRow>("DBMergeDeltaDestination", connection)
            {
                DeltaMode = DeltaMode.Delta
            };
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d2c.AssertTestData();
            Assert.True(dest.DeltaTable.Count == 4);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == "U" && row.Key == 2).Count() == 1);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == "I" && row.Key == 3).Count() == 1);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == "D" && row.Key == 4).Count() == 1);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == "D" && row.Key == 10).Count() == 1);
        }
    }
}
