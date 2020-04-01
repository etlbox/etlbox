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
    public class DbMergeIMergeableTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public DbMergeIMergeableTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow : IMergeableRow
        {
            [ColumnMap("Col1")]
            public long Key { get; set; }
            [ColumnMap("Col2")]
            public string Value { get; set; }
            public DateTime ChangeDate { get; set; }
            public string ChangeAction { get; set; }
            public string UniqueId => Key.ToString();
            public bool IsDeletion => false;
        }

        [Theory, MemberData(nameof(Connections))]
        public void NoIdColumn(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection,"DBMergeSource");
            s2c.InsertTestData();
            s2c.InsertTestDataSet2();
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(connection,"DBMergeDestination");
            d2c.InsertTestDataSet3();
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>("DBMergeSource", connection);

            //Act
            DbMerge<MySimpleRow> dest = new DbMerge<MySimpleRow>("DBMergeDestination", connection);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.True(dest.UseTruncateMethod == true);
            Assert.Equal(6, RowCountTask.Count(connection, "DBMergeDestination", $"{d2c.QB}Col1{d2c.QE} BETWEEN 1 AND 7 AND {d2c.QB}Col2{d2c.QE} LIKE 'Test%'"));
            Assert.True(dest.DeltaTable.Count == 7);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == "U").Count() == 3);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == "D" && row.Key == 10).Count() == 1);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == "I").Count() == 3);
        }

        [Theory, MemberData(nameof(Connections))]
        public void WithDeltaDestination(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection, "DBMergeSource");
            s2c.InsertTestData();
            s2c.InsertTestDataSet2();
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(connection, "DBMergeDestination");
            d2c.InsertTestDataSet3();
            TwoColumnsDeltaTableFixture delta2Columns = new TwoColumnsDeltaTableFixture(connection, "DBMergeDelta");

            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>("DBMergeSource", connection);

            //Act
            DbMerge<MySimpleRow> merge = new DbMerge<MySimpleRow>("DBMergeDestination", connection);
            DbDestination<MySimpleRow> delta = new DbDestination<MySimpleRow>("DBMergeDelta", connection);
            source.LinkTo(merge);
            merge.LinkTo(delta);
            source.Execute();
            merge.Wait();
            delta.Wait();

            //Assert
            Assert.True(merge.UseTruncateMethod == true);
            Assert.Equal(6, RowCountTask.Count(connection, "DBMergeDestination", $"{d2c.QB}Col1{d2c.QE} BETWEEN 1 AND 7 AND {d2c.QB}Col2{d2c.QE} LIKE 'Test%'"));
            Assert.Equal(7, RowCountTask.Count(connection, "DBMergeDelta", $"{d2c.QB}Col1{d2c.QE} BETWEEN 1 AND 10 AND {d2c.QB}Col2{d2c.QE} LIKE 'Test%'"));
            Assert.Equal(1, RowCountTask.Count(connection, "DBMergeDelta", $"{d2c.QB}ChangeAction{d2c.QE} = 'D' AND {d2c.QB}Col1{d2c.QE} = 10"));
            Assert.Equal(3, RowCountTask.Count(connection, "DBMergeDelta", $"{d2c.QB}ChangeAction{d2c.QE} = 'U' AND {d2c.QB}Col1{d2c.QE} IN (1,2,4)"));
            Assert.Equal(3, RowCountTask.Count(connection, "DBMergeDelta", $"{d2c.QB}ChangeAction{d2c.QE} = 'I' AND {d2c.QB}Col1{d2c.QE} IN (3,5,6)"));
        }
    }
}
