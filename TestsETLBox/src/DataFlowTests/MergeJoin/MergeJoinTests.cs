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
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class MergeJoinTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public MergeJoinTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void MergeJoinUsingOneObject()
        {
            //Arrange
            TwoColumnsTableFixture source1Table = new TwoColumnsTableFixture("MergeJoinSource1");
            source1Table.InsertTestData();
            TwoColumnsTableFixture source2Table = new TwoColumnsTableFixture("MergeJoinSource2");
            source2Table.InsertTestDataSet2();
            TwoColumnsTableFixture destTable = new TwoColumnsTableFixture("MergeJoinDestination");

            DbSource<MySimpleRow> source1 = new DbSource<MySimpleRow>("MergeJoinSource1", Connection);
            DbSource<MySimpleRow> source2 = new DbSource<MySimpleRow>("MergeJoinSource2", Connection);
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>("MergeJoinDestination", Connection);

            //Act
            MergeJoin<MySimpleRow, MySimpleRow, MySimpleRow> join = new MergeJoin<MySimpleRow, MySimpleRow, MySimpleRow>(
                (inputRow1, inputRow2) => {
                    inputRow1.Col1 += inputRow2.Col1;
                    inputRow1.Col2 += inputRow2.Col2;
                    return inputRow1;
                });
            source1.LinkTo(join.Target1);
            source2.LinkTo(join.Target2);
            join.LinkTo(dest);
            source1.Execute();
            source2.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(Connection, "MergeJoinDestination"));
            Assert.Equal(1, RowCountTask.Count(Connection, "MergeJoinDestination", "Col1 = 5 AND Col2='Test1Test4'"));
            Assert.Equal(1, RowCountTask.Count(Connection, "MergeJoinDestination", "Col1 = 7 AND Col2='Test2Test5'"));
            Assert.Equal(1, RowCountTask.Count(Connection, "MergeJoinDestination", "Col1 = 9 AND Col2='Test3Test6'"));
        }

    }
}
