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
using System.Dynamic;
using System.IO;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CrossJoinDynamicObjectTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public CrossJoinDynamicObjectTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void DynamicObjectJoin()
        {
            //Arrange
            TwoColumnsTableFixture table1 = new TwoColumnsTableFixture(SqlConnection, "CrossJoinSource1");
            table1.InsertTestData();
            TwoColumnsTableFixture table2 = new TwoColumnsTableFixture(SqlConnection, "CrossJoinSource2");
            table2.InsertTestData();
            DbSource<ExpandoObject> source1 = new DbSource<ExpandoObject>("CrossJoinSource1", SqlConnection);
            DbSource<ExpandoObject> source2 = new DbSource<ExpandoObject>("CrossJoinSource2", SqlConnection);
            MemoryDestination dest = new MemoryDestination();

            CrossJoin crossJoin = new CrossJoin(
                (data1, data2) =>
                {
                    dynamic d1 = data1 as dynamic;
                    dynamic d2 = data1 as dynamic;
                    dynamic res = new ExpandoObject();
                    res.Val = d1.Col1 + d2.Col2;
                    return res;
                }
            );

            //Act
            source1.LinkTo(crossJoin.InMemoryTarget);
            source2.LinkTo(crossJoin.PassingTarget);
            crossJoin.LinkTo(dest);
            source1.Execute();
            source2.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(9, dest.Data.Count);
        }
    }
}
