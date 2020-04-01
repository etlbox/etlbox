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
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class BlockTransformationDynamicObjectTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public BlockTransformationDynamicObjectTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void ModifyInputDataList()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("BlockTransSourceDynamic");
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("BlockTransDestDynamic");

            DbSource<ExpandoObject> source = new DbSource<ExpandoObject>("BlockTransSourceDynamic", SqlConnection);
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>("BlockTransDestDynamic", SqlConnection);

            //Act
            BlockTransformation<ExpandoObject> block = new BlockTransformation<ExpandoObject>(
                inputData => {
                    inputData.RemoveRange(1, 2);
                    dynamic nr = new ExpandoObject();
                    nr.Col1 = 4;
                    nr.Col2 = "Test4";
                    inputData.Add(nr);
                    return inputData;
                });
            source.LinkTo(block);
            block.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(2, RowCountTask.Count(SqlConnection, "BlockTransDestDynamic"));
            Assert.Equal(1, RowCountTask.Count(SqlConnection, "BlockTransDestDynamic", "Col1 = 1 AND Col2='Test1'"));
            Assert.Equal(1, RowCountTask.Count(SqlConnection, "BlockTransDestDynamic", "Col1 = 4 AND Col2='Test4'"));
        }
    }
}
