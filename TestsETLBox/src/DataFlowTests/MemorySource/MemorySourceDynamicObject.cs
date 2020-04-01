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
    public class MemorySourceDynamicObjectTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public MemorySourceDynamicObjectTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void DataIsFromList()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("MemoryDestination");
            MemorySource<ExpandoObject> source = new MemorySource<ExpandoObject>();
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>("MemoryDestination", SqlConnection);
            AddObjectsToSource(source);

            //Act
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        private static void AddObjectsToSource(MemorySource<ExpandoObject> source)
        {
            source.Data = new List<ExpandoObject>();
            dynamic item1 = new ExpandoObject();
            item1.Col2 = "Test1";
            item1.Col1 = 1;
            dynamic item2 = new ExpandoObject();
            item2.Col2 = "Test2";
            item2.Col1 = 2;
            dynamic item3 = new ExpandoObject();
            item3.Col2 = "Test3";
            item3.Col1 = 3;
            source.Data.Add(item1);
            source.Data.Add(item2);
            source.Data.Add(item3);
        }
    }
}
