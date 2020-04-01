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
    public class DbDestinationNullHandlingTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public DbDestinationNullHandlingTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void IgnoreWithObject()
        {
            //Arrange
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(SqlConnection, "DestIgnoreNullValues");
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
            source.Data = new List<MySimpleRow>()
            {
                null,
                new MySimpleRow() { Col1 = 1, Col2 = "Test1"},
                null,
                new MySimpleRow() { Col1 = 2, Col2 = "Test2"},
                new MySimpleRow() { Col1 = 3, Col2 = "Test3"},
                null
            };
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>("DestIgnoreNullValues", SqlConnection);


            //Act
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d2c.AssertTestData();
        }

        [Fact]
        public void IgnoreWithStringArray()
        {
            //Arrange
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(SqlConnection, "DestIgnoreNullValuesStringArray");
            MemorySource<string[]> source = new MemorySource<string[]>();
            source.Data = new List<string[]>()
            {
                null ,
                new string[] { "1", "Test1"},
                null,
                new string[] { "2", "Test2"},
                new string[] { "3", "Test3"},
                null
            };
            DbDestination<string[]> dest = new DbDestination<string[]>("DestIgnoreNullValuesStringArray", SqlConnection);


            //Act
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d2c.AssertTestData();
        }
    }
}
