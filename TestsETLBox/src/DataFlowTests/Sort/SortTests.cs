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
    public class SortTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public SortTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void SortSimpleDataDescending()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("SortSource");
            source2Columns.InsertTestData();
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>("SortSource", Connection);

            //Act
            List<MySimpleRow> actual = new List<MySimpleRow>();
            CustomDestination<MySimpleRow> dest = new CustomDestination<MySimpleRow>(
                row => actual.Add(row)
            );
            Comparison<MySimpleRow> comp = new Comparison<MySimpleRow>(
                   (x, y) => y.Col1 - x.Col1
                );
            Sort<MySimpleRow> block = new Sort<MySimpleRow>(comp);
            source.LinkTo(block);
            block.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            List<int> expected = new List<int>() { 3, 2, 1 };
            Assert.Equal(expected, actual.Select(row => row.Col1).ToList()) ;
        }
    }
}
