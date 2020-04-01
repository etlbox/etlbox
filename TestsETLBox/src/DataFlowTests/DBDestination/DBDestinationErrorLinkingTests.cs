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
    public class DbDestinationErrorLinkingTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");
        public DbDestinationErrorLinkingTests(DataFlowDatabaseFixture dbFixture)
        {

        }

        public class MySimpleRow
        {
            public string Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Theory, MemberData(nameof(Connections))]
        public void RedirectBatch(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(connection, "DestLinkError");
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
            source.Data = new List<MySimpleRow>()
            {
                new MySimpleRow() { Col1 = null, Col2 = "ErrorRecord"},
                new MySimpleRow() { Col1 = "X2", Col2 = "ErrorRecord"},
                new MySimpleRow() { Col1 = "1", Col2 = "Test1"},
                new MySimpleRow() { Col1 = "2", Col2 = "Test2"},
                new MySimpleRow() { Col1 = "3", Col2 = "Test3 - good, but in error batch"},
                new MySimpleRow() { Col1 = null, Col2 = "ErrorRecord"},
                new MySimpleRow() { Col1 = "3", Col2 = "Test3"},
            };
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>("DestLinkError", connection, batchSize: 2);
            MemoryDestination<ETLBoxError> errorDest = new MemoryDestination<ETLBoxError>();

            //Act
            source.LinkTo(dest);
            dest.LinkErrorTo(errorDest);
            source.Execute();
            dest.Wait();
            errorDest.Wait();

            //Assert
            d2c.AssertTestData();
            Assert.Collection<ETLBoxError>(errorDest.Data,
                d => Assert.True(!string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)),
                 d => Assert.True(!string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText))
            );
        }
    }
}
