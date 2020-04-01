using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using CsvHelper.Configuration;
using CsvHelper.Configuration.Attributes;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CsvDestinationConfigurationTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public CsvDestinationConfigurationTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            [Index(1)]
            public int Col1 { get; set; }
            [Index(2)]
            public string Col2 { get; set; }

        }

        [Fact]
        public void DisableHeader()
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture("CsvSourceNoHeader");
            s2c.InsertTestData();
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>("CsvSourceNoHeader", SqlConnection);

            //Act
            CsvDestination<MySimpleRow> dest = new CsvDestination<MySimpleRow>("./ConfigurationNoHeader.csv");
            dest.Configuration.HasHeaderRecord = false;
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(File.ReadAllText("./ConfigurationNoHeader.csv"),
                File.ReadAllText("res/CsvDestination/TwoColumnsNoHeader.csv"));
        }


    }
}
