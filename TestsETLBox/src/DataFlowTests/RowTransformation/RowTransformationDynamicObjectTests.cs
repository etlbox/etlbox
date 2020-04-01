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
    public class RowTransformationDynamicObjectTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public RowTransformationDynamicObjectTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void ConvertIntoObject()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("DestinationRowTransformationDynamic");
            CsvSource<ExpandoObject> source = new CsvSource<ExpandoObject>("res/RowTransformation/TwoColumns.csv");

            //Act
            RowTransformation<ExpandoObject> trans = new RowTransformation<ExpandoObject>(
                csvdata =>
                {
                    dynamic c = csvdata as ExpandoObject;
                    c.Col1 = c.Header1;
                    c.Col2 = c.Header2;
                    return c;
                });
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>("DestinationRowTransformationDynamic", Connection);
            source.LinkTo(trans);
            trans.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
