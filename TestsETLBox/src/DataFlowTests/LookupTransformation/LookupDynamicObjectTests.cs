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
    public class LookupDynamicObjectTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public LookupDynamicObjectTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Theory, MemberData(nameof(Connections))]
        public void SimpleLookupWithDynamicObject(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(connection,"SourceLookupDynamicObject");
            source2Columns.InsertTestData();
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture(connection,"DestinationLookupDynamicObject", -1);


            DbSource<ExpandoObject> source = new DbSource<ExpandoObject>("SourceLookupDynamicObject", connection);
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>("DestinationLookupDynamicObject", connection);

            //Act
            List<ExpandoObject> lookupList = new List<ExpandoObject>();

            CsvSource<ExpandoObject> lookupSource = new CsvSource<ExpandoObject>("res/Lookup/LookupSource.csv");

            var lookup = new ETLBox.DataFlow.LookupTransformation<ExpandoObject, ExpandoObject>(
                lookupSource,
                row =>
                {
                    dynamic r = row as ExpandoObject;
                    r.Col3 = lookupList
                            .Where(lkupRow => { dynamic lk = lkupRow as dynamic;  return int.Parse(lk.Key) == r.Col1; })
                            .Select(lkupRow => { dynamic lk = lkupRow as dynamic;
                                return lk.Column3 == string.Empty ? null : Int64.Parse(lk.Column3); })
                            .FirstOrDefault();
                    r.Col4 = lookupList
                            .Where(lkupRow => { dynamic lk = lkupRow as dynamic; return int.Parse(lk.Key) == r.Col1; })
                            .Select(lkupRow => { dynamic lk = lkupRow as dynamic; return double.Parse(lk.Column4); })
                            .FirstOrDefault();
                     return row;
                },
                lookupList
            );

            source.LinkTo(lookup);
            lookup.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest4Columns.AssertTestData();
        }
    }
}
