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
    public class LookupStringArrayTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public LookupStringArrayTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Theory, MemberData(nameof(Connections))]
        public void SimpleLookupWithoutObject(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(connection,"SourceNonGenericLookup");
            source2Columns.InsertTestData();
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture(connection,"DestinationNonGenericLookup", -1);
            FourColumnsTableFixture lookup4Columns = new FourColumnsTableFixture(connection,"LookupNonGeneric");
            lookup4Columns.InsertTestData();

            DbSource<string[]> source = new DbSource<string[]>("SourceNonGenericLookup", connection);
            DbDestination<string[]> dest = new DbDestination<string[]>("DestinationNonGenericLookup", connection);

            //Act
            List<string[]> lookupList = new List<string[]>();

            DbSource<string[]> lookupSource = new DbSource<string[]>("LookupNonGeneric", connection);
            LookupTransformation<string[], string[]> lookup = new LookupTransformation<string[], string[]> (
                lookupSource,
                row =>
                {
                    Array.Resize(ref row, 4);
                    row[2] = lookupList.Where(lkupRow => lkupRow[0] == row[0]).Select(lkupRow => lkupRow[2]).FirstOrDefault();
                    row[3] = lookupList.Where(lkupRow => lkupRow[0] == row[0]).Select(lkupRow => lkupRow[3]).FirstOrDefault();
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
