using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DbSourceColumnMappingTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public DbSourceColumnMappingTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class ColumnMapRow
        {
            public long Col1 { get; set; }
            [ColumnMap("Col2")]
            public string B { get; set; }

        }

        [Theory, MemberData(nameof(Connections))]
        public void ColumnMapping(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(connection, "Source");
            source2Columns.InsertTestData();

            //Act
            DbSource<ColumnMapRow> source = new DbSource<ColumnMapRow>("Source", connection);
            CustomDestination<ColumnMapRow> dest = new CustomDestination<ColumnMapRow>(
                input =>
                {
                    //Assert
                    Assert.InRange(input.Col1, 1, 3);
                    Assert.StartsWith("Test", input.B);
                });
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();
        }

        public class MyExtendedRow
        {
            [ColumnMap("Col3")]
            public long? Value { get; set; }
            [ColumnMap("Col4")]
            public decimal Percentage { get; set; }
            [ColumnMap("Col1")]
            public Int64 Id { get; set; }
            [ColumnMap("Col2")]
            public string Text { get; set; }
        }

        [Theory, MemberData(nameof(Connections))]
        public void ColumnMappingExtended(IConnectionManager connection)
        {
            //Arrange
            FourColumnsTableFixture source4Columns = new FourColumnsTableFixture(connection,
                "SourceColumnMapping", identityColumnIndex: 0);
            source4Columns.InsertTestData();

            //Act
            DbSource<MyExtendedRow> source = new DbSource<MyExtendedRow>("SourceColumnMapping", connection);
            CustomDestination<MyExtendedRow> dest = new CustomDestination<MyExtendedRow>(
                input =>
                {
                    //Assert
                    Assert.InRange(input.Id, 1, 3);
                    Assert.StartsWith("Test", input.Text);
                    if (input.Id == 1)
                        Assert.Null(input.Value);
                    else
                        Assert.True(input.Value > 0);
                    Assert.InRange(input.Percentage, 1, 2);
                });
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();
        }
    }
}
