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
    public class DbDestinationBatchChangesTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public DbDestinationBatchChangesTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Theory, MemberData(nameof(Connections))]
        public void WithBatchChanges(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(connection, "DbDestinationBatchChanges");
            DbDestination<string[]> dest = new DbDestination<string[]>("DbDestinationBatchChanges", connection, batchSize: 2)
            {
                BeforeBatchWrite = rowArray =>
                                   {
                                       rowArray[0][1] = "NewValue";
                                       return rowArray;
                                   }
            };

            //Act
            CsvSource<string[]> source = new CsvSource<string[]>("res/CsvSource/TwoColumns.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(connection, "DbDestinationBatchChanges"));
            Assert.Equal(2, RowCountTask.Count(connection, "DbDestinationBatchChanges", $"{d2c.QB}Col2{d2c.QE}='NewValue'"));
            Assert.Equal(1, RowCountTask.Count(connection, "DbDestinationBatchChanges", $"{d2c.QB}Col1{d2c.QE} = 2 AND {d2c.QB}Col2{d2c.QE}='Test2'"));
        }
    }
}
