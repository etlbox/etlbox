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
    [Collection("DataFlow Source and Destination")]
    public class DbDestinationDifferentDBTests
    {
        public static IEnumerable<object[]> MixedSourceDestinations() => new[] {
            //Same DB
            new object[] {
                (IConnectionManager)Config.SqlConnection.ConnectionManager("DataFlowSource"),
                (IConnectionManager)Config.SqlConnection.ConnectionManager("DataFlowDestination")
            },
            new object[] {
                (IConnectionManager)Config.SQLiteConnection.ConnectionManager("DataFlowSource"),
                (IConnectionManager)Config.SQLiteConnection.ConnectionManager("DataFlowDestination")
            },
            new object[] {
                (IConnectionManager)Config.MySqlConnection.ConnectionManager("DataFlowSource"),
                (IConnectionManager)Config.MySqlConnection.ConnectionManager("DataFlowDestination")
            },
            new object[] {
                (IConnectionManager)Config.PostgresConnection.ConnectionManager("DataFlowSource"),
                (IConnectionManager)Config.PostgresConnection.ConnectionManager("DataFlowDestination")
            },
            //Mixed
            new object[] {
                (IConnectionManager)Config.SqlConnection.ConnectionManager("DataFlowSource"),
                (IConnectionManager)Config.SQLiteConnection.ConnectionManager("DataFlowDestination")
            },
             new object[] {
                (IConnectionManager)Config.SQLiteConnection.ConnectionManager("DataFlowSource"),
                (IConnectionManager)Config.SqlConnection.ConnectionManager("DataFlowDestination")
            },
            new object[] {
                (IConnectionManager)Config.MySqlConnection.ConnectionManager("DataFlowSource"),
                (IConnectionManager)Config.PostgresConnection.ConnectionManager("DataFlowDestination")
            },
                new object[] {
                (IConnectionManager)Config.SqlConnection.ConnectionManager("DataFlowSource"),
                (IConnectionManager)Config.PostgresConnection.ConnectionManager("DataFlowDestination")
            }

        };

        public DbDestinationDifferentDBTests(DatabaseSourceDestinationFixture dbFixture)
        {
        }

        [Theory, MemberData(nameof(MixedSourceDestinations))]
        public void TestTransferBetweenDBs(IConnectionManager sourceConnection, IConnectionManager destConnection)
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(sourceConnection, "Source");
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(destConnection, "Destination");

            //Act
            DbSource<string[]> source = new DbSource<string[]>("Source", sourceConnection);
            DbDestination<string[]> dest = new DbDestination<string[]>("Destination", destConnection);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
