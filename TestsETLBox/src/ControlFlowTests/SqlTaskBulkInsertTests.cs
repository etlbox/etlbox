using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class SqlTaskBulkInsertTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("ControlFlow");
        public static IEnumerable<object[]> ConnectionsWithValue(int value)
            => Config.AllSqlConnectionsWithValue("ControlFlow", value);
        public static IEnumerable<object[]> Access => Config.AccessConnection("ControlFlow");

        public SqlTaskBulkInsertTests(ControlFlowDatabaseFixture dbFixture)
        { }


        [Theory, MemberData(nameof(Connections))
               , MemberData(nameof(Access))]  //If access fails with "Internal OLE Automation error", download and install: https://www.microsoft.com/en-us/download/confirmation.aspx?id=50040
                                              //see also: https://stackoverflow.com/questions/54632928/internal-ole-automation-error-in-ms-access-using-oledb

        public void StringArray(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture destTable = new TwoColumnsTableFixture(connection, "BulkInsert2Columns");

            TableData<string[]> data = new TableData<string[]>(
                destTable.TableDefinition,
                new List<object[]>
                {
                    new []{ "1", "Test1" },
                    new []{ "2", "Test2" },
                    new []{ "3", "Test3" }
                }
                );

            //Act
            SqlTask.BulkInsert(connection, "Bulk insert demo data", data, "BulkInsert2Columns");

            //Assert
            destTable.AssertTestData();
        }

        [Theory, MemberData(nameof(ConnectionsWithValue), 0),
        MemberData(nameof(ConnectionsWithValue), 2),
        MemberData(nameof(ConnectionsWithValue), 3)]
        public void WithIdentityShift(IConnectionManager connection, int identityIndex)
        {
            //SQLite does not support Batch Insert on Non Nullable Identity Columns
            if (connection.GetType() != typeof(SQLiteConnectionManager))
            {
                //Arrange
                FourColumnsTableFixture destTable = new FourColumnsTableFixture(connection, "BulkInsert4Columns", identityIndex);

                TableData data = new TableData(
                    destTable.TableDefinition,
                    new List<object[]>
                    {
                        new object[]{ "Test1", null, 1.2 },
                        new object[]{ "Test2", 4711, 1.23 },
                        new object[]{ "Test3", 185, 1.234 }
                    });

                //Act
                SqlTask.BulkInsert(connection, "Bulk insert demo data", data, "BulkInsert4Columns");

                //Assert
                destTable.AssertTestData();
            }
        }

    }
}
