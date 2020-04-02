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
using Microsoft.Data.SqlClient;
using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("Connection Manager")]
    public class SqlConnectionManagerTests : IDisposable
    {
        public string ConnectionStringParameter => Config.SqlConnection.RawConnectionString("ConnectionManager");
        public SqlConnectionManagerTests(ConnectionManagerFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }


        void AssertOpenConnectionCount(int allowedOpenConnections, string connectionString)
        {
            SqlConnectionString conString = new SqlConnectionString(connectionString);
            SqlConnectionManager master = new SqlConnectionManager(conString.CloneWithMasterDbName());
            string dbName = conString.Builder.InitialCatalog;
            int? openConnections =
                new SqlTask("Count open connections",
                $@"SELECT COUNT(dbid) as NumberOfConnections FROM sys.sysprocesses
                    WHERE dbid > 0 and DB_NAME(dbid) = '{dbName}'")
                { ConnectionManager = master, DisableLogging = true }
                .ExecuteScalar<int>()
                .Value;
            Assert.Equal(allowedOpenConnections, openConnections);
        }

        [Fact]
        public void TestOpeningCloseConnection()
        {
            //Arrange
            SqlConnectionManager con = new SqlConnectionManager(new SqlConnectionString(ConnectionStringParameter));

            //Act
            AssertOpenConnectionCount(0, ConnectionStringParameter);
            con.Open();
            AssertOpenConnectionCount(1, ConnectionStringParameter);
            con.Close(); //won't close any connection - ado.net will keep the connection open in it's pool in case it's needed again
            AssertOpenConnectionCount(1, ConnectionStringParameter);
            SqlConnection.ClearAllPools();

            //Assert
            AssertOpenConnectionCount(0, ConnectionStringParameter);
        }

        [Fact]
        public void TestOpeningConnectionTwice()
        {
            SqlConnectionManager con = new SqlConnectionManager(new SqlConnectionString(ConnectionStringParameter));
            AssertOpenConnectionCount(0, ConnectionStringParameter);
            con.Open();
            con.Open();
            AssertOpenConnectionCount(1, ConnectionStringParameter);
            con.Close();
            AssertOpenConnectionCount(1, ConnectionStringParameter);
            SqlConnection.ClearAllPools();
            AssertOpenConnectionCount(0, ConnectionStringParameter);
        }

        [Fact]
        public void TestOpeningConnectionsParallelOnSqlTask()
        {
            AssertOpenConnectionCount(0, ConnectionStringParameter);
            List<int> array = new List<int>() { 1, 2, 3, 4 };
            Parallel.ForEach(array, new ParallelOptions { MaxDegreeOfParallelism = 2 },
                    curNr => new SqlTask($"Test statement {curNr}", $@"
                    DECLARE @counter INT = 0;
                    CREATE TABLE dbo.test{curNr} (
                        Col1 nvarchar(50)
                    )
                    WHILE @counter <= 10000
                    BEGIN
                        SET @counter = @counter + 1;
                         INSERT INTO dbo.test{curNr}
                            values('Lorem ipsum Lorem ipsum Lorem ipsum Lorem')
                    END
            ")
                    {
                        ConnectionManager = new SqlConnectionManager(new SqlConnectionString(ConnectionStringParameter)),
                        DisableLogging = true
                    }.ExecuteNonQuery()
                 );
            AssertOpenConnectionCount(2, ConnectionStringParameter);
            SqlConnection.ClearAllPools();
            AssertOpenConnectionCount(0, ConnectionStringParameter);
        }


        [Fact]
        public void TestCloningConnection()
        {
            //Arrange
            SqlConnectionManager con = new SqlConnectionManager(ConnectionStringParameter);

            //Act
            IConnectionManager clone = con.Clone();

            //Assert
            Assert.NotEqual(clone, con);
        }
    }
}
