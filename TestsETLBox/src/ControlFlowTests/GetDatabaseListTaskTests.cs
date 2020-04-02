using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class GetDatabaseListTaskTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("ControlFlow");
        public string DBName => Config.SqlConnection.ConnectionString("ControlFlow").DbName;

        public GetDatabaseListTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }


        [Fact]
        public void GetDatabaseList()
        {
            //Arrange

            //Act
            List<string> allDatabases = GetDatabaseListTask.List(SqlConnection);

            //Assert
            Assert.True(allDatabases.Count >= 1);
            Assert.Contains(allDatabases, name => name == DBName);

        }

        public SQLiteConnectionManager SQLiteConnection => Config.SQLiteConnection.ConnectionManager("ControlFlow");

        [Fact]
        public void NotSupportedWithSQLite()
        {
            Assert.Throws<ETLBoxNotSupportedException>(
                () => GetDatabaseListTask.List(SQLiteConnection)
                );
        }
    }
}
