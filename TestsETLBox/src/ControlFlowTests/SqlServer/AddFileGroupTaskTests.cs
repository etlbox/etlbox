using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.ControlFlow.SqlServer;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests.SqlServer
{
    [Collection("ControlFlow")]
    public class AddFileGroupTaskTests
    {
        public AddFileGroupTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("ControlFlow");
        public string DBName => Config.SqlConnection.ConnectionString("ControlFlow").DbName;

        [Fact]
        public void AddFileGroup()
        {
            //Arrange
            string fgName = HashHelper.RandomString(10) + "_FG";
            Assert.Equal(0, RowCountTask.Count(Connection, "sys.filegroups", $"name = '{fgName}'"));

            //Act
            AddFileGroupTask.AddFileGroup(Connection, fgName, DBName, "2048KB", "2048KB", false);

            //Assert
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.filegroups", $"name = '{fgName}'"));
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.sysfiles", $"name = '{fgName}'"));
        }

        [Fact]
        public void AddDefaultFileGroup()
        {
            //Arrange
            string fgName = HashHelper.RandomString(10) + "_FG";
            Assert.Equal(0, RowCountTask.Count(Connection, "sys.filegroups", $"name = '{fgName}' AND is_default = 1"));

            //Act
            AddFileGroupTask.AddFileGroup(Connection, fgName, DBName, "5MB", "5MB", true);

            //Assert
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.filegroups", $"name = '{fgName}' AND is_default = 1"));
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.sysfiles", $"name = '{fgName}'"));
        }

        [Fact]
        public void NotSupportedWithOtherDBs()
        {
            Assert.Throws<ETLBoxNotSupportedException>(
                () => AddFileGroupTask.AddFileGroup(Config.SQLiteConnection.ConnectionManager("ControlFlow"), "Test", "Test", "5MB", "5MB", true)
                );
        }
    }
}
