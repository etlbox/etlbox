using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class CreateDatabaseTaskTests
    {
        public SqlConnectionManager SqlMasterConnection => new SqlConnectionManager(Config.SqlConnection.ConnectionString("ControlFlow").CloneWithMasterDbName());
        public static IEnumerable<object[]> SqlConnectionsWithMaster() => new[] {
                    new object[] { (IConnectionManager)new SqlConnectionManager(Config.SqlConnection.ConnectionString("ControlFlow").CloneWithMasterDbName()) },
                    new object[] { (IConnectionManager)new PostgresConnectionManager(Config.PostgresConnection.ConnectionString("ControlFlow").CloneWithMasterDbName()) },
                    new object[] { (IConnectionManager)new MySqlConnectionManager(Config.MySqlConnection.ConnectionString("ControlFlow").CloneWithMasterDbName()) },
        };
        public CreateDatabaseTaskTests()
        { }

        [Theory, MemberData(nameof(SqlConnectionsWithMaster))]
        public void CreateSimple(IConnectionManager connection)
        {
            //Arrange
            string dbName = "ETLBox_"+HashHelper.RandomString(10);
            var dbListBefore = GetDatabaseListTask.List(connection);
            Assert.DoesNotContain<string>(dbName, dbListBefore);

            //Act
            CreateDatabaseTask.Create(connection, dbName);

            //Assert
            var dbListAfter = GetDatabaseListTask.List(connection);
            Assert.Contains<string>(dbName, dbListAfter);

            //Cleanup
            DropDatabaseTask.Drop(connection, dbName);
        }

        [Theory, MemberData(nameof(SqlConnectionsWithMaster))]
        public void CreateWithCollation(IConnectionManager connection)
        {
            //Arrange
            string dbName = "ETLBox_" + HashHelper.RandomString(10);
            string collation = "Latin1_General_CS_AS";
            if (connection.GetType() == typeof(PostgresConnectionManager))
                collation = "en_US.utf8";
            if (connection.GetType() == typeof(MySqlConnectionManager))
                collation = "latin1_swedish_ci";
            //Act
            CreateDatabaseTask.Create(connection, dbName,collation );

            //Assert
            var dbList = GetDatabaseListTask.List(connection);
            Assert.Contains<string>(dbName, dbList);

            //Cleanup
            DropDatabaseTask.Drop(connection, dbName);
        }

        [Fact]
        public void NotSupportedWithSQLite()
        {
            Assert.Throws<ETLBoxNotSupportedException>(
                () => CreateDatabaseTask.Create(Config.SQLiteConnection.ConnectionManager("ControlFlow"), "Test")
                );
        }
    }
}
