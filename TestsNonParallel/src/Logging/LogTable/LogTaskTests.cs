using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ALE.ETLBoxTests.Logging
{
    [Collection("Logging")]
    public class LogTaskTests : IDisposable
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("Logging");
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("Logging");

        public LogTaskTests(LoggingDatabaseFixture dbFixture)
        {

        }

        public void Dispose()
        {
            ControlFlow.ClearSettings();
        }

        [Theory, MemberData(nameof(Connections))]
        public void CreateLogTable(IConnectionManager connection)
        {
            //Arrange
            //Act
            CreateLogTableTask.Create(connection, "etlbox_testlog");

            //Assert
            IfTableOrViewExistsTask.IsExisting(connection, "etlbox_testlog");
            var td = TableDefinition.FromName(connection, "etlbox_testlog");
            Assert.True(td.Columns.Count == 10);
            //Cleanup
            DropTableTask.Drop(connection, "etlbox_testlog");
        }


        [Theory, MemberData(nameof(Connections))]
        public void TestErrorLogging(IConnectionManager connection)
        {
            //Arrange
            CreateLogTableTask.Create(connection, "test_log");
            ControlFlow.AddLoggingDatabaseToConfig(connection,NLog.LogLevel.Trace, "test_log");
            //Act
            LogTask.Error(connection, "Error!");
            LogTask.Warn(connection, "Warn!");
            LogTask.Info(connection, "Info!");
            LogTask.Debug(connection, "Debug!");
            LogTask.Trace(connection, "Trace!");
            LogTask.Fatal(connection, "Fatal!");
            //Assert
            Assert.Equal(1, RowCountTask.Count(connection, ControlFlow.LogTable,
                "message = 'Error!' AND level = 'Error' and task_action = 'LOG'"));
            Assert.Equal(1, RowCountTask.Count(connection, ControlFlow.LogTable,
                "message = 'Warn!' AND level = 'Warn' and task_action = 'LOG'"));
            Assert.Equal(1, RowCountTask.Count(connection, ControlFlow.LogTable,
                "message = 'Info!' AND level = 'Info' and task_action = 'LOG'"));
            Assert.Equal(1, RowCountTask.Count(connection, ControlFlow.LogTable,
                "message = 'Debug!' AND level = 'Debug' and task_action = 'LOG'"));
            Assert.Equal(1, RowCountTask.Count(connection, ControlFlow.LogTable,
                "message = 'Trace!' AND level = 'Trace' and task_action = 'LOG'"));
            Assert.Equal(1, RowCountTask.Count(connection, ControlFlow.LogTable,
                "message = 'Fatal!' AND level = 'Fatal' and task_action = 'LOG'"));

            //Cleanup
            DropTableTask.Drop(connection, ControlFlow.LogTable);
        }

        [Fact]
        public void IsControlFlowStageLogged()
        {
            //Arrange
            CreateLogTableTask.Create(SqlConnection, "test_log_stage");
            ControlFlow.AddLoggingDatabaseToConfig(SqlConnection, NLog.LogLevel.Debug, "test_log_stage");

            //Act
            ControlFlow.STAGE = "SETUP";
            SqlTask.ExecuteNonQuery(SqlConnection, "Test Task", "Select 1 as test");

            //Assert
            Assert.Equal(2, new RowCountTask("test_log_stage",
                           $"message='Test Task' and stage = 'SETUP'")
            {
                DisableLogging = true,
                ConnectionManager = SqlConnection
            }.Count().Rows);

            //Cleanup
            DropTableTask.Drop(SqlConnection, ControlFlow.LogTable);
        }

        [Theory, MemberData(nameof(Connections))]
        public void TestReadLogTask(IConnectionManager connection)
        {
            //Arrange
            CreateLogTableTask.Create(connection, "test_readlog");
            ControlFlow.AddLoggingDatabaseToConfig(connection, NLog.LogLevel.Info, "test_readlog");
            SqlTask.ExecuteNonQuery(connection, "Test Task", "Select 1 as test");

            //Act
            List<LogEntry> entries = ReadLogTableTask.Read(connection);

            //Assert
            Assert.Collection<LogEntry>(entries,
                 l => Assert.True(l.Message == "Test Task" && l.TaskAction == "START" && l.TaskType == "SqlTask"),
                 l => Assert.True(l.Message == "Test Task" && l.TaskAction == "END" && l.TaskType == "SqlTask")
                 ) ;

            //Cleanup
            DropTableTask.Drop(connection, ControlFlow.LogTable);
        }
    }
}
