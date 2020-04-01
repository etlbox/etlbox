using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace ALE.ETLBoxTests.Logging
{
    [Collection("Logging")]
    public class LoadProcessTasksTests : IDisposable
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("Logging");
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("Logging");

        public LoadProcessTasksTests(LoggingDatabaseFixture dbFixture)
        {

        }

        public void Dispose()
        {
            ControlFlow.ClearSettings();
        }


        [Theory, MemberData(nameof(Connections))]
        public void CreateLoadProcessTable(IConnectionManager connection)
        {
            //Arrange
            //Act
            CreateLoadProcessTableTask.Create(connection, "etlbox_testloadprocess");

            //Assert
            IfTableOrViewExistsTask.IsExisting(connection, "etlbox_testloadprocess");
            var td = TableDefinition.FromName(connection, "etlbox_testloadprocess");
            Assert.True(td.Columns.Count == 11);

            //Cleanup
            DropTableTask.Drop(connection, "etlbox_testloadprocess");
        }

        [Theory, MemberData(nameof(Connections))]
        public void StartLoadProcess(IConnectionManager connection)
        {
            //Arrange
            CreateLoadProcessTableTask.Create(connection, "test_load_process");

            //Act
            StartLoadProcessTask.Start(connection, "Test process 1");

            //Assert
            Assert.True(ControlFlow.CurrentLoadProcess != null);
            Assert.Equal("Test process 1", ControlFlow.CurrentLoadProcess.ProcessName);
            Assert.True(ControlFlow.CurrentLoadProcess.StartDate >= DateTime.Now.AddSeconds(-1));
            Assert.Equal(1, RowCountTask.Count(connection, ControlFlow.LoadProcessTable,
                "start_message IS NULL and end_message IS NULL and abort_message IS NULL"));
            Assert.Equal(1, RowCountTask.Count(connection, ControlFlow.LoadProcessTable,
                "is_running = 1 AND was_successful=0 AND was_aborted=0"));

            //Cleanup
            DropTableTask.Drop(connection, "test_load_process");
        }

        [Theory, MemberData(nameof(Connections))]
        public void StartLoadProcessWithMessage(IConnectionManager connection)
        {
            //Arrange
            CreateLoadProcessTableTask.Create(connection, "test_lp_withmessage");

            //Act
            StartLoadProcessTask.Start(connection, "Test process 1", "Message 1", "SourceA");

            //Assert
            Assert.Equal(1, RowCountTask.Count(connection, "test_lp_withmessage",
                "start_message = 'Message 1' AND source='SourceA' AND end_message IS NULL AND abort_message IS NULL"));

            //Cleanup
            DropTableTask.Drop(connection, "test_lp_withmessage");
        }

        [Theory, MemberData(nameof(Connections))]
        public void EndLoadProcess(IConnectionManager connection)
        {
            //Arrange
            CreateLoadProcessTableTask.Create(connection, "test_lp_end");

            StartLoadProcessTask.Start(connection, "Test process 2");
            Assert.True(ControlFlow.CurrentLoadProcess.IsRunning == true);

            //Act
            EndLoadProcessTask.End(connection, "End process 2");

            //Assert
            Assert.True(ControlFlow.CurrentLoadProcess.IsRunning == false);
            Assert.True(ControlFlow.CurrentLoadProcess.WasSuccessful == true);
            Assert.True(ControlFlow.CurrentLoadProcess.IsFinished == true);
            Assert.True(ControlFlow.CurrentLoadProcess.EndDate >= DateTime.Now.AddSeconds(-1));

            Assert.Equal(1, RowCountTask.Count(connection, "test_lp_end",
                "is_running=0 and was_successful=1 and was_aborted=0"));
            Assert.Equal(1, RowCountTask.Count(connection, "test_lp_end",
                 "start_message IS NULL AND end_message = 'End process 2' AND abort_message IS NULL"));

            //Cleanup
            DropTableTask.Drop(connection, "test_lp_end");
        }

        [Fact]
        public void AbortLoadProcess()
        {
            //Arrange
            CreateLoadProcessTableTask.Create(SqlConnection, "test_lp_abort");

            StartLoadProcessTask.Start(SqlConnection, "Test process 3");
            Assert.True(ControlFlow.CurrentLoadProcess.IsRunning == true);

            //Act
            AbortLoadProcessTask.Abort(SqlConnection, ControlFlow.CurrentLoadProcess.Id);

            //Assert
            Assert.True(ControlFlow.CurrentLoadProcess.IsRunning == false);
            Assert.True(ControlFlow.CurrentLoadProcess.WasAborted == true);
            Assert.True(ControlFlow.CurrentLoadProcess.AbortMessage == null);
            Assert.Equal(1, RowCountTask.Count(SqlConnection, "test_lp_abort"
                , "is_running=0 and was_successful=0 and was_aborted=1"));

            //Cleanup
            DropTableTask.Drop(SqlConnection, "test_lp_abort");
        }

        [Theory, MemberData(nameof(Connections))]
        public void IsLoadProcessKeyInLog(IConnectionManager connection)
        {
            //Arrange
            CreateLoadProcessTableTask.Create(connection, "test_lpkey_inlog");
            CreateLogTableTask.Create(connection, "test_lpkey_log");
            ControlFlow.AddLoggingDatabaseToConfig(connection, NLog.LogLevel.Info, "test_lpkey_log");
            StartLoadProcessTask.Start(connection, "Test process 5");

            //Act
            SqlTask.ExecuteNonQuery(connection, "Test Task", "Select 1 as test");

            //Assert
            Assert.Equal(2, new RowCountTask("test_lpkey_log",
                $"message='Test Task' and load_process_id = {ControlFlow.CurrentLoadProcess.Id}")
            {
                DisableLogging = true,
                ConnectionManager = connection
            }.Count().Rows); ;

            //Cleanup
            DropTableTask.Drop(connection, ControlFlow.LogTable);
            DropTableTask.Drop(connection, ControlFlow.LoadProcessTable);
        }



        [Fact]
        public void ReadLastSuccessfulProcess()
        {
            //Arrange
            ControlFlow.DefaultDbConnection = SqlConnection;
            CreateLoadProcessTableTask.Create("test_lpkey_lastsuccess");
            StartLoadProcessTask.Start("Test process 8");
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact
            EndLoadProcessTask.End();
            Task.Delay(10).Wait();
            StartLoadProcessTask.Start("Test process 9");
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact
            EndLoadProcessTask.End();

            //Act
            var lp = ReadLoadProcessTableTask.ReadWithOption(ReadOptions.ReadLastSuccessful);

            //Assert
            Assert.True(lp.IsFinished);
            Assert.True(lp.WasSuccessful);
            Assert.False(lp.WasAborted);
            Assert.Equal("Test process 9", lp.ProcessName);

            //Cleanup
            DropTableTask.Drop("test_lpkey_lastsuccess");
        }

        [Fact]
        public void ReadLastAbortedProcess()
        {
            //Arrange
            ControlFlow.DefaultDbConnection = SqlConnection;
            CreateLoadProcessTableTask.Create("test_lpkey_lastabort");
            StartLoadProcessTask.Start("Test process 10");
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact
            EndLoadProcessTask.End();
            Task.Delay(10).Wait();
            StartLoadProcessTask.Start("Test process 11");
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact
            AbortLoadProcessTask.Abort();
            StartLoadProcessTask.Start("Test process 12");
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact
            EndLoadProcessTask.End();

            //Act
            var lp = ReadLoadProcessTableTask.ReadWithOption(ReadOptions.ReadLastAborted);

            //Assert
            Assert.True(lp.IsFinished);
            Assert.True(lp.WasAborted);
            Assert.False(lp.WasSuccessful);
            Assert.Equal("Test process 11", lp.ProcessName);

            //Cleanup
            DropTableTask.Drop(SqlConnection, "test_lpkey_lastabort");
        }

        [Fact]
        public void IsLoadProcessKeySetIfRestarted()
        {
            //Arrange
            ControlFlow.DefaultDbConnection = SqlConnection;
            CreateLoadProcessTableTask.Create("test_lp_restart");
            CreateLogTableTask.Create("test_log_restart");
            ControlFlow.AddLoggingDatabaseToConfig(SqlConnection, NLog.LogLevel.Info, "test_log_restart");

            //Act
            StartLoadProcessTask.Start("Test process 13");
            long? processId1 = ControlFlow.CurrentLoadProcess.Id;
            SqlTask.ExecuteNonQuery("Test Task", "Select 1 as test");
            Assert.Equal(2, new RowCountTask("test_log_restart", $"message='Test Task' AND load_process_id = {processId1}")
            { DisableLogging = true }.Count().Rows);

            StartLoadProcessTask.Start("Test process 14");
            long? processId2 = ControlFlow.CurrentLoadProcess.Id;

            //Assert
            Assert.NotEqual(processId1, processId2);
            SqlTask.ExecuteNonQuery("Test Task", "Select 1 as test");
            Assert.Equal(2, new RowCountTask("test_log_restart", $"message='Test Task' AND load_process_id = {processId2}")
            { DisableLogging = true }.Count().Rows);

            //Cleanup
            DropTableTask.Drop(ControlFlow.LogTable);
            DropTableTask.Drop(ControlFlow.LoadProcessTable);
        }

        [Fact]
        public void IsLoadProcessKeySetForLogTask()
        {
            //Arrange
            ControlFlow.DefaultDbConnection = SqlConnection;
            CreateLoadProcessTableTask.Create("test_lp_logtask");
            CreateLogTableTask.Create("test_log_logtask");
            ControlFlow.AddLoggingDatabaseToConfig(SqlConnection, NLog.LogLevel.Info, "test_log_logtask");

            //Act
            StartLoadProcessTask.Start("Test process 15");
            long? processId1 = ControlFlow.CurrentLoadProcess.Id;
            LogTask.Error("Test1");
            LogTask.Warn("Test2");
            LogTask.Info("Test3");
            //Assert
            Assert.Equal(3, new RowCountTask("test_log_logtask",
                $"message like 'Test%' AND load_process_id = {processId1}")
                { DisableLogging = true }.Count().Rows);

            //Cleanup
            DropTableTask.Drop(ControlFlow.LogTable);
            DropTableTask.Drop(ControlFlow.LoadProcessTable);
        }
    }
}
