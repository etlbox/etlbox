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
    public class DbDestinationDataTypeTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");
        public DbDestinationDataTypeTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MyDataTypeRow
        {
            public int IntCol { get; set; }
            public long LongCol { get; set; }
            public decimal DecimalCol { get; set; }
            public double DoubleCol { get; set; }
            public DateTime DateTimeCol { get; set; }
            public DateTime DateCol { get; set; }
            public string StringCol { get; set; }
            public char CharCol { get; set; }
            public string DecimalStringCol { get; set; }
            public string NullCol { get; set; }
        }

        [Theory, MemberData(nameof(Connections))]
        public void MixedTypes(IConnectionManager connection)
        {
            CreateTableTask.Create(connection, "datatypedestination",
                new List<TableColumn>() {
                    new TableColumn("IntCol", "INT", allowNulls: true),
                    new TableColumn("LongCol", "BIGINT", allowNulls: true),
                    new TableColumn("DecimalCol", "FLOAT", allowNulls: true),
                    new TableColumn("DoubleCol", "FLOAT", allowNulls: true),
                    new TableColumn("DateTimeCol", "DATETIME", allowNulls: true),
                    new TableColumn("DateCol", "DATE", allowNulls: true),
                    new TableColumn("StringCol", "VARCHAR(200)", allowNulls: true),
                    new TableColumn("CharCol", "CHAR(1)", allowNulls: true),
                    new TableColumn("DecimalStringCol", "DECIMAL(12,10)", allowNulls: true),
                    new TableColumn("NullCol", "CHAR(1)", allowNulls: true),
                });
            //Arrange
            MemorySource<MyDataTypeRow> source = new MemorySource<MyDataTypeRow>(
                new List<MyDataTypeRow>() {
                   new MyDataTypeRow() {
                       IntCol = 1,
                       LongCol = -1,
                       DecimalCol = 2.3M,
                       DoubleCol = 5.4,
                       DateTimeCol = new DateTime(2010,1,1,10,10,10),
                       DateCol =  new DateTime(2020,1,1),
                       StringCol = "Test",
                       CharCol = 'T',
                       DecimalStringCol = "13.4566",
                       NullCol = null
                   }
                }) ;

            //Act
            DbDestination<MyDataTypeRow> dest = new DbDestination<MyDataTypeRow>("datatypedestination", connection);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            //            IntCol LongCol DecimalCol DoubleCol   DateTimeCol DateCol StringCol CharCol DecimalStringCol NullCol
            //1 - 1  2.3 5.4 2010 - 01 - 01 10:10:10.100 2020 - 01 - 01  Test T   13.4566000000   NULL
            SqlTask.ExecuteReaderSingleLine(connection, "Check data", "SELECT * FROM datatypedestination",
                col => Assert.True( Convert.ToInt32(col) == 1),
                col => Assert.True( Convert.ToInt64(col) == -1),
                col => Assert.True( Convert.ToDecimal(col) == 2.3M),
                col => Assert.True( Convert.ToDecimal(col) == 5.4M),
                col => Assert.True( Convert.ToDateTime(col) == new DateTime(2010, 1, 1, 10, 10, 10)),
                col => Assert.True(Convert.ToDateTime(col) == new DateTime(2020, 1, 1)),
                col => Assert.True( Convert.ToString(col) == "Test"),
                col => Assert.True( Convert.ToString(col) == "T" || Convert.ToString(col) == "84"),
                col => Assert.True(Convert.ToString(col).Replace("0","") == "13.4566"),
                col => Assert.True(col == null)
            );

        }
    }
}
