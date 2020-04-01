using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class ExcelSourceTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public ExcelSourceTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            [ExcelColumn(0)]
            public int Col1 { get; set; }
            [ExcelColumn(1)]
            public string Col2 { get; set; }
        }

        [Fact]
        public void SimpleData()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("ExcelDestination1");

            //Act
            ExcelSource<MySimpleRow> source = new ExcelSource<MySimpleRow>("res/Excel/TwoColumnData.xlsx");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>("ExcelDestination1", Connection, 2);

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        public class OneExcelColumn
        {
            public int Col1 { get; set; }
            [ExcelColumn(1)]
            public string Col2 { get; set; }
        }

        [Fact]
        public void OnlyOneExcelColumn()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("ExcelDestination2");

            //Act
            ExcelSource<OneExcelColumn> source = new ExcelSource<OneExcelColumn>("res/Excel/TwoColumnData.xlsx");
            DbDestination<OneExcelColumn> dest = new DbDestination<OneExcelColumn>("ExcelDestination2", Connection, 2);

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(Connection, "ExcelDestination2", "Col1 = 0 AND Col2 LIKE 'Test%'"));
        }

        public class ExcelDataSheet2
        {
            [ExcelColumn(1)]
            public string Col2 { get; set; }
            [ExcelColumn(2)]
            public decimal? Col4 { get; set; }
            public string Empty { get; set; } = "";
            [ExcelColumn(0)]
            public int Col3 { get; set; }
        }


        [Fact]
        public void DataOnSheet2WithRange()
        {
            //Arrange
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture("ExcelDestination3");

            //Act
            ExcelSource<ExcelDataSheet2> source = new ExcelSource<ExcelDataSheet2>("res/Excel/DataOnSheet2.xlsx")
            {
                Range = new ExcelRange(2, 4, 5, 9),
                SheetName = "Sheet2"
            };

            DbDestination<ExcelDataSheet2> dest = new DbDestination<ExcelDataSheet2>("ExcelDestination3", Connection);

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(5, RowCountTask.Count(Connection, "ExcelDestination3"));
            Assert.Equal(1, RowCountTask.Count(Connection, "ExcelDestination3", "Col2 = 'Wert1' AND Col3 = 5 AND Col4 = 1"));
            Assert.Equal(1, RowCountTask.Count(Connection, "ExcelDestination3", "Col2 IS NULL AND Col3 = 0 AND Col4 = 1.2"));
            Assert.Equal(1, RowCountTask.Count(Connection, "ExcelDestination3", "Col2 IS NULL AND Col3 = 7 AND Col4 = 1.234"));
            Assert.Equal(1, RowCountTask.Count(Connection, "ExcelDestination3", "Col2 = 'Wert4' AND Col3 = 8 AND Col4 = 1.2345"));
            Assert.Equal(1, RowCountTask.Count(Connection, "ExcelDestination3", "Col2 = 'Wert5' AND Col3 = 9 AND Col4 = 2"));
        }

        public class Excel21Cols
        {
            [ExcelColumn(0)]
            public int Col1 { get; set; }
            [ExcelColumn(1)]
            public string Col2 { get; set; }
            [ExcelColumn(13)]
            public string N { get; set; }
            [ExcelColumn(21)]
            public string V { get; set; }
        }

        [Fact]
        public void Exceding20Columns()
        {
            //Arrange
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture("ExcelDestination");

            //Act
            ExcelSource<Excel21Cols> source = new ExcelSource<Excel21Cols>("res/Excel/MoreThan20Cols.xlsx")
            {
                Range = new ExcelRange(1, 2),
            };

            MemoryDestination<Excel21Cols> dest = new MemoryDestination<Excel21Cols>();

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            Assert.Collection<Excel21Cols>(dest.Data,
                r => Assert.True(r.Col1 == 1 && r.Col2 == "Test1" && r.N == "N" && r.V == "V"),
                r => Assert.True(r.Col1 == 2 && r.Col2 == "Test2" && r.N == "N" && r.V == "V"),
                r => Assert.True(r.Col1 == 3 && r.Col2 == "Test3" && r.N == "N" && r.V == "V")
                );
        }
    }
}
