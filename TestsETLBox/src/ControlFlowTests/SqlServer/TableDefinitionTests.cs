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

namespace ALE.ETLBoxTests.ControlFlowTests.SqlServer
{
    [Collection("ControlFlow")]
    public class TableDefinitionTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("ControlFlow");

        public TableDefinitionTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Fact]
        public void BigIntIdentity()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(SqlConnection, "Create table", @"
CREATE TABLE BigIntIdentity (
    Id BIGINT NOT NULL PRIMARY KEY IDENTITY(100,10)
)"
            );

            //Act
            var result = TableDefinition.FromName(SqlConnection, "BigIntIdentity");

            //Assert
            Assert.Collection(result.Columns,
                           tc => Assert.True(tc.DataType == "BIGINT"
                           && tc.NETDataType == typeof(Int64)
                           && tc.IsIdentity == true
                           && tc.IdentityIncrement == 10
                           && tc.IdentitySeed == 100
                           )
                       );
        }

        [Fact]
        public void NumbericDataTypes()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(SqlConnection, "Create table", @"
CREATE TABLE NumericDataTypes (
    Id BIGINT NOT NULL,
    Col1 NUMERIC,
    Col2 BIT,
    Col3 SMALLINT,
    Col4 DECIMAL,
    Col5 SMALLMONEY,
    Col6 INT,
    Col7 TINYINT,
    Col8 MONEY,
    Col9 FLOAT,
    Col10 REAL
)"
            );

            //Act
            var result = TableDefinition.FromName(SqlConnection, "NumericDataTypes");

            //Assert
            Assert.Collection(result.Columns,
                tc => Assert.True(tc.DataType == "BIGINT" && tc.NETDataType == typeof(Int64)),
                tc => Assert.True(tc.DataType == "NUMERIC" && tc.NETDataType == typeof(Decimal)),
                tc => Assert.True(tc.DataType == "BIT" && tc.NETDataType == typeof(Boolean)),
                tc => Assert.True(tc.DataType == "SMALLINT" && tc.NETDataType == typeof(Int16)),
                tc => Assert.True(tc.DataType == "DECIMAL" && tc.NETDataType == typeof(Decimal)),
                tc => Assert.True(tc.DataType == "SMALLMONEY" && tc.NETDataType == typeof(Decimal)),
                tc => Assert.True(tc.DataType == "INT" && tc.NETDataType == typeof(Int32)),
                tc => Assert.True(tc.DataType == "TINYINT" && tc.NETDataType == typeof(UInt16)),
                tc => Assert.True(tc.DataType == "MONEY" && tc.NETDataType == typeof(Decimal)),
                tc => Assert.True(tc.DataType == "FLOAT" && tc.NETDataType == typeof(Double)),
                tc => Assert.True(tc.DataType == "REAL" && tc.NETDataType == typeof(Double))
            );
        }

        [Fact]
        public void DateTimeTypes()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(SqlConnection, "Create table", @"
CREATE TABLE ReadTableDefinition (
    Col11 DATE,
    --Col12 DATETIMEOFFSET,
    Col13 DATETIME2,
    Col14 SMALLDATETIME,
    Col15 DATETIME,
    Col16 TIME

)"
            );

            //Act
            var result = TableDefinition.FromName(SqlConnection, "ReadTableDefinition");

            //Assert
            Assert.True(result.Columns.All(tc => tc.NETDataType == typeof(DateTime)));
            Assert.True(result.Columns.Count == 5);
        }

        /* Missing types
         --Col17 CHAR(10),
        --Col18 VARCHAR(20),
        --Col19 TEXT,
        --Col20 NCHAR(4000),
        --Col21 NVARCHAR(MAX),
        --Col22 NTEXT,
        --Col23 BINARY,
        --Col24 VARBINARY(20),
        --Col25 IMAGE
        */
    }
}
