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

namespace ALE.ETLBoxTests.ControlFlowTests.Postgres
{
    [Collection("ControlFlow")]
    public class TableDefinitionTests
    {
        public PostgresConnectionManager PostgresConnection => Config.PostgresConnection.ConnectionManager("ControlFlow");

        public TableDefinitionTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Fact]
        public void Identity()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(PostgresConnection, "Create table", @"
CREATE TABLE identity (
    document_id serial PRIMARY KEY,
     header_text VARCHAR (255) NOT NULL
)"
            );

            //Act
            var result = TableDefinition.FromName(PostgresConnection, "identity");

            //Assert
            Assert.Collection(result.Columns,
                tc => Assert.True(tc.DataType == "integer" && tc.NETDataType == typeof(Int32)
                                    && tc.IsIdentity && tc.IsPrimaryKey),
                tc => Assert.True(tc.DataType == "varchar(255)" && tc.NETDataType == typeof(String))
            );
        }

        [Fact]
        public void DateTypes()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(PostgresConnection, "Create table", @"
CREATE TABLE datetimetypes (
    datetype DATE,
    timetype TIME,
    timetypewithtimezone TIMETZ,
    intervaltype INTERVAL,
    tswithouttimezone TIMESTAMP,
    tswithtimezone TIMESTAMPTZ
)"
            );

            //Act
            var result = TableDefinition.FromName(PostgresConnection, "datetimetypes");

            //Assert
            Assert.Collection(result.Columns,
                tc => Assert.True(tc.DataType == "date" && tc.NETDataType == typeof(DateTime)),
                tc => Assert.True(tc.DataType == "time" && tc.NETDataType == typeof(DateTime)),
                tc => Assert.True(tc.DataType == "timetz" && tc.NETDataType == typeof(DateTime)),
                tc => Assert.True(tc.DataType == "interval" && tc.NETDataType == typeof(String)),
                tc => Assert.True(tc.DataType == "timestamp" && tc.NETDataType == typeof(DateTime)),
                tc => Assert.True(tc.DataType == "timestamptz" && tc.NETDataType == typeof(DateTime))
            );
        }


        [Fact]
        public void Character_Varying()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(PostgresConnection, "Create table", @"
CREATE TABLE varchartable (
    varchar_50 VARCHAR (50) NULL,
    varchar_novalue VARCHAR NULL,
    varchar_charvary CHARACTER VARYING (50),
    varchar_character CHARACTER (10),
    varchar_char CHAR,
    varchar_text TEXT
)"
            );

            //Act
            var result = TableDefinition.FromName(PostgresConnection, "varchartable");

            //Assert
            Assert.Collection(result.Columns,
                tc => Assert.True(tc.DataType == "varchar(50)" && tc.NETDataType == typeof(String)),
                tc => Assert.True(tc.DataType == "varchar" && tc.NETDataType == typeof(String)),
                tc => Assert.True(tc.DataType == "varchar(50)" && tc.NETDataType == typeof(String)),
                tc => Assert.True(tc.DataType == "char(10)" && tc.NETDataType == typeof(String)),
                 tc => Assert.True(tc.DataType == "char(1)" && tc.NETDataType == typeof(String)),
                tc => Assert.True(tc.DataType == "text" && tc.NETDataType == typeof(String))
            );
        }

        [Fact]
        public void Numeric()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(PostgresConnection, "Create table", @"
CREATE TABLE numerictable (
    si SMALLINT NULL,
    i INT NULL,
    bi BIGINT,
    dec DECIMAL(10,5),
    dec_2 DECIMAL(10),
    dec_3 DECIMAL,
    num NUMERIC (1,1),
    num_2 NUMERIC (2),
    num_3 NUMERIC,
    r REAL,
    d DOUBLE PRECISION
)"
            );

            //Act
            var result = TableDefinition.FromName(PostgresConnection, "numerictable");

            //Assert
            Assert.Collection(result.Columns,
                tc => Assert.True(tc.DataType == "smallint" && tc.NETDataType == typeof(Int16)),
                tc => Assert.True(tc.DataType == "integer" && tc.NETDataType == typeof(Int32)),
                tc => Assert.True(tc.DataType == "bigint" && tc.NETDataType == typeof(Int64)),
                tc => Assert.True(tc.DataType == "numeric(10,5)" && tc.NETDataType == typeof(decimal)),
                tc => Assert.True(tc.DataType == "numeric(10,0)" && tc.NETDataType == typeof(decimal)),
                tc => Assert.True(tc.DataType == "numeric" && tc.NETDataType == typeof(decimal)),
                tc => Assert.True(tc.DataType == "numeric(1,1)" && tc.NETDataType == typeof(decimal)),
                tc => Assert.True(tc.DataType == "numeric(2,0)" && tc.NETDataType == typeof(decimal)),
                tc => Assert.True(tc.DataType == "numeric" && tc.NETDataType == typeof(decimal)),
                tc => Assert.True(tc.DataType == "real" && tc.NETDataType == typeof(double)),
                tc => Assert.True(tc.DataType == "double precision" && tc.NETDataType == typeof(double))
            );
        }
    }
}
