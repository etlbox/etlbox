using ALE.ETLBox.DataFlow;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace ALE.ETLBox
{
    public class TableData : TableData<object[]>
    {
        public TableData(TableDefinition definition, List<object[]> rows = null) :
            base(definition, rows)
        { }
    }

    public class TableData<T> : ITableData
    {
        public TableData(TableDefinition definition, List<object[]> rows = null)
        {
            Definition = definition ?? throw new ArgumentNullException(nameof(definition));
            Rows = rows ?? new List<object[]>();
            typeInfo = new DBTypeInfo(typeof(T));
        }

        public IColumnMappingCollection ColumnMapping => GetColumnMappingFromDefinition();

        private IColumnMappingCollection GetColumnMappingFromDefinition()
        {
            var mapping = new DataColumnMappingCollection();
            foreach (var col in Definition.Columns)
                if (!col.IsIdentity)
                {
                    if (!typeInfo.IsDynamic && !typeInfo.IsArray)
                    {
                        if (typeInfo.HasPropertyOrColumnMapping(col.Name))
                            mapping.Add(new DataColumnMapping(col.SourceColumn, col.DataSetColumn));
                    }
                    else if (typeInfo.IsDynamic)
                    {
                        if (DynamicColumnNames.Contains(col.Name))
                            mapping.Add(new DataColumnMapping(col.SourceColumn, col.DataSetColumn));
                    }
                    else
                    {
                        mapping.Add(new DataColumnMapping(col.SourceColumn, col.DataSetColumn));
                    }
                }
            return mapping;
        }

        #region Rows

        public List<object[]> Rows
        {
            get => rows;
            set
            {
                if (value is null)
                    throw new ArgumentNullException(nameof(Rows));
                rows = value;
            }
        }
        IReadOnlyList<IReadOnlyList<object>> ITableData.Rows => Rows;

        private List<object[]> rows;

        #endregion

        public object[] CurrentRow { get; private set; }
        public List<string> DynamicColumnNames { get; } = new List<string>();
        private int readIndex;
        public TableDefinition Definition { get; }
        private int? IDColumnIndex => Definition.IDColumnIndex;
        bool HasIDColumnIndex => IDColumnIndex != null;
        private readonly DBTypeInfo typeInfo;

        public object this[string name] => Rows[GetOrdinal(name)];
        public object this[int i] => Rows[i];
        public int Depth => 0;
        public int FieldCount => Rows.Count;
        public bool IsClosed => Rows.Count == 0;
        public int RecordsAffected => Rows.Count;
        public bool GetBoolean(int i) => Convert.ToBoolean(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        public byte GetByte(int i) => Convert.ToByte(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => 0;
        public char GetChar(int i) => Convert.ToChar(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            string value = Convert.ToString(CurrentRow[ShiftIndexAroundIDColumn(i)]);
            buffer = value.Substring(bufferoffset, length).ToCharArray();
            return buffer.Length;

        }
        public DateTime GetDateTime(int i) => Convert.ToDateTime(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        public IDataReader GetData(int i) => throw new NotImplementedException();//null;
        public decimal GetDecimal(int i) => Convert.ToDecimal(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        public double GetDouble(int i) => Convert.ToDouble(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        public float GetFloat(int i) => float.Parse(Convert.ToString(CurrentRow[ShiftIndexAroundIDColumn(i)]));
        public Guid GetGuid(int i) => Guid.Parse(Convert.ToString(CurrentRow[ShiftIndexAroundIDColumn(i)]));
        public short GetInt16(int i) => Convert.ToInt16(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        public int GetInt32(int i) => Convert.ToInt32(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        public long GetInt64(int i) => Convert.ToInt64(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        public string GetName(int i) => throw new NotImplementedException();
        public string GetDataTypeName(int i) => throw new NotImplementedException();
        public Type GetFieldType(int i) => throw new NotImplementedException();

        public int GetOrdinal(string name) => FindOrdinalInObject(name);

        private int FindOrdinalInObject(string name)
        {
            if (typeInfo.IsArray)
            {
                return Definition.Columns.FindIndex(col => col.Name == name);
            }
            else if (typeInfo.IsDynamic)
            {
                int ix = DynamicColumnNames.FindIndex(n => n == name);
                if (HasIDColumnIndex)
                    if (ix >= IDColumnIndex) ix++;
                return ix;

            }
            else
            {
                int ix = typeInfo.GetIndexByPropertyNameOrColumnMapping(name);
                if (HasIDColumnIndex)
                    if (ix >= IDColumnIndex) ix++;
                return ix;
            }
        }

        public DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        //public string GetDestinationDataType(int i) => Definition.Columns[ShiftIndexAroundIDColumn(i)].DataType;
        //public System.Type GetDestinationNETDataType(int i) => Definition.Columns[ShiftIndexAroundIDColumn(i)].NETDataType;

        public string GetString(int i) => Convert.ToString(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        public object GetValue(int i) => CurrentRow.Length > ShiftIndexAroundIDColumn(i) ? CurrentRow[ShiftIndexAroundIDColumn(i)] : (object)null;

        int ShiftIndexAroundIDColumn(int i)
        {
            if (HasIDColumnIndex)
            {
                if (i > IDColumnIndex) return i - 1;
                else if (i <= IDColumnIndex) return i;
            }
            return i;
        }

        public int GetValues(object[] values)
        {
            values = CurrentRow as object[];
            return values.Length;
        }

        public bool IsDBNull(int i)
        {
            return CurrentRow.Length > ShiftIndexAroundIDColumn(i) ?
                CurrentRow[ShiftIndexAroundIDColumn(i)] == null : true;
        }

        public bool NextResult()
        {
            return (readIndex + 1) <= Rows?.Count;
        }

        public bool Read()
        {
            if (Rows.Count > readIndex)
            {
                CurrentRow = Rows[readIndex];
                readIndex++;
                return true;
            }
            else
                return false;
        }

        #region IDisposable Support
        private bool disposedValue = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    Rows.Clear();
                    Rows = null;
                }

                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        public void Close()
        {
            Dispose();
        }

        #endregion
    }
}
