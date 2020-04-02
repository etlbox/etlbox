using System.Data.Common;

namespace ALE.ETLBox
{
    /// <summary>
    /// <see cref="IDbConnectionStringWithDbName"/> base
    /// </summary>
    /// <typeparam name="T">Derived type</typeparam>
    /// <typeparam name="TBuilder"><see cref="DbConnectionString{T, TBuilder}.Builder"/> type</typeparam>
    public abstract class DbConnectionStringWithDbName<T, TBuilder> :
        DbConnectionString<T, TBuilder>,
        IDbConnectionStringWithDbName
        where T : DbConnectionStringWithDbName<T, TBuilder>, new()
        where TBuilder : DbConnectionStringBuilder, new()
    {
        protected DbConnectionStringWithDbName(string value = null) :
            base(value)
        { }

        public bool HasDbName => !string.IsNullOrWhiteSpace(DbName);
        public abstract string DbName { get; set; }

        public virtual bool HasMasterDbName => DbName == MasterDbName;
        public abstract string MasterDbName { get; }

        public T CloneWithDbName(string value = null)
        {
            if (string.IsNullOrWhiteSpace(value))
                value = null;
            var clone = Clone();
            clone.DbName = value ?? string.Empty;
            return clone;
        }
        IDbConnectionStringWithDbName IDbConnectionStringWithDbName.CloneWithDbName(string value) => CloneWithDbName();
        public T CloneWithMasterDbName() => CloneWithDbName(MasterDbName);
        IDbConnectionStringWithDbName IDbConnectionStringWithDbName.CloneWithMasterDbName() => CloneWithMasterDbName();
    }
}
