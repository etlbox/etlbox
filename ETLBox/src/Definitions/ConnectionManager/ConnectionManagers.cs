using System;

namespace ALE.ETLBox.ConnectionManager
{
    /// <summary>
    /// <see cref="IConnectionManager"/> helper
    /// </summary>
    public static class ConnectionManagers
    {
        public static T CloneWith<T>(this T connectionManager, IDbConnectionString connectionString)
            where T : class, IConnectionManager
        {
            if (connectionManager is null)
                throw new ArgumentNullException(nameof(connectionManager));
            if (connectionString is null)
                throw new ArgumentNullException(nameof(connectionString));
            var clone = (T)connectionManager.Clone();
            clone.ConnectionString = connectionString;
            return clone;
        }

        public static T CloneWithMasterDbConnectionString<T>(this T connectionManager)
            where T : class, IConnectionManager
        {
            if (connectionManager is null)
                throw new ArgumentNullException(nameof(connectionManager));
            var connectionString = connectionManager.ConnectionString as IDbConnectionStringWithDbName;
            if (connectionString is null)
                throw new ArgumentException($"Connection string must be {nameof(IDbConnectionStringWithDbName)}", nameof(connectionString));
            connectionString = connectionString.CloneWithMasterDbName();
            return connectionManager.CloneWith(connectionString);
        }
    }
}
