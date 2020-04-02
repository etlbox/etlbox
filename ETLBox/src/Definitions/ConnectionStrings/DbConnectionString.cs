using System.Data.Common;

namespace ALE.ETLBox
{
    /// <summary>
    /// <see cref="IDbConnectionString"/> base
    /// </summary>
    /// <typeparam name="T">Derived type</typeparam>
    /// <typeparam name="TBuilder"><see cref="Builder"/> type</typeparam>
    public abstract class DbConnectionString<T, TBuilder> :
        IDbConnectionString
        where T : DbConnectionString<T, TBuilder>, new()
        where TBuilder : DbConnectionStringBuilder, new()
    {
        protected DbConnectionString(string value = null) =>
            Value = value;

        public TBuilder Builder { get; private set; } = new TBuilder();

        public virtual string Value
        {
            get => Builder.ConnectionString;
            set => Builder.ConnectionString = value;
        }

        public virtual T Clone()
        {
            var clone = (T)MemberwiseClone();
            clone.Builder = new TBuilder();
            clone.Value = Value;
            return clone;
        }
        IDbConnectionString IDbConnectionString.Clone() => Clone();

        public override string ToString() => Value;
    }
}
