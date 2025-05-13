namespace GeneralFunctions
{
    [AttributeUsage(AttributeTargets.Property)]
    public class PrimaryKeyAttribute : NullSkipAttribute
    {
        public PrimaryKeyAttribute()
        {

        }
    }
    [AttributeUsage(AttributeTargets.Property)]
    public class IgnoreColumnAttribute : Attribute
    {
        public IgnoreColumnAttribute()
        {

        }
    }
    [AttributeUsage(AttributeTargets.Property)]
    public class FixedValueAttribute : Attribute
    {
        public FixedValueAttribute()
        {

        }
    }
    [AttributeUsage(AttributeTargets.Property)]
    public class ColumnNameAttribute : Attribute
    {
        public string ColumnName { get; set; }
        public ColumnNameAttribute(string columnName)
        {
            ColumnName = columnName;
        }
    }

    [AttributeUsage(AttributeTargets.Property, Inherited = true)]
    public class NullSkipAttribute : Attribute
    {
        public NullSkipAttribute()
        {

        }
    }
    [AttributeUsage(AttributeTargets.Class)]
    public class TableAttribute : Attribute
    {
    }
}
