using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Xml;
using System.Reflection;
using System.Linq;
using System.IO;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Contracts;

namespace GeneralFunctions
{
    public static class DataFunctions
    {
        private const string sepComma = ", ";
        private const string sepCn = ",\n";

        internal const double nullDouble = -999;
        internal const int nullInt = -999;

        public const string dateFormat = "yyyyMMdd";
        public const string dateHourFormat = "yyyyMMddHH";
        public const string hourFormat = "HH";
        public static readonly TimeSpan anHour = TimeSpan.FromHours(1);
        public static readonly TimeSpan aDay = TimeSpan.FromDays(1);

        internal static readonly Dictionary<Type, PropertyInfo[]> _cachedProperties = [];
        internal static readonly Dictionary<Type, string[]> _cachedCols = [];
        internal static readonly Dictionary<Type, string[]> _cachedPropName = [];

        public static PropertyInfo[] GetProperties(Type type)
        {
            if (_cachedProperties.ContainsKey(type))
                return _cachedProperties[type];
            else
            {
                _cachedProperties[type] = type.GetProperties()
            .Where(p => p.GetCustomAttribute<IgnoreColumnAttribute>() == null).ToArray(); // 예외 처리된 속성 제외
                return _cachedProperties[type];
            }
        }
        
        public static List<T> ToList<T>(this DataTable dataTable) where T : new()
        {
            List<T> voList = [];

            foreach (DataRow row in dataTable.Rows)
            {
                T vo = new();

                foreach (var prop in GetProperties<T>())
                    if (row[prop.Name] != DBNull.Value)
                        prop.SetValue(vo, Convert.ChangeType(row[prop.Name], prop.PropertyType), null);

                voList.Add(vo);
            }

            return voList;
        }
        

        public static void BeforeHour(out string basedate, out string basetime)
        {
            DateTime date = DateTime.Now;
            date -= anHour;
            basedate = date.ToString(dateFormat);
            basetime = date.ToString(hourFormat);
        }

        public static string Yesterday
        {
            get { return DateTime.Today.AddDays(-1).ToString(dateFormat); }
        }
        public static async Task DelayUntil(DateTime targetTime)
        {
            TimeSpan waitTime = targetTime - DateTime.Now;
            if (waitTime > TimeSpan.Zero)
            {
                Console.WriteLine($"다음 시각까지 {waitTime} 동안 대기합니다...");
                await Task.Delay(waitTime);
            }
        }
        public static string Today
        {
            get { return DateTime.Today.ToString(dateFormat); }
        }
        public static DataTable ToDataTable<T>(this IList<T> list)
        {
            DataTable dataTable = new();

            if (list != null && list.Count > 0)
            {
                //데이터 테이블의 열을 생성합니다.

                foreach (var prop in typeof(T).GetProperties())
                    dataTable.Columns.Add(prop.Name, prop.PropertyType);

                // 리스트의 아이템을 데이터 테이블의 행으로 추가합니다.
                foreach (var item in list)
                {
                    var row = dataTable.NewRow();
                    foreach (var prop in typeof(T).GetProperties())
                        row[prop.Name] = prop.GetValue(item, null);

                    dataTable.Rows.Add(row);
                }
            }
            return dataTable;
        }

        

        public static string[] GetColumnNames<T>()
        {
            if (_cachedCols.ContainsKey(typeof(T)))
                return _cachedCols[typeof(T)];
            else
                return _cachedCols[typeof(T)] = [.. GetProperties<T>().Select(prop => prop.CheckAttribute<ColumnNameAttribute>(out var attr) ? attr.ColumnName : prop.Name)];
        }

        public static string[] GetPropNames<T>(this T type)
        {
            if (_cachedPropName.ContainsKey(typeof(T)))
                return _cachedPropName[typeof(T)];
            else
                return _cachedPropName[typeof(T)] = [.. GetProperties<T>().Select(prop => prop.Name)];
        }

        public static string[] GetColumnNames(this IEnumerable<PropertyInfo> props)
        {
            return [.. props.Select(prop => prop.CheckAttribute<ColumnNameAttribute>(out var attr) ? attr.ColumnName : prop.Name)];
        }

        public static List<string> GetColumnNames(this DataTable source)
        {

            List<string> colNames = [];
            foreach (DataColumn dc in source.Columns)
                colNames.Add(dc.ColumnName);
            return colNames;
        }
        public static DataTable GetColumnData(this DataTable source, params string[] columnNames)
        {
            DataTable dt = new();
            foreach (string colName in columnNames)
                foreach (DataColumn col in source.Columns)
                    if (col.ColumnName.Equals(colName))
                        dt.Columns.Add(new DataColumn(colName, col.DataType));
            foreach (DataRow dr in source.Rows)
            {
                DataRow ndr = dt.Rows.Add();
                foreach (string colName in columnNames)
                    ndr[colName] = dr[colName];
            }
            return dt;

        }
        public static DataTable GetColumnData(this DataTable source, List<string> columnNames)
        {
            DataTable dt = new();
            foreach (DataColumn col in source.Columns)
                foreach (string colName in columnNames)
                    if (col.ColumnName.Equals(colName))
                        dt.Columns.Add(col);
            foreach (DataRow dr in source.Rows)
            {
                DataRow ndr = dt.Rows.Add();
                foreach (string colName in columnNames)
                    ndr[colName] = dr[colName];
            }
            return dt;
        }
        public static int ChangeColumnName(this DataTable source, IList<string> columnNames)
        {
            if (source.Columns.Count < columnNames.Count)
                throw new Exception();
            for (int i = 0; i < columnNames.Count; i++)
                source.Columns[i].ColumnName = columnNames[i];
            return columnNames.Count;
        }
        public static DataTable AppendLeftData(params DataTable[] input)
        {
            DataTable dt = new();
            foreach (DataTable idt in input)
                dt.AppendColumn(idt);
            foreach (DataTable idt in input)
                dt.PasteDataTableByCol(idt, idt.GetColumnNames());
            return dt;
        }
        public static DataTable AppendLeftData(this DataTable source, params DataTable[] input)
        {
            DataTable dt = source.Copy();
            foreach (DataTable idt in input)
                dt.AppendColumn(idt);
            foreach (DataTable idt in input)
                dt.PasteDataTableLeft(idt, idt.GetColumnNames());
            return dt;
        }
        public static DataTable AppendDownData(this DataTable source, params DataTable[] input)
        {
            DataTable dt = source.Copy();
            foreach (DataTable idt in input)
                dt.PasteDataTableByCol(idt, source.GetColumnNames());
            return dt;
        }
        public static DataTable PasteDataTableLeft(this DataTable target, DataTable input, List<string> columnNames)
        {
            for (int i = 0; i < target.Rows.Count; i++)
            {
                foreach (string colName in columnNames)
                {
                    try
                    {
                        target.Rows[i][colName] = input.Rows[i][colName];
                    }
                    catch (ArgumentException)
                    {
                        continue;
                    }
                }
            }

            return target;
        }
        public static DataTable PasteDataTableByCol(this DataTable target, DataTable input, List<string> columnNames)
        {
            for (int i = 0; i < input.Rows.Count; i++)
            {
                DataRow dr = target.NewRow();
                foreach (string colName in columnNames)
                {
                    try
                    {
                        dr[colName] = input.Rows[i][colName];
                    }
                    catch (ArgumentException)
                    {
                        continue;
                    }
                }

                target.Rows.Add(dr);
            }

            return target;
        }

        public static DataTable AppendColumn(this DataTable table, DataTable input)
        {
            foreach (DataColumn dc in input.Columns)
                table.Columns.Add(new DataColumn(dc.ColumnName, dc.DataType));
            return table;
        }



        public static PropertyInfo[] GetProperties<T>(this T type, out PropertyInfo[] properties)
        {
            return properties = GetProperties<T>();
        }

        public static PropertyInfo[] GetProperties<T>()
        {
            return GetProperties(typeof(T));
        }

        public static bool CheckAttribute<T>(this PropertyInfo prop,[NotNullWhen(true)] out T? attr) where T : Attribute
        {
            attr = prop.GetCustomAttribute<T>();
            return attr != null;
        }

        public static object? GetDefaultFieldValue<T>(string propertyName) where T : new()
        {
            return typeof(T).GetField(propertyName)?.GetValue(new T());
        }

        public static int GetPropertiesFromAttribute<Tattribute, Vo>(out PropertyInfo[] keys) where Tattribute : Attribute
        {
            keys = [.. typeof(Vo).GetProperties().Where(prop => Attribute.IsDefined(prop, typeof(Tattribute)))];

            return keys.Length;
        }
        public static int FilterAttribute<Tattribute, Vo>(this PropertyInfo[] props, out PropertyInfo[] values, out PropertyInfo[] keys) where Tattribute : Attribute
        {
            values = [.. props.Where(prop => !Attribute.IsDefined(prop, typeof(Tattribute)))];
            keys = [.. props.Where(prop => Attribute.IsDefined(prop, typeof(Tattribute)))];
            return keys.Length;
        }

        

        public static List<string> ColumnNameValueSets<T>(this IEnumerable<PropertyInfo> props, T VO, bool nullable)
        {
            List<string> res = [];
            foreach (var prop in props)
                if (prop.NullCheck(VO, out var val))
                    if (nullable)
                        res.Add($"{prop.GetCustomAttribute<ColumnNameAttribute>()?.ColumnName ?? prop.Name} = NULL");
                    else
                        throw new ArgumentNullException(prop.Name);
                else
                    res.Add($"{prop.GetCustomAttribute<ColumnNameAttribute>()?.ColumnName ?? prop.Name} = '{val}'");
            return res;
        }

        public static string Value<T>(this T VO, PropertyInfo[] properties, bool nullable)
        {
            List<string> res = [];
            foreach (var prop in properties)
                if (prop.NullCheck(VO, out var val))
                    if (Attribute.IsDefined(prop, typeof(NullSkipAttribute)))
                        return "";
                    else if (nullable)
                        res.Add("NULL");
                    else
                        throw new ArgumentNullException(prop.Name);
                else if (prop.PropertyType == typeof(string))
                    res.Add($"'{val}'");
                else
                    res.Add(val?.ToString() ?? "NULL");
            return $"({string.Join(sepComma, res)})";
        }

        public static List<string> TableColSets(this IEnumerable<PropertyInfo> props, string Atable, string Btable)
        {
            if (Atable == null || Atable.Length == 0)
                if (Btable == null || Btable.Length == 0)
                    return [.. props.Select(prop => prop.GetCustomAttribute<ColumnNameAttribute>()?.ColumnName ?? prop.Name)];
                else
                    return [.. props.Select(prop => prop.CheckAttribute<ColumnNameAttribute>(out var attr) ? $"{attr.ColumnName} = {Btable}.{attr.ColumnName}" : $"{prop.Name} = {Btable}.{prop.Name}")];
            else
                return [.. props.Select(prop => prop.CheckAttribute<ColumnNameAttribute>(out var attr) ? $"{Atable}.{attr.ColumnName} = {Btable}.{attr.ColumnName}" : $"{Atable}.{prop.Name} = {Btable}.{prop.Name}")];
        }

        public static List<string> TableColSets(this IEnumerable<PropertyInfo> props, string I_table)
        {
            if (I_table == null || I_table.Length == 0)
                return [.. props.Select(prop => prop.GetCustomAttribute<ColumnNameAttribute>()?.ColumnName ?? prop.Name)];
            else
                return [.. props.Select(prop => {
                    string propName = prop.GetCustomAttribute<ColumnNameAttribute>()?.ColumnName ?? prop.Name;
                    return $"{propName} = {I_table}.{propName}";
                })];
        }

        public static List<string> TableColSets(this IEnumerable<PropertyInfo> props)
        {
            return [.. props.Select(prop => prop.GetCustomAttribute<ColumnNameAttribute>()?.ColumnName ?? prop.Name)];
        }

        public static string SqlLog(this StringBuilder sqlBuilder)
        {
            string sql = sqlBuilder.ToString();
            Console.WriteLine(sql);
            return sql;
        }

        public static double ParseCoordinate(this string coordinate)
        {
            try
            {
                string[] parts = coordinate.Split('-');

                // 위도와 경도 값을 double로 변환
                double degrees = Convert.ToDouble(parts[0]);
                double minutes = Convert.ToDouble(parts[1]);
                double seconds = Convert.ToDouble(parts[2]);

                // 분과 초를 적절한 위치의 값으로 변환
                return degrees + (minutes / 60) + (seconds / 3600);
            }
            catch
            {
                return nullDouble;
            }
        }

        public static double GetCoordinateXml(this XmlNode node, string xpath)
        {
            try
            {
                string? coordinate = node.SelectSingleNode(xpath)?.InnerText;
                string[] parts = coordinate?.Split('-') ?? [];

                // 위도와 경도 값을 double로 변환
                double degrees = Convert.ToDouble(parts[0]);
                double minutes = Convert.ToDouble(parts[1]);
                double seconds = Convert.ToDouble(parts[2]);

                // 분과 초를 적절한 위치의 값으로 변환
                return degrees + (minutes / 60) + (seconds / 3600);
            }
            catch
            {
                return nullDouble;
            }
        }

        public static DataTable ParseDataToDataTable(this string data)
        {
            DataTable result = new();
            bool firstRow = true;
            using (StringReader sr = new(data))
            {
                string? line;
                while ((line = sr.ReadLine()) != null)
                {
                    if (line.StartsWith('#') || string.IsNullOrEmpty(line))
                        continue;
                    string[] parts = line.Split([' '], StringSplitOptions.RemoveEmptyEntries);
                    if (firstRow)
                    {
                        firstRow = false;
                        foreach (string header in parts)
                            result.Columns.Add(header, typeof(string));
                    }
                    result.Rows.Add(parts);
                }

            }
            return result;
        }

        public static double ToDouble(this object value)
        {
            return Double.TryParse(value.ToString(), out double res) ? res : nullDouble;
        }
        public static double ToDouble(this object value, double nullvalue)
        {
            return Double.TryParse(value.ToString(), out double res) ? res : nullvalue;
        }

        public static int ToInt(this object value)
        {
            return int.TryParse(value.ToString(), out int res) ? res : nullInt;
        }
        public static int ToInt(this object value, int nullvalue)
        {
            return int.TryParse(value.ToString(), out int res) ? res : nullvalue;
        }

        public static double GetDoubleXml(this XmlNode node, string xpath)
        {
            return Double.TryParse(node.SelectSingleNode(xpath)?.InnerText, out double res) ? res : nullDouble;
        }
        public static void ForEach<T>(this IEnumerable<T> source, Action<T> action)
        {
            foreach (var item in source)
            {
                action(item);
            }
        }
    }

}