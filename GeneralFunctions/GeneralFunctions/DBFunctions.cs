using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace GeneralFunctions
{
    public static class DBFunctions
    {
        private const string sepComma = ", ";
        private const string sepCn = ",\n";
        private const string sepWhereAnd = "\nand\t\t";
        private const string sepSet = ",\n\t";
        public static string Target<T>(this IList<T> list) where T : TableBase<T>, new()
        {
            if (list.Count == 0)
                return Target<T>();
            else
                return list[0].Target();
        }

        public static string Target<T>() where T : TableBase<T>, new()
        {
            return new T().Target();
        }
        public static List<T> ConvertList<T>(this IList<T> table) where T : TableBase<T>
        {
            if (table == null || table.Count == 0)
                return new List<T>();
            List<T> newList = new List<T>();
            foreach (var row in table)
                newList.Add((T)row);
            return newList;
        }
        public static List<T> ToTableList<T>(this DataTable dataTable) where T : TableBase<T>, new()
        {
            List<T> voList = new();

            foreach (DataRow row in dataTable.Rows)
            {
                T vo = new T();

                foreach (var prop in TableBase<T>.GetProperties)
                    if (row[prop.Name] != DBNull.Value)
                        prop.SetValue(vo, Convert.ChangeType(row[prop.Name], prop.PropertyType), null);

                voList.Add(vo);
            }

            return voList;
        }
        public static string CheckTempTableSql<T>() where T : TableBase<T>, new()
        {

            string tableName = TableBase<T>.tableName;
            string schemaName = TableBase<T>.schemaName;
            if (tableName.Equals(string.Empty))
                return string.Empty;
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.AppendLine($"drop table if exists {schemaName}.temp_{tableName};");
            sqlBuilder.AppendLine($"create table {schemaName}.temp_{tableName} as select * from {schemaName}.{tableName} with no data;");

            return sqlBuilder.SqlLog();
        }
        public static void DivideInsertUpdate<T>(List<T> originalList, List<T> resultList, out List<T> insertList, out List<T> updateList, out string keyName) where T : TableBase<T>
        {
            DataFunctions.GetPropertiesFromAttribute<PrimaryKeyAttribute, T>(out var keys);
            PropertyInfo keyProp = keys.First();

            if (originalList.Count > 0)
            {
                List<string> keyList = [];

                foreach (T vo in originalList)
                    keyList.Add(keyProp.GetValue(vo)?.ToString() ?? throw new Exception("키 없음"));
                insertList = [.. resultList.Where(vo => !keyList.Contains(keyProp.GetValue(vo)?.ToString() ?? throw new Exception("키 없음")))];
                updateList = [.. resultList.Where(vo =>  keyList.Contains(keyProp.GetValue(vo)?.ToString() ?? throw new Exception("키 없음")))];
            }
            else
            {
                insertList = resultList;
                updateList = [];
            }
            keyName = keyProp.Name;
        }

        public static string InsertListSql<T>(this IList<T> list) where T : TableBase<T>, new()
        {
            StringBuilder sqlBuilder = new();
            if (list == null || list.Count == 0)
                Console.WriteLine("empty list");
            else
            {
                sqlBuilder.AppendLine($"insert into {list.Target()}");
                //column name 입력
                sqlBuilder.AppendLine($"({string.Join(sepComma, DataFunctions.GetColumnNames<T>())})values");

                //dataset 입력
                List<string> rowSets = [];
                foreach (var vo in list)
                    rowSets.Add(vo.Value(DataFunctions.GetProperties<T>(), true));

                sqlBuilder.Append(string.Join(sepCn, rowSets));
            }
            sqlBuilder.AppendLine(";");

            return sqlBuilder.SqlLog();
        }

        public static string UpdateDTOSql<T>(this T DTO, string keyName) where T : TableBase<T>, new()
        {
            StringBuilder sqlBuilder = new();

            //column name 입력
            typeof(T).GetProperties(out var properties);
            PropertyInfo keyCol = properties.Where(x => x.Name == keyName).First();

            List<string> colSets = [];

            foreach (var prop in properties.Where(x => x.Name != keyName && !Attribute.IsDefined(x, typeof(FixedValueAttribute))))
                if (!prop.NullCheck(DTO, out var colValue))
                    colSets.Add($"{prop.GetCustomAttribute<ColumnNameAttribute>()?.ColumnName ?? prop.Name} = '{colValue}'");

            sqlBuilder.AppendLine($"update {DTO.Target()} set");
            sqlBuilder.AppendLine(string.Join(sepSet, colSets));
            sqlBuilder.Append($"where {keyName} = '{keyCol.GetValue(DTO, null)}';");

            return sqlBuilder.SqlLog();
        }

        public static string UpdateListSql<T>(this T DTO, bool isInputDate) where T : TableBase<T>, new()
        {
            StringBuilder sqlBuilder = new();
            sqlBuilder.AppendLine($"update {DTO.Target()} set");
            //column name 입력
            TableBase<T>.GetProperties.FilterAttribute<PrimaryKeyAttribute, T>(out var cols, out var keys);
            if (keys.Count() == 0)
                throw new Exception("PrimaryKey 속성을 가진 column이 없습니다");

            List<string> colSets = cols.ColumnNameValueSets(DTO, true);
            List<string> keySets = keys.ColumnNameValueSets(DTO, false);

            if (isInputDate)
                colSets.Add("input_date = now()");

            sqlBuilder.AppendLine(string.Join(sepSet, colSets));
            sqlBuilder.AppendLine($"where{string.Join(sepWhereAnd, keySets)};");

            return sqlBuilder.SqlLog();
        }

        public static string UpsertListSql<T>(this IList<T> list, bool isInputDate) where T : TableBase<T>, new()
        {
            StringBuilder sqlBuilder = new();
            if (list == null || list.Count == 0)
            {
                Console.WriteLine("empty list");
                return ";";
            }
            else
            {
                // 스키마 및 테이블명 정의
                string schemaName = TableBase<T>.schemaName;
                string target = $"{schemaName}.{TableBase<T>.tableName}";
                string tempTableName = $"temp_{TableBase<T>.tableName}";

                // column name 입력
                string colNames = string.Join(sepComma, DataFunctions.GetColumnNames<T>());
                PropertyInfo[] properties = DataFunctions.GetProperties<T>();

                // dataset 입력
                List<string> rowSets = [];
                foreach (var vo in list)
                {
                    string rowSet = vo.Value(properties, true);
                    if (rowSet == "")
                        continue;
                    else
                        rowSets.Add(rowSet);

                }


                //where 절에 들어갈 key col과 set절에 들어갈 value col 분리
                string tempTable = "news";
                string targetTable = "target";
                string updatedRows = "updated";
                properties.FilterAttribute<PrimaryKeyAttribute, T>(out var values, out var keys);
                List<string> valueCol = values.TableColSets(tempTable);
                List<string> keyCol = keys.TableColSets(targetTable, tempTable);

                if (isInputDate)
                    valueCol.Add("input_date = now()");

                //임시 테이블 입력
                sqlBuilder.AppendLine($"insert into {schemaName}.{tempTableName}");
                sqlBuilder.AppendLine($"({colNames}) VALUES ");
                sqlBuilder.Append(string.Join(sepCn, rowSets));
                sqlBuilder.AppendLine(";");

                // upsert 진행
                // update
                sqlBuilder.AppendLine($"with {updatedRows} as(");
                sqlBuilder.AppendLine($"update {target} as {targetTable}");
                sqlBuilder.AppendLine($"set\t{string.Join(sepSet, valueCol)}");
                sqlBuilder.AppendLine($"from {schemaName}.{tempTableName} as {tempTable}");
                sqlBuilder.AppendLine($"where\t{string.Join(sepWhereAnd, keyCol)}");
                sqlBuilder.AppendLine("returning *");
                sqlBuilder.AppendLine(")");

                // update 진행되지 않은 행 insert 진행
                sqlBuilder.AppendLine($"insert into {target}");
                sqlBuilder.AppendLine($"\t\t({colNames})");
                sqlBuilder.AppendLine($"select\t {colNames} ");
                sqlBuilder.AppendLine($"from {schemaName}.{tempTableName}");
                sqlBuilder.AppendLine($"where not exists(select * from {updatedRows});");

                //임시테이블 데이터 제거
                sqlBuilder.AppendLine($"truncate table {schemaName}.{tempTableName};");
            }
            return sqlBuilder.SqlLog();
        }

        public static string InsertDataTableSql<T>(this DataTable dt, string schemaName, string tableName) where T : TableBase<T>
        {
            StringBuilder sqlBuilder = new();
            if (dt == null || dt.Rows.Count == 0)
            {
                Console.WriteLine("empty list");
                return ";";
            }
            else
            {
                sqlBuilder.AppendLine($"insert into {schemaName}.{tableName}");
                //column name 입력
                List<string> cols = [];
                typeof(T).GetProperties(out var properties);
                foreach (DataColumn col in dt.Columns)
                    if (properties.Select(x => x.Name).Contains(col.ColumnName))
                        cols.Add(col.ColumnName);
                sqlBuilder.AppendLine($"({string.Join(sepComma, cols)}) values");

                //dataset 입력
                List<string> rowSets = [];
                foreach (DataRow row in dt.Rows)
                {
                    List<string> rowValues = [];
                    foreach (DataColumn col in dt.Columns)
                    {
                        if (!properties.Select(x => x.Name).Contains(col.ColumnName))
                            continue;
                        object colValue = row[col];

                        if (NullCheck(colValue))
                            rowValues.Add("NULL");
                        else
                            rowValues.Add($"'{colValue}'");
                    }
                    rowSets.Add($"({string.Join(sepComma, rowValues)})");
                }
                sqlBuilder.Append(string.Join(sepCn, rowSets));
            }
            sqlBuilder.AppendLine(";");

            return sqlBuilder.SqlLog();
        }

        public static string SelectFuncSql<Tresult, Vsearch>(this Vsearch vs, IEnumerable<string> wheres) where Tresult : new() where Vsearch : Function<Vsearch>
        {
            StringBuilder sqlBuilder = vs.SelectFuncSqlMain<Tresult, Vsearch>();
            sqlBuilder.AppendWheres(wheres);

            return sqlBuilder.SqlLog();
        }

        public static string SelectFuncSql<Tresult, Vsearch>(this Vsearch vs) where Tresult : new() where Vsearch : Function<Vsearch>
        {

            StringBuilder sqlBuilder = vs.SelectFuncSqlMain<Tresult, Vsearch>();
            sqlBuilder.AppendLine(";");

            return sqlBuilder.SqlLog();
        }

        public static StringBuilder SelectFuncSqlMain<Tresult, Vsearch>(this Vsearch vs) where Tresult : new() where Vsearch : Function<Vsearch>
        {
            StringBuilder sqlBuilder = new();

            typeof(Vsearch).GetProperties(out var searProps);

            sqlBuilder.AppendLine($"select {string.Join(sepComma, DataFunctions.GetColumnNames<Vsearch>())}");
            sqlBuilder.Append($"from {Function<Vsearch>.schemaName}.{Function<Vsearch>.FunctionName}{vs.Value(searProps, false)}");

            return sqlBuilder;
        }

        public static string SelectTableSql<T>(IEnumerable<string> wheres) where T : TableBase<T>, new()
        {
            StringBuilder sqlBuilder = SelectTableSqlMain<T>();
            sqlBuilder.AppendWheres(wheres);

            return sqlBuilder.SqlLog();
        }

        public static string SelectTableSql<T>() where T : TableBase<T>, new()
        {
            StringBuilder sqlBuilder = SelectTableSqlMain<T>();
            sqlBuilder.AppendLine(";");

            return sqlBuilder.SqlLog();
        }

        public static StringBuilder SelectTableSqlMain<T>() where T : TableBase<T>, new()
        {
            StringBuilder sqlBuilder = new();
            if (TableBase<T>.tableName.Equals(string.Empty) || TableBase<T>.tableName.Equals(""))
                throw new Exception("tableName 속성이 비어있습니다");
            string[] colNamePropNameArray =[.. DataFunctions.GetColumnNames<T>().Zip(typeof(T).GetPropNames(), (col, prop) => col == prop ? prop : $"{col} as {prop}")];

            sqlBuilder.AppendLine($"select {string.Join(sepComma, colNamePropNameArray)}");
            sqlBuilder.Append($"from {Target<T>()}");
            
            return sqlBuilder;
        }

        public static void AppendWheres(this StringBuilder sqlBuilder, IEnumerable<string> wheres)
        {
            if (!wheres.Any())
                sqlBuilder.AppendLine(";");
            else
            {
                sqlBuilder.AppendLine($"\nwhere\t{string.Join(sepWhereAnd, wheres)};");
            }
        }



        public static bool NullCheck(object? val)
        {
            if (val == null || val == DBNull.Value)
                return true;
            else
                return double.TryParse(val.ToString(), out double convVal) && convVal == DataFunctions.nullDouble;
        }
        public static bool NullCheck<T>(this PropertyInfo prop, T VO, out object? val)
        {
            val = prop.GetValue(VO);
            return NullCheck(val);
        }

        public static bool NullCheck<T>(this PropertyInfo prop, T VO)
        {
            return NullCheck(prop.GetValue(VO, null));
        }
    }


    public abstract class Schemas<T> where T : class// : DBProperties
    {
        public static string schemaName = "public";
        public static PropertyInfo[] GetProperties { get => DataFunctions.GetProperties<T>(); }
    }
    [Table]
    public abstract class TableBase<T> : Schemas<TableBase<T>> where T : class
    {
        public static string tableName = string.Empty;
        public static bool inputDate = false;
        internal static DBProperties db = MDB.Instance;
        public virtual string GetTableShortName() { return tableName; }
        public virtual string Target() { return $"{schemaName}.{tableName}"; }

        internal static void InitializeMeta()
        {
            var tables = Assembly.GetExecutingAssembly()
                .GetTypes()
                .Where(t => t.GetCustomAttribute<TableAttribute>() != null);

            foreach (var table in tables)
                System.Runtime.CompilerServices.RuntimeHelpers.RunClassConstructor(table.TypeHandle);
        }
    }

    public abstract class Function<T> : Schemas<Function<T>>
    {
        public static string FunctionName = string.Empty;
        public virtual string? GetFunctionShortName() { return FunctionName; }
    }

    public abstract class DBProperties
    {
        // schemas
        public string schemaName = "public";

        // tables
        internal string tableA = string.Empty;

        internal string tableB = string.Empty;


        // functions
        //public static string asdfFunction = @"fd_fasdf({0},{1})"
    }

    public class MDB : DBProperties
    {
        public static MDB Instance { get; } = new MDB();
        private MDB()
        {
            // schemas
            schemaName = "public";

            // tables
            tableA = "tbl_a";

            tableB = "tbl_b";
        }


    }
}
