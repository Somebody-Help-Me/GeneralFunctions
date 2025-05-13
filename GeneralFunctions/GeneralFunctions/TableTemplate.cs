using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GeneralFunctions
{
    public class TableDTO : TableBase<TableDTO>
    {
        static TableDTO()
        {
            schemaName = db.schemaName;
            tableName = db.tableA;
        }
    }

    public class TableVO : TableBase<TableVO>
    {
        [IgnoreColumn]
        public static Dictionary<string, TableVO> VOMap { get; } = [];
        [IgnoreColumn]
        public static List<TableVO> VOList { get => [..VOMap.Values]; }
        static TableVO()
        {
            schemaName = db.schemaName;
            tableName = db.tableB;
        }
        private string _id = string.Empty;
        [PrimaryKey]
        public string Id
        {
            get => _id;
            set {
                _id = value;
                VOMap[_id] = this;
            }
        }
    }
}
