using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Linq.Expressions;
using System.Text;
using System.Reflection;

namespace TSVCEO.DataModelling
{
    public enum SQLQuoteType
    {
        Quote,
        Bracket,
        BackTick
    }

    public class SQLDbInitializer
    {
        public bool AllowDataLoss { get; set; }
        public virtual SQLQuoteType IdentifierQuoteType { get; set; }

        List<IEntityMap> newmaps;
        List<IEntityMap> oldmaps;

        public SQLDbInitializer(IEnumerable<IEntityMap> maps)
        {
            newmaps = maps.ToList();
            oldmaps = new List<IEntityMap>();
        }

        public SQLDbInitializer(IEnumerable<IEntityMap> maps, IEnumerable<IEntityMap> original)
        {
            newmaps = maps.ToList();
            oldmaps = original.ToList();
        }

        protected virtual string EscapeColumnName(string colname)
        {
            if (IdentifierQuoteType == SQLQuoteType.Bracket)
            {
                return "[" + colname + "]";
            }
            else if (IdentifierQuoteType == SQLQuoteType.BackTick)
            {
                return "`" + colname + "`";
            }
            else
            {
                return "\"" + colname + "\"";
            }
        }

        protected virtual string EscapeTableName(string tablename)
        {
            if (IdentifierQuoteType == SQLQuoteType.Bracket)
            {
                return "[" + tablename + "]";
            }
            else if (IdentifierQuoteType == SQLQuoteType.BackTick)
            {
                return "`" + tablename + "`";
            }
            else
            {
                return "\"" + tablename + "\"";
            }
        }

        protected virtual string GetBaseTypeName(IColumnType type)
        {
            switch (type.DataType)
            {
                case DbType.AnsiString: return String.Format("VARCHAR({0})", type.Length == null ? "MAX" : type.Length.ToString());
                case DbType.AnsiStringFixedLength: return String.Format("CHAR({0})", type.Length.ToString());
                case DbType.Binary: return String.Format("VARBINARY({0})", type.Length == null ? "MAX" : type.Length.ToString());
                case DbType.Boolean: return "BIT";
                case DbType.Byte: return "TINYINT";
                case DbType.Currency: return "MONEY";
                case DbType.Date: return "DATE";
                case DbType.DateTime: return "DATETIME";
                case DbType.DateTime2: return "DATETIME2";
                case DbType.DateTimeOffset: return "DATETIMEOFFSET";
                case DbType.Decimal: return String.Format("DECIMAL({0},{1})", type.Precision, type.Scale);
                case DbType.Double: return "FLOAT";
                case DbType.Guid: return "UNIQUEIDENTIFIER";
                case DbType.Int16: return "SMALLINT";
                case DbType.Int32: return "INT";
                case DbType.Int64: return "BIGINT";
                case DbType.Single: return "REAL";
                case DbType.String: return String.Format("NVARCHAR({0})", type.Length == null ? "MAX" : type.Length.ToString());
                case DbType.StringFixedLength: return String.Format("NCHAR({0})", type.Length);
                case DbType.Time: return "TIME";
                default: throw new InvalidOperationException();
            }
        }

        protected virtual string GetTypeName(IColumnType type)
        {
            return String.Format("{0} {1}",
                GetBaseTypeName(type),
                type.IsNullable ? "NULL" : "NOT NULL"
            );
        }

        protected IEnumerable<IUniqueKeyMap> GetAddedUniqueKeys(IEntityMap map, IEntityMap original)
        {
            return map == null ? new IUniqueKeyMap[0] : map.UniqueKeys.Where(ak => original == null || !original.UniqueKeys.Any(oak => oak.KeyName == ak.KeyName && oak.Equals(ak)));
        }

        protected IEnumerable<IUniqueKeyMap> GetDroppedUniqueKeys(IEntityMap map, IEntityMap original)
        {
            return original == null ? new IUniqueKeyMap[0] : original.UniqueKeys.Where(ak => map == null || !map.UniqueKeys.Any(nak => nak.KeyName == ak.KeyName && nak.Equals(ak)));
        }

        protected IEnumerable<IForeignKeyMap> GetAddedForeignKeys(IEntityMap map, IEntityMap original)
        {
            return map == null ? new IForeignKeyMap[0] : map.ForeignKeys.Where(fk => original == null || !original.ForeignKeys.Any(ofk => ofk.KeyName == fk.KeyName && ofk.Equals(fk)));
        }

        protected IEnumerable<IForeignKeyMap> GetDroppedForeignKeys(IEntityMap map, IEntityMap original)
        {
            return original == null ? new IForeignKeyMap[0] : original.ForeignKeys.Where(fk => map == null || !map.ForeignKeys.Any(nfk => nfk.KeyName == fk.KeyName && nfk.Equals(fk)));
        }

        protected IEnumerable<IColumnDef> GetAddedColumns(IEntityMap map, IEntityMap original)
        {
            return (original == null || map == null) ? new IColumnDef[0] : map.Columns.Where(c => !original.Columns.Any(oc => oc.Column.Name == c.Column.Name)).Select(c => c.Column);
        }

        protected IEnumerable<IColumnDef> GetDroppedColumns(IEntityMap map, IEntityMap original)
        {
            return (original == null || map == null) ? new IColumnDef[0] : original.Columns.Where(c => !map.Columns.Any(nc => nc.Column.Name == c.Column.Name)).Select(c => c.Column);
        }

        protected IEnumerable<Tuple<IColumnDef, IColumnDef>> GetChangedColumns(IEntityMap map, IEntityMap original)
        {
            return (original == null || map == null) ? new Tuple<IColumnDef, IColumnDef>[0] : map.Columns.Select(c => new Tuple<IColumnDef, IColumnDef>(c.Column, original.Columns.Where(oc => oc.Column.Name == c.Column.Name).Select(oc => oc.Column).SingleOrDefault())).Where(t => t.Item2 != null && !t.Item1.Equals(t.Item2));
        }
        
        protected IEnumerable<IColumnDef> GetBreakingChangedColumns(IEntityMap map, IEntityMap original)
        {
            if (map != null && original != null)
            {
                return GetChangedColumns(map, original)
                    .Where(c =>
                        c.Item1.Type.DataType != c.Item2.Type.DataType ||
			c.Item1.Type.Length < c.Item2.Type.Length ||
			c.Item1.Type.Precision < c.Item2.Type.Precision ||
			c.Item1.Type.Scale < c.Item2.Type.Scale
                    )
                    .Select(c => c.Item1);
            }
            else
            {
                return new IColumnDef[0];
            }
        }

        protected IEnumerable<IColumnDef> GetNonBreakingChangedColumns(IEntityMap map, IEntityMap original)
        {
            if (map != null && original != null)
            {
                return GetChangedColumns(map, original)
                    .Where(c =>
                        c.Item1.Type.DataType == c.Item2.Type.DataType &&
                        c.Item1.Type.Length >= c.Item2.Type.Length &&
                        c.Item1.Type.Precision >= c.Item2.Type.Precision &&
                        c.Item1.Type.Scale >= c.Item2.Type.Scale
                    )
                    .Select(c => c.Item1);
            }
            else
            {
                return new IColumnDef[0];
            }
        }

        protected IEnumerable<IIndexMap> GetAddedIndexes(IEntityMap map, IEntityMap original)
        {
            return map == null ? new IIndexMap[0] : map.Indexes.Where(ix => original == null || !original.Indexes.Any(oix => oix.KeyName == ix.KeyName && oix.Equals(ix)));
        }

        protected IEnumerable<IIndexMap> GetDroppedIndexes(IEntityMap map, IEntityMap original)
        {
            return original == null ? new IIndexMap[0] : original.Indexes.Where(ix => map == null || !map.Indexes.Any(nix => nix.KeyName == ix.KeyName && nix.Equals(ix)));
        }

        protected virtual IEnumerable<string> DDLDropColumns(IEntityMap map, IEntityMap original)
        {
            if (!AllowDataLoss)
            {
                foreach (IColumnDef coldef in GetDroppedColumns(map, original))
                {
                    yield return String.Format("ALTER TABLE {0} DROP COLUMN {1}",
                        EscapeTableName(original.TableName),
                        EscapeColumnName(coldef.Name)
                    );
                }
            }
        }

        protected virtual IEnumerable<string> DDLAlterColumns(IEntityMap map, IEntityMap original)
        {
            foreach (IColumnDef coldef in GetBreakingChangedColumns(map, original))
            {
                if (!AllowDataLoss) throw new InvalidOperationException("Change would result in data loss");
                yield return String.Format("ALTER TABLE {0} DROP COLUMN {1}",
                    EscapeTableName(map.TableName),
                    EscapeColumnName(coldef.Name)
                );
                yield return String.Format("ALTER TABLE {0} ADD {1} {2}",
                    EscapeTableName(map.TableName),
                    EscapeColumnName(coldef.Name),
                    GetTypeName(coldef.Type)
                );
            }

            foreach (IColumnDef coldef in GetNonBreakingChangedColumns(map, original))
            {
                yield return String.Format("ALTER TABLE {0} ALTER COLUMN {1} {2}",
                    EscapeTableName(map.TableName),
                    EscapeColumnName(coldef.Name),
                    GetTypeName(coldef.Type)
                );
            }
        }

        protected virtual IEnumerable<string> DDLAddColumns(IEntityMap map, IEntityMap original)
        {
            foreach (IColumnDef coldef in GetAddedColumns(map, original))
            {
                yield return String.Format("ALTER TABLE {0} ADD {1} {2}",
                    EscapeTableName(map.TableName),
                    EscapeColumnName(coldef.Name),
                    GetTypeName(coldef.Type)
                );
            }
        }
        
        protected virtual IEnumerable<string> DDLDropUniqueKeys(IEntityMap map, IEntityMap original)
        {
            foreach (IUniqueKeyMap dropkey in GetDroppedUniqueKeys(map, original))
            {
                yield return String.Format("ALTER TABLE {0} DROP CONSTRAINT {1}",
                    EscapeTableName(original.TableName),
                    dropkey.KeyName
                );
            }
        }

        protected virtual IEnumerable<string> DDLAddUniqueKeys(IEntityMap map, IEntityMap original)
        {
            foreach (IUniqueKeyMap addkey in GetAddedUniqueKeys(map, original))
            {
                yield return String.Format("ALTER TABLE {0} ADD CONSTRAINT {1} {2} ({3})",
                    EscapeTableName(map.TableName),
                    addkey.KeyName,
                    addkey is IPrimaryKeyMap ? "PRIMARY KEY" : "UNIQUE",
                    String.Join(", ", addkey.Columns.Select(c => EscapeColumnName(c.Name)).ToArray())
                );
            }
        }
        
        protected virtual IEnumerable<string> DDLDropForeignKeys(IEntityMap map, IEntityMap original)
        {
            foreach (IForeignKeyMap dropkey in GetDroppedForeignKeys(map, original))
            {
                yield return String.Format("ALTER TABLE {0} DROP CONSTRAINT {1}",
                    EscapeTableName(original.TableName),
                    dropkey.KeyName
                );
            }
        }

        protected virtual IEnumerable<string> DDLAddForeignKeys(IEntityMap map, IEntityMap original)
        {
            foreach (IForeignKeyMap addkey in GetAddedForeignKeys(map, original))
            {
                yield return String.Format("ALTER TABLE {0} ADD CONSTRAINT {1} FOREIGN KEY ({2}) REFERENCES {3} ({4})",
                    EscapeTableName(map.TableName),
                    addkey.KeyName,
                    String.Join(", ", addkey.Columns.Select(c => EscapeColumnName(c.Name)).ToArray()),
                    addkey.ReferencedKey.Columns.Table.TableName,
                    String.Join(", ", addkey.ReferencedKey.Columns.Select(c => EscapeColumnName(c.Name)).ToArray())
                );
            }
        }

        protected virtual IEnumerable<string> DDLDropIndexes(IEntityMap map, IEntityMap original)
        {
            foreach (IIndexMap index in GetDroppedIndexes(map, original))
            {
                yield return String.Format("DROP INDEX {0} ON {1}",
                    index.KeyName,
                    EscapeTableName(original.TableName)
                );
            }
        }

        protected virtual IEnumerable<string> DDLAddIndexes(IEntityMap map, IEntityMap original)
        {
            foreach (IIndexMap index in GetAddedIndexes(map, original))
            {
                yield return String.Format("CREATE INDEX {0} ON {1} ({2})",
                    index.KeyName,
                    EscapeTableName(map.TableName),
                    String.Join(", ", index.Columns.Select(c => EscapeColumnName(c.Name)).ToArray())
                );
            }
        }

        protected virtual IEnumerable<string> DDLDropTable(IEntityMap map, IEntityMap original)
        {
            if (map == null && AllowDataLoss)
            {
                yield return String.Format("DROP TABLE {0}",
                    EscapeTableName(original.TableName)
                );
            }
        }

        protected virtual IEnumerable<string> DDLAddTable(IEntityMap map, IEntityMap original)
        {
            if (original == null)
            {
                yield return String.Format("CREATE TABLE {0} ({1})",
                    EscapeTableName(map.TableName),
                    String.Join(", ", 
                        map.Columns.Select(c =>
                            String.Format("{0} {1}",
                                EscapeColumnName(c.Column.Name), GetTypeName(c.Column.Type)
                            )
                        )
                    )
                );
            }
        }

        public IEnumerable<string> GetDDL()
        {
            var mappairs = newmaps.Join(
                    oldmaps, 
                    m => m.TableName, 
                    m => m.TableName,
                    (n, o) => new Tuple<IEntityMap, IEntityMap>(n, o)
                )
                .Union(
                    oldmaps.Where(o => !newmaps.Any(n => n.TableName == o.TableName))
                        .Select(o => new Tuple<IEntityMap, IEntityMap>(null, o))
                )
                .Union(
                    newmaps.Where(n => !oldmaps.Any(o => o.TableName == n.TableName))
                        .Select(n => new Tuple<IEntityMap, IEntityMap>(n, null))
                )
                .Select(p => new { newmap = p.Item1, oldmap = p.Item2 })
                .ToList();

            return mappairs.SelectMany(p => DDLDropIndexes(p.newmap, p.oldmap))
                .Union(mappairs.SelectMany(p => DDLDropForeignKeys(p.newmap, p.oldmap)))
                .Union(mappairs.SelectMany(p => DDLDropUniqueKeys(p.newmap, p.oldmap)))
                .Union(mappairs.SelectMany(p => DDLDropTable(p.newmap, p.oldmap)))
                .Union(mappairs.SelectMany(p => DDLDropColumns(p.newmap, p.oldmap)))
                .Union(mappairs.SelectMany(p => DDLAlterColumns(p.newmap, p.oldmap)))
                .Union(mappairs.SelectMany(p => DDLAddColumns(p.newmap, p.oldmap)))
                .Union(mappairs.SelectMany(p => DDLAddTable(p.newmap, p.oldmap)))
                .Union(mappairs.SelectMany(p => DDLAddUniqueKeys(p.newmap, p.oldmap)))
                .Union(mappairs.SelectMany(p => DDLAddForeignKeys(p.newmap, p.oldmap)))
                .Union(mappairs.SelectMany(p => DDLAddIndexes(p.newmap, p.oldmap)));
        }


    }
}
