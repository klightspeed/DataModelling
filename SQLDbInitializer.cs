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

    public class EntityMappingPair
    {
        public IEntityMap Old;
        public IEntityMap New;
    }

    public class ColumnDefPair
    {
        public IColumnDef Old;
        public IColumnDef New;
    }

    public abstract class SQLDbInitializer
    {
        public bool AllowDataLoss { get; set; }
        public bool AllowTableCopy { get; set; }
        public abstract SQLQuoteType IdentifierQuoteType { get; }
        public abstract bool SupportsCreateTableWithConstraints { get; }
        public abstract bool SupportsAlterColumn { get; }
        public abstract bool SupportsDropColumn { get; }
        public abstract bool SupportsAddDropConstraint { get; }
        public abstract bool ColumnDataIsVariant { get; }

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

        protected virtual IEnumerable<IUniqueKeyMap> GetAddedUniqueKeys(IEntityMap map, IEntityMap original)
        {
            return map == null ? new IUniqueKeyMap[0] : map.UniqueKeys.Where(ak => original == null || !original.UniqueKeys.Any(oak => oak.KeyName == ak.KeyName && oak.Equals(ak)));
        }

        protected virtual IEnumerable<IUniqueKeyMap> GetDroppedUniqueKeys(IEntityMap map, IEntityMap original)
        {
            return original == null ? new IUniqueKeyMap[0] : original.UniqueKeys.Where(ak => map == null || !map.UniqueKeys.Any(nak => nak.KeyName == ak.KeyName && nak.Equals(ak)));
        }

        protected virtual IEnumerable<IForeignKeyMap> GetAddedForeignKeys(IEntityMap map, IEntityMap original)
        {
            return map == null ? new IForeignKeyMap[0] : map.ForeignKeys.Where(fk => original == null || !original.ForeignKeys.Any(ofk => ofk.KeyName == fk.KeyName && ofk.Equals(fk)));
        }

        protected virtual IEnumerable<IForeignKeyMap> GetDroppedForeignKeys(IEntityMap map, IEntityMap original)
        {
            return original == null ? new IForeignKeyMap[0] : original.ForeignKeys.Where(fk => map == null || !map.ForeignKeys.Any(nfk => nfk.KeyName == fk.KeyName && nfk.Equals(fk)));
        }

        protected virtual IEnumerable<IColumnDef> GetCopiedColumns(IEntityMap map, IEntityMap original)
        {
            return (original == null || map == null) ? new IColumnDef[0] : map.Columns.Where(c => original.Columns.Any(oc => oc.Column.Name == c.Column.Name)).Select(c => c.Column);
        }

        protected virtual IEnumerable<IColumnDef> GetAddedColumns(IEntityMap map, IEntityMap original)
        {
            return (original == null || map == null) ? new IColumnDef[0] : map.Columns.Where(c => !original.Columns.Any(oc => oc.Column.Name == c.Column.Name)).Select(c => c.Column);
        }

        protected virtual IEnumerable<IColumnDef> GetDroppedColumns(IEntityMap map, IEntityMap original)
        {
            return (original == null || map == null) ? new IColumnDef[0] : original.Columns.Where(c => !map.Columns.Any(nc => nc.Column.Name == c.Column.Name)).Select(c => c.Column);
        }

        protected virtual IEnumerable<ColumnDefPair> GetChangedColumns(IEntityMap map, IEntityMap original)
        {
            return (original == null || map == null) ? new ColumnDefPair[0] : map.Columns.Select(c => new ColumnDefPair { New = c.Column, Old = original.Columns.Where(oc => oc.Column.Name == c.Column.Name).Select(oc => oc.Column).SingleOrDefault() }).Where(t => t.Old != null && !t.New.Equals(t.Old));
        }
        
        protected virtual IEnumerable<IColumnDef> GetBreakingChangedColumns(IEntityMap map, IEntityMap original)
        {
            if (map != null && original != null)
            {
                return GetChangedColumns(map, original)
                    .Where(c =>
                        c.New.Type.DataType != c.Old.Type.DataType ||
			            c.New.Type.Length < c.Old.Type.Length ||
			            c.New.Type.Precision < c.Old.Type.Precision ||
			            c.New.Type.Scale < c.Old.Type.Scale
                    )
                    .Select(c => c.New);
            }
            else
            {
                return new IColumnDef[0];
            }
        }

        protected virtual IEnumerable<IColumnDef> GetNonBreakingChangedColumns(IEntityMap map, IEntityMap original)
        {
            if (map != null && original != null)
            {
                return GetChangedColumns(map, original)
                    .Where(c =>
                        c.New.Type.DataType == c.Old.Type.DataType &&
                        c.New.Type.Length >= c.Old.Type.Length &&
                        c.New.Type.Precision >= c.Old.Type.Precision &&
                        c.New.Type.Scale >= c.Old.Type.Scale
                    )
                    .Select(c => c.New);
            }
            else
            {
                return new IColumnDef[0];
            }
        }

        protected virtual IEnumerable<IIndexMap> GetAddedIndexes(IEntityMap map, IEntityMap original)
        {
            return map == null ? new IIndexMap[0] : map.Indexes.Where(ix => original == null || !original.Indexes.Any(oix => oix.KeyName == ix.KeyName && oix.Equals(ix)));
        }

        protected virtual IEnumerable<IIndexMap> GetDroppedIndexes(IEntityMap map, IEntityMap original)
        {
            return original == null ? new IIndexMap[0] : original.Indexes.Where(ix => map == null || !map.Indexes.Any(nix => nix.KeyName == ix.KeyName && nix.Equals(ix)));
        }

        protected virtual bool RequireCopyRenameTable(IEntityMap map, IEntityMap original)
        {
            if (map != null && original != null)
            {
                if (!SupportsAlterColumn)
                {
                    if (GetNonBreakingChangedColumns(map, original).Count() != 0)
                    {
                        if (AllowTableCopy || !ColumnDataIsVariant)
                        {
                            return true;
                        }
                    }
                }

                if (!SupportsDropColumn)
                {
                    if (GetBreakingChangedColumns(map, original).Count() != 0)
                    {
                        if (AllowTableCopy || !ColumnDataIsVariant)
                        {
                            return true;
                        }
                    }

                    if (GetDroppedColumns(map, original).Count() != 0)
                    {
                        if (AllowDataLoss && AllowTableCopy)
                        {
                            return true;
                        }
                    }
                }

                if (!SupportsAddDropConstraint)
                {
                    if (GetAddedUniqueKeys(map, original).Count() != 0 ||
                        GetDroppedUniqueKeys(map, original).Count() != 0 ||
                        GetAddedForeignKeys(map, original).Count() != 0 ||
                        GetDroppedUniqueKeys(map, original).Count() != 0)
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        protected virtual IEnumerable<string> DDLColumnDefinitions(IEnumerable<IColumnDef> cols)
        {
            foreach (IColumnDef coldef in cols)
            {
                yield return String.Format("{0} {1}",
                    EscapeColumnName(coldef.Name),
                    GetTypeName(coldef.Type)
                );
            }
        }

        protected virtual IEnumerable<string> DDLDropColumns(IEntityMap map, IEntityMap original)
        {
            if (!AllowDataLoss && SupportsDropColumn)
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
            if (SupportsDropColumn)
            {
                foreach (IColumnDef coldef in GetBreakingChangedColumns(map, original))
                {
                    if (!AllowDataLoss) throw new InvalidOperationException("Change would result in data loss");
                    yield return String.Format("ALTER TABLE {0} DROP COLUMN {1}",
                        EscapeTableName(map.TableName),
                        EscapeColumnName(coldef.Name)
                    );
                }

                foreach (string coldef in DDLColumnDefinitions(GetBreakingChangedColumns(map, original)))
                {
                    yield return String.Format("ALTER TABLE {0} ADD {1}",
                        EscapeTableName(map.TableName),
                        coldef
                    );
                }
            }

            if (SupportsAlterColumn)
            {
                foreach (string coldef in DDLColumnDefinitions(GetNonBreakingChangedColumns(map, original)))
                {
                    yield return String.Format("ALTER TABLE {0} ALTER COLUMN {1}",
                        EscapeTableName(map.TableName),
                        coldef
                    );
                }
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
            if (original != null && SupportsAddDropConstraint)
            {
                foreach (IUniqueKeyMap dropkey in GetDroppedUniqueKeys(map, original))
                {
                    yield return String.Format("ALTER TABLE {0} DROP CONSTRAINT {1}",
                        EscapeTableName(original.TableName),
                        dropkey.KeyName
                    );
                }
            }
        }

        protected virtual IEnumerable<string> DDLUniqueKeyConstraints(IEnumerable<IUniqueKeyMap> keys)
        {
            foreach (IUniqueKeyMap key in keys)
            {
                yield return String.Format("CONSTRAINT {0} {1} ({2})",
                    key.KeyName,
                    key is IPrimaryKeyMap ? "PRIMARY KEY" : "UNIQUE",
                    String.Join(", ", key.Columns.Select(c => EscapeColumnName(c.Name)).ToArray())
                );
            }
        }

        protected virtual IEnumerable<string> DDLAddUniqueKeys(IEntityMap map, IEntityMap original)
        {
            if (map != null && (original != null || !SupportsCreateTableWithConstraints) && SupportsAddDropConstraint)
            {
                foreach (string constraint in DDLUniqueKeyConstraints(GetAddedUniqueKeys(map, original)))
                {
                    yield return String.Format("ALTER TABLE {0} ADD {1}",
                        EscapeTableName(map.TableName),
                        constraint
                    );
                }
            }
        }
        
        protected virtual IEnumerable<string> DDLDropForeignKeys(IEntityMap map, IEntityMap original)
        {
            if (original != null && SupportsAddDropConstraint)
            {
                foreach (IForeignKeyMap dropkey in GetDroppedForeignKeys(map, original))
                {
                    yield return String.Format("ALTER TABLE {0} DROP CONSTRAINT {1}",
                        EscapeTableName(original.TableName),
                        dropkey.KeyName
                    );
                }
            }
        }

        protected virtual IEnumerable<string> DDLForeignKeyConstraints(IEnumerable<IForeignKeyMap> keys)
        {
            foreach (IForeignKeyMap addkey in keys)
            {
                yield return String.Format("CONSTRAINT {0} FOREIGN KEY ({1}) REFERENCES {2} ({3})",
                    addkey.KeyName,
                    String.Join(", ", addkey.Columns.Select(c => EscapeColumnName(c.Name)).ToArray()),
                    addkey.ReferencedKey.Columns.Table.TableName,
                    String.Join(", ", addkey.ReferencedKey.Columns.Select(c => EscapeColumnName(c.Name)).ToArray())
                );
            }
        }

        protected virtual IEnumerable<string> DDLAddForeignKeys(IEntityMap map, IEntityMap original)
        {
            if (map != null && (original != null || !SupportsCreateTableWithConstraints) && SupportsAddDropConstraint)
            {
                foreach (string constraint in DDLForeignKeyConstraints(GetAddedForeignKeys(map, original)))
                {
                    yield return String.Format("ALTER TABLE {0} ADD {1}",
                        EscapeTableName(map.TableName),
                        constraint
                    );
                }
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

        protected virtual IEnumerable<string> DDLRenameTable(string oldname, string newname)
        {
            if (newname != oldname)
            {
                yield return String.Format("ALTER TABLE {0} RENAME TO {1}",
                    EscapeTableName(oldname),
                    EscapeTableName(newname)
                );
            }
        }

        protected virtual IEnumerable<string> DDLCopyTable(IEntityMap map, IEntityMap original, string copytoname)
        {
            if (copytoname != map.TableName)
            {
                yield return String.Format("INSERT INTO {0} SELECT {1} FROM {2}",
                    EscapeTableName(copytoname),
                    String.Join(", ", GetCopiedColumns(map, original).Select(c => EscapeColumnName(c.Name))),
                    map.TableName
                );
            }
        }

        protected virtual IEnumerable<string> DDLCopyAndRenameTable(IEntityMap map, IEntityMap original)
        {
            if (RequireCopyRenameTable(map, original))
            {
                if (!AllowTableCopy)
                {
                    throw new InvalidOperationException("Change would require copying data to new table");
                }

                if (!ColumnDataIsVariant && !AllowDataLoss && GetBreakingChangedColumns(map, original).Count() != 0)
                {
                    throw new InvalidOperationException("Change would result in data loss");
                }

                if (!AllowDataLoss && GetDroppedColumns(map, original).Count() != 0)
                {
                    throw new InvalidOperationException("Change would result in data loss");
                }

                string newtablename = map.TableName + "_copy_" + Guid.NewGuid().ToString().Substring(16, 8);

                return DDLAddTable(map, null, newtablename)
                    .Concat(DDLCopyTable(map, original, newtablename))
                    .Concat(DDLDropTable(null, original))
                    .Concat(DDLRenameTable(newtablename, map.TableName));
            }
            else
            {
                return new string[0];
            }
        }

        protected virtual IEnumerable<string> DDLAddTable(IEntityMap map, IEntityMap original, string tablename = null)
        {
            if (original == null)
            {
                yield return String.Format("CREATE TABLE {0} ({1})",
                    EscapeTableName(tablename ?? map.TableName),
                    String.Join(", ", 
                        map.Columns.Select(c =>
                            String.Format("{0} {1}",
                                EscapeColumnName(c.Column.Name), GetTypeName(c.Column.Type)
                            )
                        ).Concat(
                            SupportsCreateTableWithConstraints ? DDLUniqueKeyConstraints(map.UniqueKeys) : new string[0]
                        ).Concat(
                            SupportsCreateTableWithConstraints ? DDLForeignKeyConstraints(map.ForeignKeys) : new string[0]
                        )
                    )
                );
            }
        }

        protected virtual IEnumerable<string> DDLBeforeCopyAndRenameTables(IEnumerable<EntityMappingPair> mappairs)
        {
            throw new NotImplementedException();
        }

        protected virtual IEnumerable<string> DDLAfterCopyAndRenameTables(IEnumerable<EntityMappingPair> mappairs)
        {
            throw new NotImplementedException();
        }
        
        protected virtual IEnumerable<string> DDLCopyAndRenameTables(IEnumerable<EntityMappingPair> mappairs)
        {
            if (mappairs.Any(m => RequireCopyRenameTable(m.New, m.Old)))
            {
                return DDLBeforeCopyAndRenameTables(mappairs)
                    .Concat(mappairs.SelectMany(p => DDLCopyAndRenameTable(p.New, p.Old)))
                    .Concat(DDLAfterCopyAndRenameTables(mappairs));
            }
            else
            {
                return new string[0];
            }
        }

        public virtual IEnumerable<string> GetDDL()
        {
            var mappairs = newmaps.Join(
                    oldmaps, 
                    m => m.TableName, 
                    m => m.TableName,
                    (n, o) => new EntityMappingPair { New = n, Old = o }
                )
                .Concat(
                    oldmaps.Where(o => !newmaps.Any(n => n.TableName == o.TableName))
                        .Select(o => new EntityMappingPair { New = null as IEntityMap, Old = o })
                )
                .Concat(
                    newmaps.Where(n => !oldmaps.Any(o => o.TableName == n.TableName))
                        .Select(n => new EntityMappingPair { New = n, Old = null as IEntityMap })
                )
                .ToList();

            return mappairs.SelectMany(p => DDLDropIndexes(p.New, p.Old))
                .Concat(mappairs.SelectMany(p => DDLDropForeignKeys(p.New, p.Old)))
                .Concat(mappairs.SelectMany(p => DDLDropUniqueKeys(p.New, p.Old)))
                .Concat(mappairs.SelectMany(p => DDLDropTable(p.New, p.Old)))
                .Concat(mappairs.SelectMany(p => DDLDropColumns(p.New, p.Old)))
                .Concat(mappairs.SelectMany(p => DDLAlterColumns(p.New, p.Old)))
                .Concat(mappairs.SelectMany(p => DDLAddColumns(p.New, p.Old)))
                .Concat(DDLCopyAndRenameTables(mappairs))
                .Concat(mappairs.SelectMany(p => DDLAddTable(p.New, p.Old)))
                .Concat(mappairs.SelectMany(p => DDLAddUniqueKeys(p.New, p.Old)))
                .Concat(mappairs.SelectMany(p => DDLAddForeignKeys(p.New, p.Old)))
                .Concat(mappairs.SelectMany(p => DDLAddIndexes(p.New, p.Old)));
        }
    }
}
