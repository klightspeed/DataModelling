using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Linq.Mapping;
using System.Xml.Linq;

namespace TSVCEO.DataModelling.Linq
{
    public class XmlMappingBuilder
    {
        protected static readonly XNamespace ns = "http://schemas.microsoft.com/linqtosql/mapping/2007";

        public static XmlMappingSource CreateMapping(IEnumerable<IEntityMap> maps, string dbname)
        {
            XDocument mapxml = CreateMappingXml(maps, dbname);
            return XmlMappingSource.FromXml(mapxml.ToString());
        }

        protected static XDocument CreateMappingXml(IEnumerable<IEntityMap> maps, string dbname)
        {
            return new XDocument(
                new XElement(ns + "Database",
                    new XAttribute("xmlns", ns.NamespaceName),
                    new XAttribute("Name", dbname),
                    maps.Select(m => CreateEntityMapping(m))
                )
            );
        }

        protected static XElement CreateEntityMapping(IEntityMap map)
        {
            string pkcolname = map.PrimaryKey.KeyColumn.Name;

            return new XElement(ns + "Table",
                new XAttribute("Name", map.TableName),
                new XAttribute("Member", map.EntityType.Name),
                new XElement(ns + "Type",
                    new XAttribute("Name", map.EntityType.FullName),
                    map.Columns.Select(c => CreateColumnMapping(c, pkcolname)),
                    map.ForeignKeys.Select(fk => CreateForeignKeyMapping(fk)),
                    map.ManyToOneMappings.Select(mm => CreateAssociationMapping(mm))
                )
            );
        }

        protected static XElement CreateColumnMapping(IColumnMap colmap, string pkcolname)
        {
            return new XElement(ns + "Column",
                new XAttribute("Name", colmap.Column.Name),
                new XAttribute("Member", colmap.PropertyRef.PropertyName),
                new XAttribute("IsPrimaryKey", (colmap.Column.Name == pkcolname) ? "True" : "False"),
                new XAttribute("CanBeNull", colmap.Column.Type.IsNullable)
            );
        }

        protected static XElement CreateForeignKeyMapping(IForeignKeyMap fkmap)
        {
            if (fkmap.PropertyRef.PropertyName != null)
            {
                return new XElement(ns + "Association",
                    new XAttribute("Member", fkmap.PropertyRef.PropertyName),
                    new XAttribute("ThisKey", fkmap.IdColumn.PropertyName),
                    new XAttribute("OtherKey", fkmap.ReferencedKey.Table.IdColumn.PropertyRef.PropertyName),
                    new XAttribute("IsForeignKey", "True")
                );
            }
            else
            {
                return null;
            }
        }

        protected static XElement CreateAssociationMapping(IManyToOneMap manymap)
        {
            if (manymap.PropertyRef.PropertyName != null)
            {
                return new XElement(ns + "Association",
                    new XAttribute("Member", manymap.PropertyRef.PropertyName),
                    new XAttribute("ThisKey", manymap.Table.IdColumn.PropertyRef.PropertyName),
                    new XAttribute("OtherKey", manymap.ForeignKey.IdColumn.PropertyName),
                    new XAttribute("IsUnique", manymap.HasMany ? "False" : "True")
                );
            }
            else
            {
                return null;
            }
        }
    }
}
