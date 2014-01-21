using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using System.Data;
using System.Reflection;

namespace TSVCEO.DataModelling
{
    #region Map Consumer Interfaces

    public interface IEntityMapConsumer
    {
        void Add(IEntityMap map);
    }

    #endregion

    #region Entity Map Interfaces

    public interface IEntityMap
    {
        Type EntityType { get; }
        string TableName { get; }

        IColumnMap IdColumn { get; set; }
        IPrimaryKeyMap PrimaryKey { get; set; }
        ICollection<IColumnMap> Columns { get; }
        ICollection<IForeignKeyMap> ForeignKeys { get; }
        ICollection<IManyToOneMap> ManyToOneMappings { get; }
        ICollection<IUniqueKeyMap> UniqueKeys { get; }
        ICollection<IIndexMap> Indexes { get; }
        IFullTextIndexMap FullTextIndex { get; set; }
        IEnumerable<IPropertyMap> Properties { get; }
    }

    public interface IColumnType : IEquatable<IColumnType>
    {
        Type CLRType { get; }
        DbType DataType { get; }
        bool IsNullable { get; set; }
        int? Length { get; set; }
        int? Precision { get; set; }
        int? Scale { get; set; }
    }

    public interface IPropertyRef : IEquatable<IPropertyRef>
    {
        LambdaExpression Selector { get; }
        string PropertyName { get; }
        Type PropertyType { get; }
        Type EntityType { get; }
        bool IsValid { get; }
        IEntityMap Table { get; }
    }

    public interface IColumnRef : IPropertyRef, IEquatable<IColumnRef>
    {
        string Name { get; }
    }

    public interface IColumnRefCollection : ICollection<IColumnRef>, IEquatable<IColumnRefCollection>
    {
        IEntityMap Table { get; }
    }

    public interface IColumnDef : IColumnRef
    {
        IColumnType Type { get; }
    }

    public interface IPropertyMap
    {
        IPropertyRef PropertyRef { get; }
    }

    public interface IColumnMap : IPropertyMap, IEquatable<IColumnMap>
    {
        IEntityMap Table { get; }
        IColumnDef Column { get; }
    }

    public interface IConstraintMap
    {
        IEntityMap Table { get; }
        string KeyName { get; }
        IColumnRefCollection Columns { get; }
    }

    public interface IForeignKeyMap : IConstraintMap, IPropertyMap, IEquatable<IForeignKeyMap>
    {
        bool IsOptional { get; set; }
        IColumnRef IdColumn { get; }
        IUniqueKeyMap ReferencedKey { get; set; }
        IManyToOneMap ManyToOneMap { get; set; }
    }

    public interface IUniqueKeyMap : IConstraintMap, IEquatable<IUniqueKeyMap>
    {
        ICollection<IManyToOneMap> ManyToOneMaps { get; }
    }

    public interface IPrimaryKeyMap : IUniqueKeyMap, IEquatable<IPrimaryKeyMap>
    {
        IColumnRef KeyColumn { get; }
    }

    public interface IManyToOneMap : IPropertyMap
    {
        IEntityMap Table { get; }
        bool HasMany { get; set; }
        IForeignKeyMap ForeignKey { get; set; }
    }

    public interface IIndexMap : IEquatable<IIndexMap>
    {
        IEntityMap Table { get; }
        string KeyName { get; }
        IColumnRefCollection Columns { get; }
    }

    public interface IFullTextIndexMap : IEquatable<IFullTextIndexMap>
    {
        IEntityMap Table { get; }
        string KeyName { get; }
        IColumnRefCollection Columns { get; }
    }

    #endregion
}
