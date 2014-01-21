using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Data;
using System.Diagnostics;

namespace TSVCEO.DataModelling
{
    public class DeferredActionException : Exception
    {
        private string _StackTrace;

        public DeferredActionException(string message, Exception innerException, string stacktrace)
            : base(message, innerException)
        {
            _StackTrace = stacktrace;
        }

        public override string StackTrace { get { return _StackTrace; } }
    }

    public abstract class EntityMapper
    {
        public IEntityMap EntityMap;
        public EntityMapBuilder Maps;

        protected List<Tuple<string, Action>> DeferredForeignKeyActions = new List<Tuple<string, Action>>();
        protected List<Tuple<string, Action>> DeferredManyToOneActions = new List<Tuple<string, Action>>();

        protected void AddDeferredForeignKeyAction(Action action)
        {
            DeferredForeignKeyActions.Add(new Tuple<string, Action>(new StackTrace(true).ToString(), action));
        }

        protected void AddDeferredManyToOneAction(Action action)
        {
            DeferredManyToOneActions.Add(new Tuple<string, Action>(new StackTrace(true).ToString(), action));
        }

        public void ExecuteDeferredForeignKeyActions()
        {
            foreach (Tuple<string, Action> action in DeferredForeignKeyActions)
            {
                try
                {
                    action.Item2();
                }
                catch (Exception ex)
                {
                    throw new DeferredActionException(ex.Message, ex, action.Item1);
                }
            }
        }

        public void ExecuteDeferredManyToOneActions()
        {
            foreach (Tuple<string, Action> action in DeferredManyToOneActions)
            {
                try
                {
                    action.Item2();
                }
                catch (Exception ex)
                {
                    throw new DeferredActionException(ex.Message, ex, action.Item1);
                }
            }
        }
    }

    public class EntityMapper<TEntity> : EntityMapper where TEntity : class
    {
        public EntityMapper(IEntityMap map, EntityMapBuilder maps)
        {
            this.EntityMap = map;
            this.Maps = maps;
        }

        protected IEnumerable<LambdaExpression> GetSelectors(LambdaExpression selectors)
        {
            if (selectors.Body is NewExpression)
            {
                return ((NewExpression)selectors.Body).Arguments.Select(ex => Expression.Lambda(ex, selectors.Parameters)).ToArray();
            }
            else
            {
                return new LambdaExpression[] { selectors };
            }
        }

        public IColumnMap AddColumn<TProperty>(Expression<Func<TEntity, TProperty>> propsel, Action<IColumnMap> colmap = null)
        {
            string colname;
            
            colname = ((MemberExpression)propsel.Body).Member.Name;

            var col = new ColumnMap(EntityMap, propsel, colname);

            if (colmap != null)
            {
                colmap(col);
            }

            EntityMap.Columns.Add(col);

            return col;
        }

        public IColumnMap AddColumn<TTargetEntity, TIdProperty>(Expression<Func<TEntity, TTargetEntity>> propsel, Expression<Func<TTargetEntity, TIdProperty>> idpropsel, Action<IColumnMap> colmap = null)
        {
            string colname = ((MemberExpression)propsel.Body).Member.Name + "_" + ((MemberExpression)idpropsel.Body).Member.Name;
            Type idproptype = ((PropertyInfo)((MemberExpression)idpropsel.Body).Member).PropertyType;
            var col = new ColumnMap(EntityMap, propsel, colname, idproptype);

            if (colmap != null)
            {
                colmap(col);
            }

            EntityMap.Columns.Add(col);

            return col;
        }

        public IPrimaryKeyMap IdColumn<TProperty>(Expression<Func<TEntity, TProperty>> propsel, Action<IColumnMap> colmap = null)
        {
            string colname = ((MemberExpression)propsel.Body).Member.Name;
            IColumnMap col = AddColumn(propsel, colmap);
            IPrimaryKeyMap prikey = new PrimaryKeyMap(EntityMap, "PK_" + typeof(TEntity).Name, col.Column);
            EntityMap.PrimaryKey = prikey;

            return prikey;
        }

        protected IUniqueKeyMap AddUniqueKey(string keyname, IEnumerable<LambdaExpression> columns)
        {
            var uq = new UniqueKeyMap(EntityMap, "AK_" + typeof(TEntity).Name + "_" + keyname);
            List<IColumnDef> coldefs = new List<IColumnDef>();
            foreach (LambdaExpression colsel in columns)
            {
                string propname = ((MemberExpression)colsel.Body).Member.Name;
                uq.Columns.Add(EntityMap.Columns.Single(c => c.Column.PropertyName == propname).Column);
            }

            EntityMap.UniqueKeys.Add(uq);

            return uq;
        }
        
        public IUniqueKeyMap AddUniqueKey<TOut>(string keyname, Expression<Func<TEntity, TOut>> colsels)
        {
            return AddUniqueKey(keyname, GetSelectors(colsels));
        }

        protected void AddForeignKeyInternal<TTargetEntity, TIdProperty>(
            Expression<Func<TEntity, TTargetEntity>> propsel, 
            Expression<Func<TTargetEntity, TIdProperty>> idpropsel, 
            IColumnMap fkcolumn, 
            IUniqueKeyMap uniquekey, 
            Action<IColumnMap> idcolmap = null, 
            Action<IForeignKeyMap> fkmap = null
        )
        {
            string foreignkeyname = ((MemberExpression)propsel.Body).Member.Name;
            string idpropname = ((MemberExpression)idpropsel.Body).Member.Name;
            var fk = new ForeignKeyMap(this.EntityMap, propsel, fkcolumn.Column, "FK_" + typeof(TEntity).Name + "_" + foreignkeyname);

            fk.ReferencedKey = uniquekey;

            fk.Columns.Add(fkcolumn.Column);

            if (fkmap != null)
            {
                fkmap(fk);
            }

            EntityMap.ForeignKeys.Add(fk);
        }

        protected void AddForeignKeyInternal<TTargetEntity, TIdProperty, TOut>(
            Expression<Func<TEntity, TTargetEntity>> propsel, 
            Expression<Func<TTargetEntity, TIdProperty>> idpropsel, 
            IColumnMap fkcolumn, 
            IUniqueKeyMap uniquekey, 
            Expression<Func<TEntity, TOut>> colsels, 
            Action<IColumnMap> idcolmap = null, 
            Action<IForeignKeyMap> fkmap = null)
        {
            AddForeignKeyInternal(propsel, idpropsel, fkcolumn, uniquekey, idcolmap: idcolmap, fkmap: fk =>
            {
                foreach (LambdaExpression colsel in GetSelectors(colsels))
                {
                    string propname;

                    if (colsel.Body is MemberExpression)
                    {
                        propname = ((MemberExpression)colsel.Body).Member.Name;
                    }
                    else if (colsel.Body is ParameterExpression)
                    {
                        propname = EntityMap.PrimaryKey.KeyColumn.PropertyName;
                    }
                    else
                    {
                        throw new InvalidOperationException("Column selector is neither a property nor this.");
                    }

                    fk.Columns.Add(EntityMap.Columns.Single(c => c.Column.PropertyName == propname).Column);
                }

                if (fkmap != null)
                {
                    fkmap(fk);
                }
            });
        }

        protected void AddForeignKeyDeferred<TTargetEntity, TIdProperty>(
            Expression<Func<TEntity, TTargetEntity>> propsel, 
            Expression<Func<TTargetEntity, TIdProperty>> idpropsel, 
            IColumnMap fkcolumn, 
            Action<IColumnMap> idcolmap = null, 
            Action<IForeignKeyMap> fkmap = null)
            where TTargetEntity : class
        {
            string foreignkeyname = ((MemberExpression)propsel.Body).Member.Name;
            string idpropname = ((MemberExpression)idpropsel.Body).Member.Name;

            var tgt = Maps.GetMapForType<TTargetEntity>();

            if (tgt != null)
            {
                var uniquekey = tgt.PrimaryKey;

                if (uniquekey == null)
                {
                    throw new InvalidOperationException("Target entity does not have a primary key");
                }

                AddForeignKeyInternal(propsel, idpropsel, fkcolumn, uniquekey, idcolmap: idcolmap, fkmap: fkmap);

                return;
            }

            throw new InvalidOperationException("Foreign Key mapping failed");
        }

        public void AddForeignKey<TTargetEntity, TIdProperty>(Expression<Func<TEntity, TTargetEntity>> propsel, Expression<Func<TTargetEntity, TIdProperty>> idpropsel, Expression<Func<TEntity, Nullable<TIdProperty>>> fkpropsel, Action<IColumnMap> idcolmap = null, Action<IForeignKeyMap> fkmap = null)
            where TTargetEntity : class
            where TIdProperty : struct
        {
            string foreignkeyname = ((MemberExpression)propsel.Body).Member.Name;
            string idpropname = ((MemberExpression)idpropsel.Body).Member.Name;

            IColumnMap fkcolumn;

            if (fkpropsel == null)
            {
                fkcolumn = AddColumn(propsel, idpropsel, idcolmap);
            }
            else
            {
                fkcolumn = AddColumn(fkpropsel, idcolmap);
            }

            AddDeferredForeignKeyAction(() => AddForeignKeyDeferred(propsel, idpropsel, fkcolumn, idcolmap, fk => { fk.IsOptional = true; if (fkmap != null) fkmap(fk); }));
        }

        public void AddForeignKey<TTargetEntity, TIdProperty>(Expression<Func<TEntity, TTargetEntity>> propsel, Expression<Func<TTargetEntity, TIdProperty>> idpropsel, Expression<Func<TEntity, TIdProperty>> fkpropsel, Action<IColumnMap> idcolmap = null, Action<IForeignKeyMap> fkmap = null)
            where TTargetEntity : class
        {
            string foreignkeyname = ((MemberExpression)propsel.Body).Member.Name;
            string idpropname = ((MemberExpression)idpropsel.Body).Member.Name;
            string idcolname = foreignkeyname;

            IColumnMap fkcolumn;

            if (fkpropsel == null)
            {
                fkcolumn = AddColumn(propsel, idpropsel, idcolmap);
            }
            else
            {
                fkcolumn = AddColumn(fkpropsel, idcolmap);
            }

            AddDeferredForeignKeyAction(() => AddForeignKeyDeferred(propsel, idpropsel, fkcolumn, idcolmap, fkmap));
        }

        protected void AddForeignKeyMultiColumnDeferred<TTargetEntity, TIdProperty, TOut>(Expression<Func<TEntity, TTargetEntity>> propsel, Expression<Func<TTargetEntity, TIdProperty>> idpropsel, IColumnMap fkcolumn, Expression<Func<TEntity, TOut>> columns, Expression<Func<TTargetEntity, TOut>> tgtcolumns, Action<IColumnMap> idcolmap = null, Action<IForeignKeyMap> fkmap = null)
            where TTargetEntity : class
        {
            IEntityMap tgt = Maps.GetMapForType<TTargetEntity>();
            EntityMapper<TTargetEntity> tgtmapper = Maps.GetMapperForType<TTargetEntity>();
            string keyname = typeof(TEntity).Name + "_" + ((MemberExpression)(propsel.Body)).Member.Name;

            if (tgt != null)
            {
                IUniqueKeyMap uniquekey = tgtmapper.AddUniqueKey(keyname, new LambdaExpression[] { idpropsel }.Union(GetSelectors(tgtcolumns)));

                if (uniquekey == null)
                {
                    throw new InvalidOperationException("Unique key creation failed");
                }

                AddForeignKeyInternal(propsel, idpropsel, fkcolumn, uniquekey, columns, idcolmap, fkmap);
                return;
            }

            throw new InvalidOperationException("Foreign Key mapping failed");
        }

        public void AddForeignKeyMultiColumn<TTargetEntity, TIdProperty, TOut>(Expression<Func<TEntity, TTargetEntity>> propsel, Expression<Func<TTargetEntity, TIdProperty>> idpropsel, Expression<Func<TEntity, TIdProperty>> fkpropsel, Expression<Func<TEntity, TOut>> columns, Expression<Func<TTargetEntity, TOut>> tgtcolumns, Action<IColumnMap> idcolmap = null, Action<IForeignKeyMap> fkmap = null)
            where TTargetEntity : class
        {
            string foreignkeyname = ((MemberExpression)propsel.Body).Member.Name;
            string idpropname = ((MemberExpression)idpropsel.Body).Member.Name;
            string idcolname = foreignkeyname;

            IColumnMap fkcolumn;

            if (fkpropsel == null)
            {
                fkcolumn = AddColumn(propsel, idpropsel, idcolmap);
            }
            else
            {
                fkcolumn = AddColumn(fkpropsel, idcolmap);
            }

            AddDeferredForeignKeyAction(() => AddForeignKeyMultiColumnDeferred(propsel, idpropsel, fkcolumn, columns, tgtcolumns, idcolmap, fkmap));
        }

        public void AddForeignKeyMultiColumn<TTargetEntity, TIdProperty, TOut>(Expression<Func<TEntity, TTargetEntity>> propsel, Expression<Func<TTargetEntity, TIdProperty>> idpropsel, Expression<Func<TEntity, Nullable<TIdProperty>>> fkpropsel, Expression<Func<TEntity, TOut>> columns, Expression<Func<TTargetEntity, TOut>> tgtcolumns, Action<IColumnMap> idcolmap = null, Action<IForeignKeyMap> fkmap = null)
            where TTargetEntity : class
            where TIdProperty : struct
        {
            string foreignkeyname = ((MemberExpression)propsel.Body).Member.Name;
            string idpropname = ((MemberExpression)idpropsel.Body).Member.Name;
            string idcolname = foreignkeyname;

            IColumnMap fkcolumn;

            if (fkpropsel == null)
            {
                fkcolumn = AddColumn(propsel, idpropsel, idcolmap);
            }
            else
            {
                fkcolumn = AddColumn(fkpropsel, idcolmap);
            }

            AddDeferredForeignKeyAction(() => AddForeignKeyMultiColumnDeferred(propsel, idpropsel, fkcolumn, columns, tgtcolumns, idcolmap, fk => { fk.IsOptional = true; if (fkmap != null) fkmap(fk); }));
        }

        protected void AddManyToOneMapDeferred<TTargetEntity>(Expression<Func<TEntity, ICollection<TTargetEntity>>> propsel, Expression<Func<TTargetEntity, TEntity>> fksel)
        {
            string foreignkeyname = ((MemberExpression)fksel.Body).Member.Name;
            var tgt = Maps.GetMapForType(typeof(TTargetEntity));

            if (tgt != null)
            {
                var fk = tgt.ForeignKeys.SingleOrDefault(k => k.KeyName == "FK_" + typeof(TTargetEntity).Name + "_" + foreignkeyname);

                if (fk != null)
                {
                    var mto = new ManyToOneMap(EntityMap, propsel);
                    mto.HasMany = true;
                    mto.ForeignKey = fk;
                    fk.ManyToOneMap = mto;
                    EntityMap.ManyToOneMappings.Add(mto);

                    return;
                }
            }

            throw new InvalidOperationException("Many-To-One mapping failed");
        }

        public void AddManyToOneMap<TTargetEntity>(Expression<Func<TEntity, ICollection<TTargetEntity>>> propsel, Expression<Func<TTargetEntity, TEntity>> fksel)
        {
            AddDeferredManyToOneAction(() => AddManyToOneMapDeferred(propsel, fksel));
        }

        protected void AddOneToOneMapDeferred<TTargetEntity>(Expression<Func<TEntity, TTargetEntity>> propsel, Expression<Func<TTargetEntity, TEntity>> fksel)
        {
            string foreignkeyname = ((MemberExpression)fksel.Body).Member.Name;
            var tgt = Maps.GetMapForType(typeof(TTargetEntity));

            if (tgt != null)
            {
                var fk = tgt.ForeignKeys.SingleOrDefault(k => k.KeyName == "FK_" + typeof(TTargetEntity).Name + "_" + foreignkeyname);

                if (fk != null)
                {
                    var mto = new ManyToOneMap(EntityMap, propsel);
                    mto.HasMany = false;
                    mto.ForeignKey = fk;
                    fk.ManyToOneMap = mto;
                    EntityMap.ManyToOneMappings.Add(mto);

                    return;
                }
            }
        }

        public void AddOneToOneMap<TTargetEntity>(Expression<Func<TEntity, TTargetEntity>> propsel, Expression<Func<TTargetEntity, TEntity>> fksel)
        {
            AddDeferredManyToOneAction(() => AddOneToOneMapDeferred(propsel, fksel));
        }
   
        public IFullTextIndexMap AddFullTextIndex<TProperty>(Expression<Func<TEntity, TProperty>> propsel)
        {
            string keyname = "IX_" + typeof(TEntity).Name + "__FullText";

            IFullTextIndexMap index = new FullTextIndexMap(EntityMap, keyname);

            foreach (LambdaExpression selector in GetSelectors(propsel))
            {
                string colname = ((MemberExpression)selector.Body).Member.Name;
                index.Columns.Add(EntityMap.Columns.Single(c => c.Column.Name == colname).Column);
            }

            EntityMap.FullTextIndex = index;
            return index;
        }
    }

    public abstract class EntityMapBuilder : IDatabaseSeed
    {
        public virtual string Name
        {
            get
            {
                return null;
            }
        }

        public EntityMapBuilder()
        {
        }

        protected List<EntityMapper> Mappers;

        public EntityMapper<TEntity> GetMapperForType<TEntity>()
            where TEntity : class
        {
            return this.Mappers.SingleOrDefault(m => m.EntityMap.EntityType == typeof(TEntity)) as EntityMapper<TEntity>;
        }

        public IEntityMap GetMapForType<TEntity>()
        {
            return GetMapForType(typeof(TEntity));
        }

        public IEntityMap GetMapForType(Type type)
        {
            EntityMapper mapper = this.Mappers.SingleOrDefault(m => m.EntityMap.EntityType == type);
            if (mapper == null)
            {
                return null;
            }
            else
            {
                return mapper.EntityMap;
            }
        }

        protected void AddTable<TEntity>(string tablename, Action<EntityMapper<TEntity>> map)
            where TEntity : class
        {
            var ent = new EntityMap(typeof(TEntity), tablename);
            var mapper = new EntityMapper<TEntity>(ent, this);

            map(mapper);

            this.Mappers.Add(mapper);
        }

        protected void AddTable<TEntity>(Action<EntityMapper<TEntity>> map)
            where TEntity : class
        {
            string tablename = typeof(TEntity).Name + "s";
            AddTable<TEntity>(tablename, map);
        }

        protected abstract void FillMaps();

        public IEntityMap[] GetMaps()
        {
            if (this.Mappers == null)
            {
                this.Mappers = new List<EntityMapper>();

                FillMaps();

                foreach (EntityMapper mapper in Mappers)
                {
                    mapper.ExecuteDeferredForeignKeyActions();
                }

                foreach (EntityMapper mapper in Mappers)
                {
                    mapper.ExecuteDeferredManyToOneActions();
                }
            }

            return Mappers.Select(m => m.EntityMap).ToArray();
        }

        public abstract void Seed(IDataSession session);
    }

    public class EntityMap : IEntityMap
    {
        public Type EntityType { get; protected set; }

        public string TableName { get; protected set; }

        public IColumnMap IdColumn
        {
            get
            {
                return UniqueKeys.OfType<IPrimaryKeyMap>().Join(Columns, pk => pk.KeyColumn, c => c.Column, (pk, c) => c).SingleOrDefault();
            }
            set
            {
                PrimaryKey = new PrimaryKeyMap(value.Table, "PK_" + value.Table.TableName, value.PropertyRef as IColumnRef);
            }
        }

        public IPrimaryKeyMap PrimaryKey
        {
            get
            {
                return UniqueKeys.OfType<IPrimaryKeyMap>().SingleOrDefault();
            }
            set
            {
                foreach (IPrimaryKeyMap pk in UniqueKeys.OfType<IPrimaryKeyMap>().ToArray())
                {
                    UniqueKeys.Remove(pk);
                }
                    
                UniqueKeys.Add(value);
            }
        }

        public IEnumerable<IPropertyMap> Properties { get { return Columns.OfType<IPropertyMap>().Union(ForeignKeys.OfType<IPropertyMap>()).Union(ManyToOneMappings.OfType<IPropertyMap>()); } }
        public ICollection<IColumnMap> Columns { get; private set; }
        public ICollection<IForeignKeyMap> ForeignKeys { get; private set; }
        public ICollection<IManyToOneMap> ManyToOneMappings { get; private set; }
        public ICollection<IUniqueKeyMap> UniqueKeys { get; private set; }
        public ICollection<IIndexMap> Indexes { get; private set; }
        public IFullTextIndexMap FullTextIndex { get; set; }

        public EntityMap(string tablename)
        {
            this.TableName = tablename;
            this.Columns = new ColumnMapCollection(this);
            this.ForeignKeys = new ForeignKeyMapCollection(this);
            this.ManyToOneMappings = new ManyToOneMapCollection(this);
            this.UniqueKeys = new UniqueKeyMapCollection(this);
            this.Indexes = new IndexMapCollection(this);
        }

        public EntityMap(Type type, string tablename)
            : this(tablename)
        {
            this.EntityType = type;
        }

        public override string ToString()
        {
            return "[" + TableName + "]";
        }
    }

    public class ColumnType : IColumnType
    {
        public Type CLRType { get; private set; }
        public DbType DataType { get; private set; }
        public bool IsNullable { get; set; }
        public int? Length { get; set; }
        public int? Precision { get; set; }
        public int? Scale { get; set; }

        public ColumnType(Type type)
        {
            CLRType = type;
            IsNullable = false;

            SetDataType(type);
        }

        private void SetDataType(Type type)
        {
            if (type.IsGenericType && type.Name == "Nullable`1")
            {
                IsNullable = true;
                SetDataType(type.GetGenericArguments()[0]);
            }
            else if (type.IsEnum)
            {
                SetDataType(type.GetEnumUnderlyingType());
            }
            else if (type == typeof(Boolean))
            {
                DataType = DbType.Boolean;
            }
            else if (type == typeof(Byte))
            {
                DataType = DbType.Byte;
            }
            else if (type == typeof(DateTime))
            {
                DataType = DbType.DateTime;
            }
            else if (type == typeof(DateTimeOffset))
            {
                DataType = DbType.DateTimeOffset;
            }
            else if (type == typeof(Decimal))
            {
                DataType = DbType.Decimal;
            }
            else if (type == typeof(Double))
            {
                DataType = DbType.Double;
            }
            else if (type == typeof(Guid))
            {
                DataType = DbType.Guid;
            }
            else if (type == typeof(Int16))
            {
                DataType = DbType.Int16;
            }
            else if (type == typeof(Int32))
            {
                DataType = DbType.Int32;
            }
            else if (type == typeof(Int64))
            {
                DataType = DbType.Int64;
            }
            else if (type == typeof(SByte))
            {
                DataType = DbType.SByte;
            }
            else if (type == typeof(Single))
            {
                DataType = DbType.Single;
            }
            else if (type == typeof(UInt16))
            {
                DataType = DbType.UInt16;
            }
            else if (type == typeof(UInt32))
            {
                DataType = DbType.UInt32;
            }
            else if (type == typeof(UInt64))
            {
                DataType = DbType.UInt64;
            }
            else if (type == typeof(String))
            {
                DataType = DbType.String;
            }
            else if (type == typeof(Char[]))
            {
                DataType = DbType.StringFixedLength;
            }
            else if (type == typeof(Char))
            {
                DataType = DbType.StringFixedLength;
                Length = 1;
            }
            else if (type == typeof(Byte[]))
            {
                DataType = DbType.Binary;
            }
            else
            {
                DataType = DbType.Object;
            }
        }

        public bool Equals(IColumnType other)
        {
            return other != null &&
                    this.DataType == other.DataType &&
                    this.IsNullable == other.IsNullable &&
                    this.Length == other.Length &&
                    this.Precision == other.Precision &&
                    this.Scale == other.Scale;
        }

        public override bool Equals(object other)
        {
            return other != null &&
                    other is IColumnType &&
                    this.Equals(other);
        }

        public override int GetHashCode()
        {
            return this.DataType.GetHashCode() ^ this.IsNullable.GetHashCode() ^ this.Length.GetHashCode() ^ this.Precision.GetHashCode() ^ this.Scale.GetHashCode();
        }
    }

    public class PropertyRef : IPropertyRef
    {
        public LambdaExpression Selector { get; set; }
        public IEntityMap Table { get; set; }
        public PropertyInfo MemberInfo { get { return Selector == null ? null : (PropertyInfo)((MemberExpression)Selector.Body).Member; } }
        public string PropertyName { get { return MemberInfo == null ? null : MemberInfo.Name; } }
        public Type EntityType { get { return MemberInfo == null ? null : MemberInfo.DeclaringType; } }
        public Type PropertyType { get { return MemberInfo == null ? null : MemberInfo.PropertyType; } }
        public virtual bool IsValid { get { return Selector != null; } }

        public PropertyRef(IEntityMap table, LambdaExpression selector)
        {
            this.Table = table;
            this.Selector = selector;
        }

        public PropertyRef()
        {
        }

        public virtual bool Equals(IPropertyRef other)
        {
            return other != null && this.EntityType == other.EntityType && this.PropertyName == other.PropertyName;
        }

        public override string ToString()
        {
            return EntityType.Name + "." + PropertyName + " <" + PropertyType.Name + ">";
        }
    }

    public class ColumnRef : PropertyRef, IColumnRef
    {
        public string Name { get; protected set; }
        public override bool IsValid { get { return Name != null && Table != null; } }

        public ColumnRef(IEntityMap table, LambdaExpression selector, string name)
            : base(table, selector)
        {
            this.Name = name;
        }

        public ColumnRef(IEntityMap table, string name)
            : base()
        {
            this.Table = table;
            this.Name = name;
        }

        public ColumnRef()
        {
        }

        public override bool Equals(IPropertyRef other)
        {
            if (other == null)
            {
                return false;
            }
            if (other is IColumnRef)
            {
                return Equals(other as IColumnRef);
            }
            else
            {
                return base.Equals(other);
            }
        }
            
        public virtual bool Equals(IColumnRef other)
        {
            return other != null && this.Table.TableName == other.Table.TableName && this.Name == other.Name;
        }

        public override string ToString()
        {
            return "[" + Table.TableName + "].[" + Name + "]";
        }
    }

    public class ColumnDef : ColumnRef, IColumnDef
    {
        public IColumnType Type { get; protected set; }

        public ColumnDef(IEntityMap table, LambdaExpression selector, string name, Type type)
            : base(table, selector, name)
        {
            this.Type = new ColumnType(type);
        }

        public ColumnDef(IEntityMap table, LambdaExpression selector, string name)
            : base(table, selector, name)
        {
            this.Type = new ColumnType(this.PropertyType);
        }

        public ColumnDef(IEntityMap table, string name, Type type)
            : base(table, name)
        {
            this.Type = new ColumnType(type);
        }

        public bool Equals(IColumnDef other)
        {
            return base.Equals(other as IColumnRef);
        }

        public override string ToString()
        {
            return "[" + Table.TableName + "].[" + Name + "] " + Type.DataType.ToString();
        }
    }

    public class ColumnRefCollection : IColumnRefCollection
    {
        public IEntityMap Table { get; protected set; }
        protected readonly ICollection<IColumnRef> Items = new HashSet<IColumnRef>();

        public ColumnRefCollection(IEntityMap entityMap)
        {
            this.Table = entityMap;
        }

        public int Count { get { return Items.Count; } }
        public bool IsReadOnly { get { return false; } }

        public void Add(IColumnRef item)
        {
            if (item.Table != this.Table) throw new ArgumentException("Item table is different to collection table");
            Items.Add(item);
        }

        public void Clear()
        {
            Items.Clear();
        }

        public bool Contains(IColumnRef item)
        {
            return Items.Contains(item);
        }

        public void CopyTo(IColumnRef[] array, int arrayIndex)
        {
            Items.CopyTo(array, arrayIndex);
        }

        public bool Remove(IColumnRef item)
        {
            return Items.Remove(item);
        }

        public IEnumerator<IColumnRef> GetEnumerator()
        {
            return Items.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public bool Equals(IColumnRefCollection other)
        {
            return other != null && this.Count == other.Count && this.Zip(other, (t, o) => new { t, o }).All(ka => ka.t.Equals(ka.o));
        }

        public override bool Equals(object other)
        {
            return this.Equals(other as IColumnRefCollection);
        }

        public override int GetHashCode()
        {
            return Items.Aggregate(0, (a, r) => a ^ r.GetHashCode());
        }
    }

    public class ColumnRefSingleton : IColumnRefCollection
    {
        public IEntityMap Table { get; protected set; }
        protected readonly IColumnRef ColumnRef;

        public ColumnRefSingleton(IEntityMap entityMap, IColumnRef colref)
        {
            this.Table = entityMap;
            this.ColumnRef = colref;
        }

        public int Count { get { return 1; } }
        public bool IsReadOnly { get { return true; } }

        public void Add(IColumnRef item)
        {
            throw new ReadOnlyException();
        }

        public void Clear()
        {
            throw new ReadOnlyException();
        }

        public bool Contains(IColumnRef item)
        {
            return this.ColumnRef.Equals(item);
        }

        public void CopyTo(IColumnRef[] array, int arrayIndex)
        {
            array[arrayIndex] = this.ColumnRef;
        }

        public bool Remove(IColumnRef item)
        {
            throw new ReadOnlyException();
        }

        public IEnumerator<IColumnRef> GetEnumerator()
        {
            yield return this.ColumnRef;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public bool Equals(IColumnRefCollection other)
        {
            return other != null && this.Count == other.Count && this.Zip(other, (t, o) => new { t, o }).All(ka => ka.t.Equals(ka.o));
        }

        public override bool Equals(object other)
        {
            return this.Equals(other as IColumnRefCollection);
        }

        public override int GetHashCode()
        {
            return this.ColumnRef.GetHashCode();
        }
    }

    public abstract class PropertyMap : IPropertyMap
    {
        public virtual IPropertyRef PropertyRef { get; protected set; }

        protected PropertyMap(IEntityMap table, LambdaExpression selector)
        {
            this.PropertyRef = new PropertyRef(table, selector);
        }

        protected PropertyMap(IPropertyRef propref)
        {
            this.PropertyRef = propref;
        }

        public override string ToString()
        {
            return PropertyRef.ToString();
        }
    }

    public class ColumnMap : PropertyMap, IColumnMap
    {
        public IEntityMap Table { get { return PropertyRef.Table; } }

        public ColumnMap(IEntityMap entityMap, LambdaExpression selector, string columnName, Type type)
            : base(new ColumnDef(entityMap, selector, columnName, type))
        {
        }

        public ColumnMap(IEntityMap entityMap, LambdaExpression selector, string columnName)
            : base(new ColumnDef(entityMap, selector, columnName))
        {
        }

        public ColumnMap(IEntityMap entityMap, string columnName, Type type)
            : base(new ColumnDef(entityMap, columnName, type))
        {
        }

        public IColumnDef Column { get { return PropertyRef as IColumnDef; } }

        public bool Equals(IColumnMap other)
        {
            return other != null && this.Column.Equals(other.Column);
        }

        public override bool Equals(object other)
        {
            return this.Equals(other as IColumnMap);
        }

        public override int GetHashCode()
        {
            return this.Column.GetHashCode();
        }
    }

    public class ColumnMapCollection : ICollection<IColumnMap>
    {
        protected readonly IEntityMap Table;
        protected readonly ICollection<IColumnMap> Items = new HashSet<IColumnMap>();

        public ColumnMapCollection(IEntityMap entityMap)
        {
            this.Table = entityMap;
        }

        public int Count { get { return Items.Count; } }
        public bool IsReadOnly { get { return false; } }

        public void Add(IColumnMap item)
        {
            if (item.Column.Table != this.Table) throw new ArgumentException("Item table is different to collection table");
            Items.Add(item);
        }

        public void Clear()
        {
            Items.Clear();
        }

        public bool Contains(IColumnMap item)
        {
            return Items.Contains(item);
        }

        public void CopyTo(IColumnMap[] array, int arrayIndex)
        {
            Items.CopyTo(array, arrayIndex);
        }

        public bool Remove(IColumnMap item)
        {
            return Items.Remove(item);
        }

        public IEnumerator<IColumnMap> GetEnumerator()
        {
            return Items.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    public class ForeignKeyMap : PropertyMap, IForeignKeyMap
    {
        public IEntityMap Table { get { return PropertyRef.Table; } }
        public string KeyName { get; private set; }
        public IColumnRefCollection Columns { get; private set; }
        public bool IsOptional { get; set; }
        public IColumnRef IdColumn { get; private set; }
        public IUniqueKeyMap ReferencedKey { get; set; }
        public IManyToOneMap ManyToOneMap { get; set; }

        public ForeignKeyMap(IEntityMap table, LambdaExpression selector, IColumnRef colref, string keyName)
            : base(table, selector)
        {
            this.IdColumn = colref;
            this.KeyName = keyName;
            this.Columns = new ColumnRefCollection(table);
        }
        
        public ForeignKeyMap(IEntityMap table, string idColumnName, string keyName)
            : base(new ColumnRef(table, idColumnName))
        {
            this.IdColumn = new ColumnRef(table, idColumnName);
            this.KeyName = keyName;
            this.Columns = new ColumnRefCollection(table);
        }

        public bool Equals(IForeignKeyMap other)
        {
            return other != null &&
                this.KeyName == other.KeyName &&
                this.Columns.Equals(other.Columns) &&
                this.ReferencedKey.Equals(other.ReferencedKey);
        }

        public override bool Equals(object other)
        {
            return this.Equals(other as IForeignKeyMap);
        }

        public override int GetHashCode()
        {
            return KeyName.GetHashCode();
        }
    }

    public class ForeignKeyMapCollection : ICollection<IForeignKeyMap>
    {
        protected readonly IEntityMap Table;
        protected readonly ICollection<IForeignKeyMap> Items = new HashSet<IForeignKeyMap>();

        public ForeignKeyMapCollection(IEntityMap entityMap)
        {
            this.Table = entityMap;
        }

        public int Count { get { return Items.Count; } }
        public bool IsReadOnly { get { return false; } }

        public void Add(IForeignKeyMap item)
        {
            if (item.Table != this.Table) throw new ArgumentException("Item table is different to collection table");
            Items.Add(item);
        }

        public void Clear()
        {
            Items.Clear();
        }

        public bool Contains(IForeignKeyMap item)
        {
            return Items.Contains(item);
        }

        public void CopyTo(IForeignKeyMap[] array, int arrayIndex)
        {
            Items.CopyTo(array, arrayIndex);
        }

        public bool Remove(IForeignKeyMap item)
        {
            return Items.Remove(item);
        }

        public IEnumerator<IForeignKeyMap> GetEnumerator()
        {
            return Items.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    public class ManyToOneMap : PropertyMap, IManyToOneMap
    {
        public IEntityMap Table { get { return PropertyRef.Table; } }
        public bool HasMany { get; set; }
        public IForeignKeyMap ForeignKey { get; set; }

        public ManyToOneMap(IEntityMap table, LambdaExpression selector)
            : base(table, selector)
        {
        }
    }

    public class ManyToOneMapCollection : ICollection<IManyToOneMap>
    {
        protected readonly IEntityMap EntityMap;
        protected readonly ICollection<IManyToOneMap> Items = new HashSet<IManyToOneMap>();

        public ManyToOneMapCollection(IEntityMap entityMap)
        {
            this.EntityMap = entityMap;
        }

        public int Count { get { return Items.Count; } }
        public bool IsReadOnly { get { return false; } }

        public void Add(IManyToOneMap item)
        {
            if (item.Table != this.EntityMap) throw new ArgumentException("Item table is different to collection table");
            Items.Add(item);
        }

        public void Clear()
        {
            Items.Clear();
        }

        public bool Contains(IManyToOneMap item)
        {
            return Items.Contains(item);
        }

        public void CopyTo(IManyToOneMap[] array, int arrayIndex)
        {
            Items.CopyTo(array, arrayIndex);
        }

        public bool Remove(IManyToOneMap item)
        {
            return Items.Remove(item);
        }

        public IEnumerator<IManyToOneMap> GetEnumerator()
        {
            return Items.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    public class UniqueKeyMap : IUniqueKeyMap
    {
        public IEntityMap Table { get { return Columns.Table; } }
        public string KeyName { get; private set; }
        public IColumnRefCollection Columns { get; private set; }
        public ICollection<IManyToOneMap> ManyToOneMaps { get; private set; }

        public UniqueKeyMap(IEntityMap table, string keyName)
        {
            this.KeyName = keyName;
            this.Columns = new ColumnRefCollection(table);
            this.ManyToOneMaps = new ManyToOneMapCollection(table);
        }

        public virtual bool Equals(IUniqueKeyMap other)
        {
            return other != null && this.Columns.Count == other.Columns.Count && this.Columns.Zip(other.Columns, (i, o) => new { i, o }).All(v => v.i.Equals(v.o));
        }
    }

    public class PrimaryKeyMap : IPrimaryKeyMap
    {
        public IEntityMap Table { get; private set; }
        public string KeyName { get; private set; }
        public IColumnRefCollection Columns { get { return new ColumnRefSingleton(Table, KeyColumn); } }
        public ICollection<IManyToOneMap> ManyToOneMaps { get; private set; }
        public IColumnRef KeyColumn { get; private set; }

        public PrimaryKeyMap(IEntityMap table, string keyName, IColumnRef keyColumn)
        {
            this.Table = table;
            this.KeyName = keyName;
            this.KeyColumn = keyColumn;
            this.ManyToOneMaps = new ManyToOneMapCollection(table);
        }

        public bool Equals(IUniqueKeyMap other)
        {
            return other != null && other.Columns.Count == 1 && this.KeyColumn.Equals(other.Columns.First());
        }

        public bool Equals(IPrimaryKeyMap other)
        {
            return other != null && this.KeyColumn.Equals(other.KeyColumn);
        }
    }

    public class UniqueKeyMapCollection : ICollection<IUniqueKeyMap>
    {
        protected readonly IEntityMap EntityMap;
        protected readonly ICollection<IUniqueKeyMap> Items = new HashSet<IUniqueKeyMap>();

        public UniqueKeyMapCollection(IEntityMap entityMap)
        {
            this.EntityMap = entityMap;
        }

        public int Count { get { return Items.Count; } }
        public bool IsReadOnly { get { return false; } }

        public void Add(IUniqueKeyMap item)
        {
            if (item.Columns.Table != this.EntityMap) throw new ArgumentException("Item table is different to collection table");
            Items.Add(item);
        }

        public void Clear()
        {
            Items.Clear();
        }

        public bool Contains(IUniqueKeyMap item)
        {
            return Items.Contains(item);
        }

        public void CopyTo(IUniqueKeyMap[] array, int arrayIndex)
        {
            Items.CopyTo(array, arrayIndex);
        }

        public bool Remove(IUniqueKeyMap item)
        {
            return Items.Remove(item);
        }

        public IEnumerator<IUniqueKeyMap> GetEnumerator()
        {
            return Items.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    public class IndexMap : IIndexMap
    {
        public IEntityMap Table { get; private set; }
        public string KeyName { get; private set; }
        public IColumnRefCollection Columns { get; private set; }

        public IndexMap(IEntityMap table, string keyName)
        {
            this.Table = table;
            this.KeyName = keyName;
            this.Columns = new ColumnRefCollection(table);
        }

        public virtual bool Equals(IIndexMap other)
        {
            return other != null && this.Table == other.Table && this.Columns.Count == other.Columns.Count && this.Columns.Zip(other.Columns, (i, o) => new { i, o }).All(v => v.i.Equals(v.o));
        }
    }

    public class FullTextIndexMap : IFullTextIndexMap
    {
        public IEntityMap Table { get; private set; }
        public string KeyName { get; private set; }
        public IColumnRefCollection Columns { get; private set; }

        public FullTextIndexMap(IEntityMap table, string keyName)
        {
            this.Table = table;
            this.KeyName = keyName;
            this.Columns = new ColumnRefCollection(table);
        }

        public virtual bool Equals(IFullTextIndexMap other)
        {
            return other != null && this.Table == other.Table && this.Columns.Count == other.Columns.Count && this.Columns.Zip(other.Columns, (i, o) => new { i, o }).All(v => v.i.Equals(v.o));
        }
    }

    public class IndexMapCollection : ICollection<IIndexMap>
    {
        protected readonly IEntityMap EntityMap;
        protected readonly ICollection<IIndexMap> Items = new HashSet<IIndexMap>();

        public IndexMapCollection(IEntityMap entityMap)
        {
            this.EntityMap = entityMap;
        }

        public int Count { get { return Items.Count; } }
        public bool IsReadOnly { get { return false; } }

        public void Add(IIndexMap item)
        {
            if (item.Columns.Table != this.EntityMap) throw new ArgumentException("Item table is different to collection table");
            Items.Add(item);
        }

        public void Clear()
        {
            Items.Clear();
        }

        public bool Contains(IIndexMap item)
        {
            return Items.Contains(item);
        }

        public void CopyTo(IIndexMap[] array, int arrayIndex)
        {
            Items.CopyTo(array, arrayIndex);
        }

        public bool Remove(IIndexMap item)
        {
            return Items.Remove(item);
        }

        public IEnumerator<IIndexMap> GetEnumerator()
        {
            return Items.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
