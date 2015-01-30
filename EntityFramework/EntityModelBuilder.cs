using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Data.Entity;
using System.Data.Entity.ModelConfiguration;
using System.Data.Entity.ModelConfiguration.Configuration;
using System.Reflection;

namespace TSVCEO.DataModelling.EntityFramework
{
    public class EntityModelBuilder : IEntityMapConsumer
    {
        private ConfigurationRegistrar Configurations;

        private EntityModelBuilder(ConfigurationRegistrar configs)
        {
            this.Configurations = configs;
        }

        public static void Populate(ConfigurationRegistrar configs, IEnumerable<IEntityMap> maps)
        {
            EntityModelBuilder builder = new EntityModelBuilder(configs);

            foreach (IEntityMap map in maps)
            {
                builder.Add(map);
            }
        }

        public void Add(IEntityMap map)
        {
            MethodInfo method = new Action<IEntityMap>(this.Add<object>).Method.GetGenericMethodDefinition().MakeGenericMethod(map.EntityType);

            ParameterExpression param = Expression.Parameter(typeof(IEntityMap));

            ((Action<IEntityMap>)Expression.Lambda(
                Expression.Call(
                    Expression.Constant(this),
                    method,
                    param
                ),
                param
            ).Compile())(map);
        }

        public void Add<TEntity>(IEntityMap map)
            where TEntity : class
        {
            Configurations.Add(new EntityConfig<TEntity>(map));
        }
    }

    public class EntityConfig<TEntity> : EntityTypeConfiguration<TEntity>
        where TEntity : class
    {
        public EntityConfig(IEntityMap map)
        {
            HashSet<string> cfgprops = new HashSet<string>();

            Add(map.PrimaryKey);

            foreach (IColumnMap colmap in map.Columns)
            {
                Add(colmap);
                cfgprops.Add(colmap.Column.PropertyName);
            }

            foreach (IUniqueKeyMap akmap in map.UniqueKeys)
            {
                Add(akmap);
            }

            foreach (IForeignKeyMap fkmap in map.ForeignKeys)
            {
                Add(fkmap);
                cfgprops.Add(fkmap.PropertyRef.PropertyName);
            }

            foreach (IManyToOneMap momap in map.ManyToOneMappings)
            {
                Add(momap);
                cfgprops.Add(momap.PropertyRef.PropertyName);
            }

            foreach (IIndexMap ixmap in map.Indexes)
            {
                Add(ixmap);
            }

            if (map.FullTextIndex != null)
            {
                Add(map.FullTextIndex);
            }

            foreach (var prop in typeof(TEntity).GetProperties())
            {
                if (!cfgprops.Contains(prop.Name))
                {
                    Ignore(prop);
                }
            }

            ToTable(map.TableName);
        }

        private Expression<Func<TEntity, TProperty>> To<TProperty>(LambdaExpression selector)
        {
            return (Expression<Func<TEntity, TProperty>>)selector;
        }

        private Type PropType(LambdaExpression selector)
        {
            return ((PropertyInfo)((MemberExpression)selector.Body).Member).PropertyType;
        }

        private void CfgFunc<TConfig, TMockProp, TResult, TState>(TConfig cfg, TState state, Expression<Func<TEntity, TMockProp>> mockprop, Expression<Func<TConfig, Expression<Func<TEntity, TMockProp>>, TResult>> mockfunc, LambdaExpression selector, Expression<Action<TResult, TState>> postprocess, Type[] functypes, Type[] resulttypes, Type[] posttypes)
        {
            MethodInfo funcmethod = ((MethodCallExpression)mockfunc.Body).Method;
            MethodInfo postmethod = ((MethodCallExpression)postprocess.Body).Method;
            Expression postinstance = ((MethodCallExpression)postprocess.Body).Object;
            Type resulttype = typeof(TResult);

            if (funcmethod.IsGenericMethod)
            {
                funcmethod = funcmethod.GetGenericMethodDefinition().MakeGenericMethod(functypes);
            }

            if (postmethod.IsGenericMethod)
            {
                postmethod = postmethod.GetGenericMethodDefinition().MakeGenericMethod(posttypes);
            }

            if (resulttype.IsGenericType)
            {
                resulttype = resulttype.GetGenericTypeDefinition().MakeGenericType(resulttypes);
            }

            ParameterExpression selparam = Expression.Parameter(typeof(LambdaExpression));
            ParameterExpression stateparam = Expression.Parameter(typeof(TState));
            LambdaExpression call = Expression.Lambda(
                Expression.Call(
                    postinstance,
                    postmethod,
                    Expression.Call(
                        Expression.Constant(cfg),
                        funcmethod,
                        Expression.Convert(selparam, selector.GetType())
                    ),
                    stateparam
                ),
                selparam,
                stateparam
            );

            ((Action<LambdaExpression, TState>)call.Compile())(selector, state);
        }

        private void CfgAction<TConfig, TMockProp>(TConfig cfg, Expression<Func<TEntity, TMockProp>> mockprop, Expression<Action<TConfig, Expression<Func<TEntity, TMockProp>>>> mockfunc, LambdaExpression selector, params Type[] types)
        {
            MethodInfo method = ((MethodCallExpression)mockfunc.Body).Method;

            if (method.IsGenericMethod)
            {
                method = method.GetGenericMethodDefinition().MakeGenericMethod(types);
            }

            ParameterExpression param = Expression.Parameter(typeof(LambdaExpression));
            LambdaExpression call = Expression.Lambda(
                Expression.Block(
                    Expression.Call(
                        Expression.Constant(cfg),
                        method,
                        Expression.Convert(param, selector.GetType())
                    ),
                    Expression.Empty()
                ),
                param
            );

            ((Action<LambdaExpression>)call.Compile())(selector);
        }

        private void Config(DateTimePropertyConfiguration cfg, IColumnDef col)
        {
            Console.Write(".HasColumnName({0})", col.Name);
            cfg.HasColumnName(col.Name);

            if (col.Type.Precision != null)
            {
                Console.Write(".HasPrecision({0})", col.Type.Precision);
                cfg.HasPrecision((byte)col.Type.Precision);
            }

            if (col.Type.IsNullable)
            {
                Console.Write(".IsOptional()");
                cfg.IsOptional();
            }
            else
            {
                Console.Write(".IsRequired()");
                cfg.IsRequired();
            }
        }

        private void Config(BinaryPropertyConfiguration cfg, IColumnDef col)
        {
            Console.Write(".HasColumnName({0})", col.Name);
            cfg.HasColumnName(col.Name);

            if (col.Type.Length == null)
            {
                Console.Write(".IsMaxLength()");
                cfg.IsMaxLength();
            }
            else
            {
                Console.Write(".HasMaxLength({0})", col.Type.Length);
                cfg.HasMaxLength(col.Type.Length);
            }

            if (col.Type.IsNullable)
            {
                Console.Write(".IsOptional()");
                cfg.IsOptional();
            }
            else
            {
                Console.Write(".IsRequired()");
                cfg.IsRequired();
            }
        }

        private void Config(DecimalPropertyConfiguration cfg, IColumnDef col)
        {
            Console.Write(".HasColumnName({0})", col.Name);
            cfg.HasColumnName(col.Name);

            if (col.Type.Precision != null)
            {
                Console.Write(".HasPrecision({0}, {1})", col.Type.Precision, (col.Type.Scale ?? 0));
                cfg.HasPrecision((byte)col.Type.Precision, (byte)(col.Type.Scale ?? 0));
            }

            if (col.Type.IsNullable)
            {
                Console.Write(".IsOptional()");
                cfg.IsOptional();
            }
            else
            {
                Console.Write(".IsRequired()");
                cfg.IsRequired();
            }
        }

        private void Config(PrimitivePropertyConfiguration cfg, IColumnDef col)
        {
            Console.Write(".HasColumnName({0})", col.Name);
            cfg.HasColumnName(col.Name);

            if (col.Type.IsNullable)
            {
                Console.Write(".IsOptional()");
                cfg.IsOptional();
            }
            else
            {
                Console.Write(".IsRequired()");
                cfg.IsRequired();
            }
        }

        private void Config(StringPropertyConfiguration cfg, IColumnDef col)
        {
            Console.Write(".HasColumnName({0})", col.Name);
            cfg.HasColumnName(col.Name);

            if (col.Type.Length == null)
            {
                Console.Write(".IsMaxLength()");
                cfg.IsMaxLength();
            }
            else
            {
                Console.Write(".HasMaxLength({0})", col.Type.Length);
                cfg.HasMaxLength(col.Type.Length);
            }

            if (col.Type.IsNullable)
            {
                Console.Write(".IsOptional()");
                cfg.IsOptional();
            }
            else
            {
                Console.Write(".IsRequired()");
                cfg.IsRequired();
            }
        }

        private void Config<TTargetEntity>(RequiredNavigationPropertyConfiguration<TEntity, TTargetEntity> cfg, IForeignKeyMap fkmap)
            where TTargetEntity : class
        {
            if (fkmap.ReferencedKey is IManyToOneMap && fkmap.ManyToOneMap.HasMany)
            {
                Console.Write(".WithOptional({0})", fkmap.ManyToOneMap.PropertyRef.Selector.ToString());
                var one = cfg.WithOptional((Expression<Func<TTargetEntity, TEntity>>)fkmap.ManyToOneMap.PropertyRef.Selector);
                Config(one, fkmap);
            }
            else
            {
                if (fkmap.ManyToOneMap == null)
                {
                    Console.Write(".WithMany()");
                    Config(cfg.WithMany(), fkmap);
                }
                else if (fkmap.ManyToOneMap.HasMany)
                {
                    Console.Write(".WithMany({0})", fkmap.ManyToOneMap.PropertyRef.Selector.ToString());
                    Config(cfg.WithMany((Expression<Func<TTargetEntity, ICollection<TEntity>>>)fkmap.ManyToOneMap.PropertyRef.Selector), fkmap);
                }
                else
                {
                    Console.Write(".WithOptional({0})", fkmap.ManyToOneMap.PropertyRef.Selector.ToString());
                    Config(cfg.WithOptional((Expression<Func<TTargetEntity, TEntity>>)fkmap.ManyToOneMap.PropertyRef.Selector), fkmap);
                }
            }
        }

        private void Config<TTargetEntity>(OptionalNavigationPropertyConfiguration<TEntity, TTargetEntity> cfg, IForeignKeyMap fkmap)
            where TTargetEntity : class
        {
            if (fkmap.ManyToOneMap != null && fkmap.ManyToOneMap.HasMany)
            {
                Console.Write(".WithMany({0})", fkmap.ManyToOneMap.PropertyRef.Selector.ToString());
                Config(cfg.WithMany((Expression<Func<TTargetEntity, ICollection<TEntity>>>)fkmap.ManyToOneMap.PropertyRef.Selector), fkmap);
            }
            else
            {
                Console.Write(".WithMany()");
                Config(cfg.WithMany(), fkmap);
            }
        }

        private void Config(DependentNavigationPropertyConfiguration<TEntity> cfg, IForeignKeyMap fkmap)
        {
            var colsel = fkmap.IdColumn.Selector;
            if (colsel != null)
            {
                Console.Write(".HasForeignKey({0})", colsel.ToString());
                CfgFunc(
                    cfg,
                    fkmap,
                    e => 1,
                    (m, s) => m.HasForeignKey(s),
                    colsel,
                    (r, s) => Config(r, s),
                    new[] { PropType(colsel) },
                    null,
                    null
                );
            }
            else
            {
                Console.Write(".Map(fk => fk.MapKey(\"{0}\"))", fkmap.IdColumn.Name);
                Config(cfg.Map(fk => fk.MapKey(fkmap.IdColumn.Name)), fkmap);
            }
        }

        private void Config(CascadableNavigationPropertyConfiguration cfg, IForeignKeyMap fkmap)
        {
            Console.Write(".WillCascadeOnDelete(false)");
            cfg.WillCascadeOnDelete(false);
        }

        private void Config<TTargetEntity>(ManyNavigationPropertyConfiguration<TEntity, TTargetEntity> cfg, IManyToOneMap manymap)
            where TTargetEntity : class
        {
            if (manymap.ForeignKey.IsOptional)
            {
                Console.Write(".WithOptional({0})", manymap.ForeignKey.PropertyRef.Selector.ToString());
                cfg.WithOptional((Expression<Func<TTargetEntity, TEntity>>)manymap.ForeignKey.PropertyRef.Selector);
            }
            else
            {
                Console.Write(".WithRequired({0})", manymap.ForeignKey.PropertyRef.Selector.ToString());
                cfg.WithRequired((Expression<Func<TTargetEntity, TEntity>>)manymap.ForeignKey.PropertyRef.Selector);
            }
        }

        private void Config<TTargetEntity>(OptionalNavigationPropertyConfiguration<TEntity, TTargetEntity> cfg, IManyToOneMap manymap)
            where TTargetEntity : class
        {
            if (manymap.ForeignKey.IsOptional)
            {
                Console.Write(".WithOptionalPrincipal({0})", manymap.ForeignKey.PropertyRef.Selector.ToString());
                cfg.WithOptionalPrincipal((Expression<Func<TTargetEntity, TEntity>>)manymap.ForeignKey.PropertyRef.Selector);
            }
            else
            {
                Console.Write(".WithRequired({0})", manymap.ForeignKey.PropertyRef.Selector.ToString());
                cfg.WithRequired((Expression<Func<TTargetEntity, TEntity>>)manymap.ForeignKey.PropertyRef.Selector);
            }
        }

        public void Add(IColumnMap colmap)
        {
            if (colmap.Column.Type.CLRType == colmap.Column.PropertyType)
            {
                LambdaExpression sel = colmap.Column.Selector;
                IColumnDef col = colmap.Column;
                Type type = colmap.Column.Type.CLRType;

                Console.Write("map.Property({0})", sel.ToString());
                if (type.IsEnum)
                {
                    throw new InvalidOperationException(String.Format("Error in entity {0} property {1}: cannot handle enums", typeof(TEntity).Name, col.PropertyName));
                }
                else if (type == typeof(byte[]))
                {
                    Config(Property(To<byte[]>(sel)), col);
                }
                else if (type == typeof(string))
                {
                    Config(Property(To<string>(sel)), col);
                }
                else if (type == typeof(DateTime))
                {
                    Config(Property(To<DateTime>(sel)), col);
                }
                else if (type == typeof(DateTime?))
                {
                    Config(Property(To<DateTime?>(sel)), col);
                }
                else if (type == typeof(TimeSpan))
                {
                    Config(Property(To<TimeSpan>(sel)), col);
                }
                else if (type == typeof(TimeSpan?))
                {
                    Config(Property(To<TimeSpan?>(sel)), col);
                }
                else if (type == typeof(Decimal))
                {
                    Config(Property(To<Decimal>(sel)), col);
                }
                else if (type == typeof(Decimal?))
                {
                    Config(Property(To<Decimal?>(sel)), col);
                }
                else if (type == typeof(DateTimeOffset))
                {
                    Config(Property(To<DateTimeOffset>(sel)), col);
                }
                else if (type == typeof(DateTimeOffset?))
                {
                    Config(Property(To<DateTimeOffset?>(sel)), col);
                }
                else if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    CfgFunc(this, col, e => (int?)1, (m, s) => m.Property(s), sel, (r, s) => Config(r, s), PropType(sel).GetGenericArguments(), null, null);
                }
                else if (type.IsValueType)
                {
                    CfgFunc(this, col, e => (int)1, (m, s) => m.Property(s), sel, (r, s) => Config(r, s), new Type[] { PropType(sel) }, null, null);
                }
                Console.WriteLine();
            }
        }

        public void Add(IPrimaryKeyMap pkmap)
        {
            LambdaExpression sel = pkmap.KeyColumn.Selector;
            Type type = pkmap.KeyColumn.PropertyType;

            Console.WriteLine("map.HasKey({0})", sel.ToString());
            CfgAction(this, e => (int)1, (m, s) => m.HasKey(s), sel, type);
        }

        public void Add(IForeignKeyMap fkmap)
        {
            LambdaExpression sel = fkmap.PropertyRef.Selector;

            if (sel != null)
            {
                if (fkmap.IsOptional)
                {
                    Console.Write("map.HasOptional({0})", sel.ToString());
                    CfgFunc(this, fkmap, e => (object)null, (m, s) => m.HasOptional(s), sel, (r, s) => Config(r, s), new Type[] { PropType(sel) }, new Type[] { typeof(TEntity), PropType(sel) }, new Type[] { PropType(sel) });
                    Console.WriteLine();
                }
                else
                {
                    Console.Write("map.HasRequired({0})", sel.ToString());
                    CfgFunc(this, fkmap, e => (object)null, (m, s) => m.HasRequired(s), sel, (r, s) => Config(r, s), new Type[] { PropType(sel) }, new Type[] { typeof(TEntity), PropType(sel) }, new Type[] { PropType(sel) });
                    Console.WriteLine();
                }
            }
        }

        public void Add(IManyToOneMap manymap)
        {
            LambdaExpression sel = manymap.PropertyRef.Selector;

            if (manymap.HasMany)
            {
                Type collectiontype = PropType(sel).GetGenericArguments().First();
                Console.Write("map.HasMany({0})", sel.ToString());
                CfgFunc(this, manymap, e => (ICollection<object>)(new object[0]), (m, s) => m.HasMany(s), sel, (r, s) => Config(r, s), new Type[] { collectiontype }, new Type[] { typeof(TEntity), collectiontype }, new Type[] { collectiontype });
                Console.WriteLine();
            }
            else
            {
                Console.Write("map.HasOptional({0})", sel.ToString());
                CfgFunc(this, manymap, e => (object)null, (m, s) => m.HasOptional(s), sel, (r, s) => Config(r, s), new Type[] { PropType(sel) }, new Type[] { typeof(TEntity), PropType(sel) }, new Type[] { PropType(sel) });
                Console.WriteLine();
            }
        }

        public void Add(IUniqueKeyMap akmap)
        {
        }

        public void Add(IIndexMap ixmap)
        {
        }

        public void Add(IFullTextIndexMap ftmap)
        {
        }

        public void Ignore(PropertyInfo prop)
        {
            ParameterExpression param = Expression.Parameter(typeof(TEntity));
            LambdaExpression exp = Expression.Lambda(Expression.MakeMemberAccess(param, prop), param);
            CfgAction(this, e => (int)1, (m, s) => m.Ignore(s), exp, prop.PropertyType);
        }
    }
}
