using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Common;
using System.Data.Entity;

namespace TSVCEO.DataModelling
{
    public abstract class DataContext : DbContext, IDataSession
    {
        protected static ISQLDbInitializer _Initializer;
        protected static EntityMapBuilder _Mapper;
        protected static IDatabaseSeed _Seeder;

        public EntityMapBuilder Mapper { get; protected set; }
        protected ISQLDbInitializer Initializer { get { return _Initializer; } }

        public DataContext(string connstring)
            : base(_Initializer.Connect(connstring), true)
        {
        }

        public static void SetInitializer<TMapper>(ISQLDbInitializer initializer)
            where TMapper : EntityMapBuilder, new()
        {
            DataContext<TMapper>.SetInitializer(initializer);
        }

        public abstract EntityMapBuilder GetEntityMapper();

        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            EntityModelBuilder.Populate(modelBuilder.Configurations, GetEntityMapper().GetMaps());
        }

        IRepository<TEntity> IDataSession.Set<TEntity>()
        {
            return new DataRepository<TEntity> { DataSet = Set<TEntity>() };
        }

        void IDataSession.SaveChanges()
        {
            SaveChanges();
        }

        void IDataSession.Close()
        {
            Dispose();
        }

        DbConnection IDataSession.Database
        {
            get
            {
                return Database.Connection;
            }
        }
    }

    public class SQLDbInitializerWrapper<TContext> : IDatabaseInitializer<TContext>
        where TContext : DataContext
    {
        ISQLDbInitializer Initializer;

        public SQLDbInitializerWrapper(ISQLDbInitializer initializer)
        {
            this.Initializer = initializer;
        }

        public void InitializeDatabase(TContext context)
        {
            this.Initializer.InitializeDatabase(context.Database.Connection, context.GetEntityMapper());
        }
    }

    public class DataContext<TMapper> : DataContext
        where TMapper : EntityMapBuilder, new()
    {
        protected ISQLDbInitializer initializer;

        public DataContext(string connstring) : base(connstring) { }

        public static void SetInitializer(ISQLDbInitializer initializer)
        {
            _Initializer = initializer;
            Database.SetInitializer<DataContext<TMapper>>(new SQLDbInitializerWrapper<DataContext<TMapper>>(initializer));
        }
        
        public override EntityMapBuilder GetEntityMapper()
        {
            return new TMapper();
        }
    }
}
