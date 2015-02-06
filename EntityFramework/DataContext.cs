using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Common;
using System.Data.Entity;

namespace TSVCEO.DataModelling.EntityFramework
{
    public abstract class DataContext : DbContext, IDataSession
    {
        public DataContextFactory Factory { get; protected set; }

        public DataContext(DataContextFactory factory, DbConnection conn, bool ownsConnection)
            : base(conn, ownsConnection)
        {
            this.Factory = factory;
        }

        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            EntityModelBuilder.Populate(modelBuilder.Configurations, Factory.GetEntityMapper().GetMaps());
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
        protected ISQLDbInitializer Initializer;

        public bool HasRun { get; protected set; }

        public SQLDbInitializerWrapper(ISQLDbInitializer initializer)
        {
            this.Initializer = initializer;
        }

        public void InitializeDatabase(TContext context)
        {
            if (context.Database.Connection.State == System.Data.ConnectionState.Closed)
            {
                context.Database.Connection.Open();
            }

            this.Initializer.InitializeDatabase(context.Database.Connection, context.Factory.GetEntityMapper());
            this.HasRun = true;
        }
    }

    public class DataContext<TMapper> : DataContext
        where TMapper : EntityMapBuilder, new()
    {
        public DataContext(DataContextFactory factory, DbConnection conn, bool ownsConnection) : base(factory, conn, ownsConnection) { }
    }

    public abstract class DataContextFactory : IDataSessionFactory
    {
        protected static ISQLDbInitializer _Initializer;
        protected static EntityMapBuilder _Mapper;
        protected static IDatabaseSeed _Seeder;

        public EntityMapBuilder Mapper { get; protected set; }
        protected ISQLDbInitializer Initializer { get { return _Initializer; } }

        public static void SetInitializer<TMapper>(ISQLDbInitializer initializer)
            where TMapper : EntityMapBuilder, new()
        {
            DataContextFactory<TMapper>.SetInitializer(initializer);
        }

        public abstract EntityMapBuilder GetEntityMapper();

        public abstract bool IsInitialized { get; }

        public abstract void Init();

        public abstract IDataSession Create();
    }

    public class DataContextFactory<TMapper> : DataContextFactory
        where TMapper : EntityMapBuilder, new()
    {
        protected string ConnectionString;
        protected static SQLDbInitializerWrapper<DataContext<TMapper>> _Initwrapper;

        public static void SetInitializer(ISQLDbInitializer initializer)
        {
            _Initializer = initializer;
            _Initwrapper = new SQLDbInitializerWrapper<DataContext<TMapper>>(initializer);
            Database.SetInitializer<DataContext<TMapper>>(_Initwrapper);
        }

        public DataContextFactory(string connstring)
        {
            this.ConnectionString = connstring;
        }

        public override bool IsInitialized
        {
            get
            {
                return _Initwrapper.HasRun;
            }
        }

        public override void Init()
        {
            new DataContext<TMapper>(this, _Initializer.Connect(ConnectionString), true).Database.Initialize(false);
        }

        public override IDataSession Create()
        {
            return new DataContext<TMapper>(this, _Initializer.Connect(ConnectionString), true);
        }

        public override EntityMapBuilder GetEntityMapper()
        {
            return new TMapper();
        }
    }
}
