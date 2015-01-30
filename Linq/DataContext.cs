using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Common;
using System.Data.Linq;

namespace TSVCEO.DataModelling.Linq
{
    public abstract class DataContext : System.Data.Linq.DataContext, IDataSession
    {
        protected bool _OwnsConnection;

        public DataContextFactory Factory { get; protected set; }

        public DataContext(DataContextFactory factory, DbConnection conn, bool ownsconn)
            : base(conn, XmlMappingBuilder.CreateMapping(factory.GetEntityMapper().GetMaps(), conn.Database))
        {
            this._OwnsConnection = ownsconn;
        }

        protected override void Dispose(bool disposing)
        {
            if (_OwnsConnection)
            {
                this.Connection.Dispose();
            }

            base.Dispose(disposing);
        }

        IRepository<TEntity> IDataSession.Set<TEntity>()
        {
            return new DataRepository<TEntity> { DataSet = GetTable<TEntity>() };
        }

        void IDataSession.SaveChanges()
        {
            try
            {
                this.SubmitChanges();
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        void IDataSession.Close()
        {
            Dispose();
        }

        DbConnection IDataSession.Database
        {
            get
            {
                return this.Connection;
            }
        }
    }

    public class DataContext<TMapper> : DataContext
        where TMapper : EntityMapBuilder, new()
    {
        public DataContext(DataContextFactory factory, DbConnection conn, bool ownsconn) : base(factory, conn, ownsconn) { }
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
        protected static bool _InitializerHasRun;

        public static void SetInitializer(ISQLDbInitializer initializer)
        {
            _Initializer = initializer;
        }

        public DataContextFactory(string connstring)
        {
            this.ConnectionString = connstring;
        }

        public override bool IsInitialized
        {
            get
            {
                return _InitializerHasRun;
            }
        }

        public override void Init()
        {
            using (DbConnection conn = _Initializer.Connect(ConnectionString))
            {
                conn.Open();
                _Initializer.InitializeDatabase(conn, this.GetEntityMapper());
                _InitializerHasRun = true;
            }
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
