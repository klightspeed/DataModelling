using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Common;

namespace TSVCEO.DataModelling
{
    public class DataSessionFactory<TMapper, TInitializer> : IDataSessionFactory
        where TMapper : EntityMapBuilder
    {
        protected string ConnectionString;
        protected SQLDbInitializer Initializer;
        protected IDatabaseSeed Seed;

        public bool IsInitialized { get; protected set; }

        public void SetConnectionString(string connstring)
        {
            if (this.ConnectionString == null)
            {
                this.ConnectionString = connstring;
            }
        }

        public void SetInitializer(SQLDbInitializer initializer)
        {
            if (this.Initializer == null)
            {
                this.Initializer = initializer;
            }
        }

        public void SetDatabaseSeed(IDatabaseSeed seed)
        {
            if (this.Seed == null)
            {
                this.Seed = seed;
            }
        }
    }
}
