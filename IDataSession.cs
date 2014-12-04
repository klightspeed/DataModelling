using System;
using System.Collections.Generic;
using System.Data.Common;

namespace TSVCEO.DataModelling
{
    public interface IDataSession : IDisposable
    {
        IRepository<TEntity> Set<TEntity>() where TEntity : class;
        void SaveChanges();
        void Close();
        DbConnection Database { get; }
    }
}