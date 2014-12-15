using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Entity;

namespace TSVCEO.DataModelling
{
    public class DataRepository<TEntity> : IRepository<TEntity>
        where TEntity : class
    {
        public DbSet<TEntity> DataSet { get; set; }

        TEntity IRepository<TEntity>.Add(TEntity entity)
        {
            return DataSet.Add(entity);
        }

        IEnumerable<TEntity> IRepository<TEntity>.AddRange(IEnumerable<TEntity> entities)
        {
            return DataSet.AddRange(entities);
        }

        TEntity IRepository<TEntity>.Attach(TEntity entity)
        {
            return DataSet.Attach(entity);
        }

        void IRepository<TEntity>.Remove(TEntity entity)
        {
            DataSet.Remove(entity);
        }

        IQueryable<TEntity> IRepository<TEntity>.Query()
        {
            return DataSet.AsQueryable<TEntity>();
        }
    }
}
