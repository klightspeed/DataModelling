using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Linq;

namespace TSVCEO.DataModelling.Linq
{
    public class DataRepository<TEntity> : IRepository<TEntity>
        where TEntity : class
    {
        public ITable<TEntity> DataSet { get; set; }

        TEntity IRepository<TEntity>.Add(TEntity entity)
        {
            DataSet.InsertOnSubmit(entity);
            return entity;
        }

        IEnumerable<TEntity> IRepository<TEntity>.AddRange(IEnumerable<TEntity> entities)
        {
            foreach (TEntity e in entities)
            {
                DataSet.InsertOnSubmit(e);
            }

            return entities;
        }

        TEntity IRepository<TEntity>.Attach(TEntity entity)
        {
            DataSet.Attach(entity);

            return entity;
        }

        void IRepository<TEntity>.Remove(TEntity entity)
        {
            DataSet.DeleteOnSubmit(entity);
        }

        

        IQueryable<TEntity> IRepository<TEntity>.Query()
        {
            return DataSet.AsQueryable<TEntity>();
        }
    }
}
