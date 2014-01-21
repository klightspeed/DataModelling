using System;
using System.Collections.Generic;
using System.Linq;

namespace TSVCEO.DataModelling
{
    public interface IRepository<TEntity>
        where TEntity : class
    {
        TEntity Add(TEntity entity);
        IEnumerable<TEntity> AddRange(IEnumerable<TEntity> entities);
        TEntity Attach(TEntity entity);
        void Remove(TEntity entity);
        IQueryable<TEntity> Query();
    }
}
