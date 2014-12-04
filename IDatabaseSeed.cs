using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Common;

namespace TSVCEO.DataModelling
{
    public interface IDatabaseSeed
    {
        void Seed(IDataSession session);
    }
}
