using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TSVCEO.DataModelling
{
    public interface IDataSessionFactory
    {
        bool IsInitialized { get; }
        void Init();
        IDataSession Create();
    }
}
