﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TSVCEO.DataModelling
{
    public interface IDatabaseSeed
    {
        void Seed(IDataSession session);
    }
}
