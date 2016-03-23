using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tests.Functional.ContextStacking
{
    internal interface IContextInput<T>
    {
        int Counter();
    }

    internal class ContextInput<T> : IContextInput<T>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(ContextInput<T>));
        private static int s_staticCounter = 0;

        [Inject]
        internal ContextInput()
        {
            s_staticCounter++;
            Logger.Log(Level.Info, string.Format("Entering context input constructor with counter {0}", s_staticCounter));
            if (s_staticCounter == 2)
            {
                throw new Exception("Entered the constructor twice");
            }
        }

        public int Counter()
        {
            return s_staticCounter;
        }
    }
}
