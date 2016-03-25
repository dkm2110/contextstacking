using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Org.Apache.REEF.Common.Events;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tests.Functional.ContextStacking
{
    internal class ContextStart<T> : IObserver<IContextStart>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(ContextStart<T>));
        private static int s_counter = 0;

        [Inject]
        private ContextStart(IContextInput<T> contextInput)
        {
            s_counter++;
            Logger.Log(Level.Info, string.Format("Entering context start constructor with counter {0}", s_counter));
            if (s_counter == 2)
            {
                Logger.Log(Level.Info, "Entered the context start constructor twice");
            }
        }

        public void OnNext(IContextStart value)
        {
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }
    }
}
