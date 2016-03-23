// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.Globalization;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Events;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Tests.Functional.Messaging;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using IRunningTask = Org.Apache.REEF.Driver.Task.IRunningTask;

namespace Org.Apache.REEF.Tests.Functional.ContextStacking
{
    public class ContextStackingDriver :
        IObserver<IAllocatedEvaluator>, 
        IObserver<IActiveContext>, 
        IObserver<IDriverStarted>
    {
        public const int NumberOfEvaluator = 1;
        private static readonly Logger Logger = Logger.GetLogger(typeof(ContextStackingDriver));
        private readonly IEvaluatorRequestor _evaluatorRequestor;

        [Inject]
        public ContextStackingDriver(IEvaluatorRequestor evaluatorRequestor)
        {
            _evaluatorRequestor = evaluatorRequestor;
        }
        public void OnNext(IAllocatedEvaluator eval)
        {
            var contextConf =
                ContextConfiguration.ConfigurationModule.Set(ContextConfiguration.Identifier, "Stage1").Build();

            var serviceConf =
                TangFactory.GetTang()
                    .NewConfigurationBuilder(ServiceConfiguration.ConfigurationModule.Build())
                    .BindSetEntry<ContextConfigurationOptions.StartHandlers, ContextStart<int>, IObserver<IContextStart>>(
                        GenericType<ContextConfigurationOptions.StartHandlers>.Class,
                        GenericType<ContextStart<int>>.Class)
                    .BindImplementation(GenericType<IContextInput<int>>.Class, GenericType<ContextInput<int>>.Class)
                    .Build();

            eval.SubmitContextAndService(contextConf, serviceConf);
        }

        public void OnNext(IActiveContext context)
        {
            if (context.Id.Equals("Stage1"))
            {
                var contextConf =
                    ContextConfiguration.ConfigurationModule.Set(ContextConfiguration.Identifier, "Stage2").Build();
                context.SubmitContext(contextConf);
            }
            else
            {
                if (context.Id.Equals("Stage2"))
                {
                    Logger.Log(Level.Info, "Activated both contexts. Exiting");
                    context.Dispose();
                }
                else
                {
                    throw new Exception("Invalid context ID");
                }
            }
        }

        public void OnNext(IDriverStarted value)
        {
            var request =
                _evaluatorRequestor.NewBuilder()
                    .SetNumber(NumberOfEvaluator)
                    .SetMegabytes(512)
                    .SetCores(2)
                    .SetRackName("WonderlandRack")
                    .SetEvaluatorBatchId("TaskContextStack")
                    .Build();
            _evaluatorRequestor.Submit(request);
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }
    }
}