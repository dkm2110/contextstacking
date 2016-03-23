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
using System.Collections.Generic;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Driver.Defaults;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.ContextStacking
{
    [Collection("FunctionalTests")]
    public class TestContextStacking : ReefFunctionalTest
    {
        public TestContextStacking()
        {
            CleanUp();
            Init();
        }

        /// <summary>
        /// This test is to test if the context stack works or not. Specifically constructor of 
        /// ContextStartHandler of base context and service should be called only once
        /// </summary>
        [Fact]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Test comtext stacking")]
        public void TestReefContextStacking()
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid();
            CleanUp(testFolder);
            TestRun(DriverConfigurations(), typeof(ContextStackingDriver), 1, "simpleHandler", "local", testFolder);
            ValidateSuccessForLocalRuntime(1, testFolder: testFolder);

            var messages = new List<string>();
            messages.Add("Activated both contexts. Exiting");
            ValidateMessageSuccessfullyLogged(messages, "driver", DriverStdout, testFolder, 0);

            var messages2 = new List<string>();
            messages.Add("Entering context input constructor with counter 1");
            messages.Add("Entering context start constructor with counter 1");
            ValidateMessageSuccessfullyLogged(messages2, "Node-*", EvaluatorStdout, testFolder, 0);

            CleanUp(testFolder);
        }

        public IConfiguration DriverConfigurations()
        {
            IConfiguration driverConfig = DriverConfiguration.ConfigurationModule
            .Set(DriverConfiguration.OnDriverStarted, GenericType<ContextStackingDriver>.Class)
            .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<ContextStackingDriver>.Class)
            .Set(DriverConfiguration.OnContextActive, GenericType<ContextStackingDriver>.Class)
            .Set(DriverConfiguration.CustomTraceListeners, GenericType<DefaultCustomTraceListener>.Class)
            .Set(DriverConfiguration.CustomTraceLevel, Level.Info.ToString())
            .Build();

            IConfiguration taskConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindSetEntry<DriverBridgeConfigurationOptions.SetOfAssemblies, string>(typeof(ContextInput<>).Assembly.GetName().Name)
                .BindSetEntry<DriverBridgeConfigurationOptions.SetOfAssemblies, string>(typeof(NameClient).Assembly.GetName().Name)
                .Build();

            return Configurations.Merge(driverConfig, taskConfig);
        }
    }
}