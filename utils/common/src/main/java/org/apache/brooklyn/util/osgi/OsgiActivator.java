/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.brooklyn.util.osgi;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.launch.Framework;
import org.osgi.framework.startlevel.FrameworkStartLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * handle bundle activation/deactivation
 */
public class OsgiActivator implements BundleActivator {

  private static Logger LOG = LoggerFactory.getLogger(OsgiActivator.class);

  @Override
  public void start(BundleContext context) throws Exception {
      Framework f = (Framework) context.getBundle(0);
      // https://issues.apache.org/jira/browse/KARAF-6920: framework start level should report 80 but instead says 100 on clean start
      // when this is resolved we will no longer need the `startlevel.postinit` property in OsgiLauncherImpl
      LOG.debug("Starting "+context.getBundle()+", at OSGi start-level "+f.adapt(FrameworkStartLevel.class).getStartLevel()+", bundle state "+context.getBundle().getState());
  }

  @Override
  public void stop(BundleContext context) throws Exception {
      LOG.debug("Stopping "+context.getBundle());
      OsgiUtil.shutdown();
  }

}
