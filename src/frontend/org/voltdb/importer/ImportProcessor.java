/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.importer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.CatalogContext;
import org.voltdb.ImporterServerAdapterImpl;
import org.voltdb.VoltDB;
import org.voltdb.importer.formatter.FormatterBuilder;
import org.voltdb.modular.ModuleManager;
import org.voltdb.utils.CatalogUtil.ImportConfiguration;

import com.google_voltpatches.common.base.Throwables;
import java.util.concurrent.ExecutionException;

public class ImportProcessor implements ImportDataProcessor {

    private static final VoltLogger m_logger = new VoltLogger("IMPORT");
    private final Map<String, ImporterWrapper> m_bundles = new HashMap<String, ImporterWrapper>();
//    private final Map<String, ImporterWrapper> m_bundlesByName = new HashMap<String, ImporterWrapper>();
    private final ModuleManager m_moduleManager;
    private final ChannelDistributer m_distributer;
    private final ExecutorService m_es = CoreUtils.getSingleThreadExecutor("ImportProcessor");
    private final ImporterServerAdapter m_importServerAdapter;
    private final String m_clusterTag;
    private AtomicBoolean m_stopping =  new AtomicBoolean(false);

    public ImportProcessor(
            int myHostId,
            ChannelDistributer distributer,
            ModuleManager moduleManager,
            ImporterStatsCollector statsCollector,
            String clusterTag)
    {
        m_moduleManager = moduleManager;
        m_distributer = distributer;
        m_importServerAdapter = new ImporterServerAdapterImpl(statsCollector);
        m_clusterTag = clusterTag;
    }

    //This abstracts OSGi based and class based importers.
    public class ImporterWrapper {
//        private final URI m_bundleURI;
        private AbstractImporterFactory m_importerFactory;
        private ImporterLifeCycleManager m_importerTypeMgr;

        public ImporterWrapper(AbstractImporterFactory importerFactory) {
            m_importerFactory = importerFactory;
            m_importerFactory.setImportServerAdapter(m_importServerAdapter);
            m_importerTypeMgr = new ImporterLifeCycleManager(
                    m_importerFactory, m_distributer, m_clusterTag);
        }

        public String getImporterType() {
            return m_importerFactory.getTypeName();
        }

        public void configure(Properties props, FormatterBuilder formatterBuilder) {
            m_importerTypeMgr.configure(props, formatterBuilder);
        }

        public int getConfigsCount() {
            return m_importerTypeMgr.getConfigsCount();
        }

        public void stop() {
            try {
                //Handler can be null for initial period if shutdown come quickly.
                if (m_importerFactory != null) {
                    m_importerTypeMgr.stop();
                }
            } catch (Exception ex) {
                m_logger.error("Failed to stop the import bundles.", ex);
            }
        }
    }

    @Override
    public int getPartitionsCount() {
        int count = 0;
        for (ImporterWrapper wapper : m_bundles.values()) {
            if (wapper != null) {
                count += wapper.getConfigsCount();
            }
        }
        return count;
    }

    @Override
    public synchronized void readyForData(final CatalogContext catContext, final HostMessenger messenger) {

        if (m_stopping.get()) {
            m_logger.info("HH: !!!!!READY for data called during shutdown");
        }

        Future<?> task = m_es.submit(new Runnable() {
            @Override
            public void run() {
                for (ImporterWrapper bw : m_bundles.values()) {
                    try {
                        bw.m_importerTypeMgr.readyForData();
                    } catch (Exception ex) {
                        //Should never fail. crash.
                        VoltDB.crashLocalVoltDB("Import failed to set Handler", true, ex);
                        m_logger.error("Failed to start the import handler: " + bw.m_importerFactory.getTypeName(), ex);
                    }
                }
            }
        });
        // wait for start
//        try {
//            if(task.get() == null) {
//                m_logger.info("Importer started ");
//            }
//            else {
//                m_logger.info("Importer starting, blocking did not help");
//            }
//        } catch (Exception e) {
//        }
    }

    @Override
    public synchronized void shutdown() {
        if (!m_stopping.compareAndSet(false, true)) {
            m_logger.info("HH: ImportProcessor - already in shutdown sequence .... ");
        }
        //Task that shutdowns all the bundles we wait for it to finish.
        Future<?> task = m_es.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    //Stop all the bundle wrappers.
                    for (ImporterWrapper bw : m_bundles.values()) {
                        try {
                            bw.stop();
                        } catch (Exception ex) {
                            m_logger.error("Failed to stop the import handler: " + bw.m_importerFactory.getTypeName(), ex);
                        }
                    }
                    m_bundles.clear();
                } catch (Exception ex) {
                    m_logger.error("Failed to stop the import bundles.", ex);
                    Throwables.propagate(ex);
                }
            }
        });

        //And wait for it.
        try {
            if (task.get() == null) {
                m_logger.info("HH: ImportProcessor - Importers stopped sucessfully");
            }
            else {
                m_logger.info("HH: ImportProcessor - Importers couldn't be stopped sucessfully");
            }
        } catch (InterruptedException | ExecutionException ex) {
            m_logger.error("Failed to stop import processor.", ex);
        }
        StringBuilder msg = new StringBuilder("HH: Importerprocess shutdown wait ");
        try {
            m_es.shutdown();
            m_es.awaitTermination(365, TimeUnit.DAYS);
//            if (m_es.awaitTermination(30, TimeUnit.SECONDS)) {
//                msg.append("- Shutdown completed");
//            }
//            else {
//                msg.append(" - Timedout waiting for shutdown to complete");
//                m_es.shutdownNow();
//                if (!m_es.awaitTermination(30, TimeUnit.SECONDS)) {
//                    msg.append(". Force shutdown suceeeded");
//                } else {
//                    msg.append(". Force shutdown suceeeded");
//                }
//            }
        } catch (InterruptedException ex) {
            m_logger.error("Failed to stop import processor executor.", ex);
        } finally {
            m_logger.info(msg.toString());
        }
    }

    private void addProcessorConfig(ImportConfiguration config, Map<String, AbstractImporterFactory> bundles) {
        Properties properties = config.getmoduleProperties();

        String module = properties.getProperty(ImportDataProcessor.IMPORT_MODULE);
        String attrs[] = module.split("\\|");
        String bundleJar = attrs[1];

        FormatterBuilder formatterBuilder = config.getFormatterBuilder();
        try {

            ImporterWrapper wrapper = m_bundles.get(bundleJar);
            if (wrapper == null) {
                AbstractImporterFactory importFactory = bundles.get(bundleJar);
                wrapper = new ImporterWrapper(importFactory);
                String name = wrapper.getImporterType();
                if (name == null || name.trim().isEmpty()) {
                    throw new RuntimeException("Importer must implement and return a valid unique name.");
                }
                if (!m_bundles.isEmpty()) {
                    StringBuilder debugMsg = new StringBuilder("HH: bundles: " + m_bundles.keySet().toString());
                    m_logger.info(debugMsg.toString());
                }
                m_bundles.put(bundleJar, wrapper);
            }
            wrapper.configure(properties, formatterBuilder);
        } catch(Throwable t) {
            m_logger.error("Failed to configure import handler for " + bundleJar, t);
            Throwables.propagate(t);
        }
    }

    @Override
    public void setProcessorConfig(Map<String, ImportConfiguration> config,
            final Map<String, AbstractImporterFactory> bundles) {
        for (String configName : config.keySet()) {
            ImportConfiguration importConfig = config.get(configName);
            addProcessorConfig(importConfig, bundles);
        }
    }

}
