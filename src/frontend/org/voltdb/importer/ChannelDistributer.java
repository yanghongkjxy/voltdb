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

import java.net.URI;
import java.util.Set;

/**
 * Created by bshaw on 5/26/17.
 */
public interface ChannelDistributer extends ChannelChangeCallback {


    String getClusterTag();


    void registerCallback(String m_distributerDesignation, ChannelChangeCallback importerLifeCycleManager);

    void registerChannels(String m_distributerDesignation, Set<URI> uris);

    void unregisterCallback(String m_distributerDesignation);

    void shutdown();
}
