/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.services.afs.storage;

import com.powsybl.afs.AppData;
import com.powsybl.afs.mapdb.storage.MapDbAppStorage;
import com.powsybl.afs.storage.DefaultListenableAppStorage;
import com.powsybl.afs.storage.ListenableAppStorage;
import com.powsybl.afs.ws.server.utils.sb.AppDataBeanSB;
import org.mockito.Mockito;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import javax.annotation.PostConstruct;
import java.util.Collections;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
@Configuration
@Profile("test")
public class AppDataBeanTestSB extends AppDataBeanSB {

    static final String TEST_FS_NAME = "mem";

    @PostConstruct
    @Override
    public void init() {
        appData = Mockito.mock(AppData.class);
        ListenableAppStorage storage = new DefaultListenableAppStorage(MapDbAppStorage.createHeap(TEST_FS_NAME));

        Mockito.when(appData.getRemotelyAccessibleStorage(TEST_FS_NAME))
                .thenReturn(storage);
        Mockito.when(appData.getRemotelyAccessibleFileSystemNames())
                .thenReturn(Collections.singletonList(TEST_FS_NAME));
    }
}
