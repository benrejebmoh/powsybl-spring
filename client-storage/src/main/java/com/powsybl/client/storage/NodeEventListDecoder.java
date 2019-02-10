/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.client.storage;

import com.powsybl.afs.storage.events.NodeEventList;
import com.powsybl.client.commons.JacksonDecoder;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class NodeEventListDecoder extends JacksonDecoder<NodeEventList> {

    public NodeEventListDecoder() {
        super(NodeEventList.class);
    }
}
