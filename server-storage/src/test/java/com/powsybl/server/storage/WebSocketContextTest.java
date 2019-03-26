/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.server.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import javax.websocket.CloseReason;
import javax.websocket.CloseReason.CloseCodes;
import javax.websocket.Session;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
@Component
@Profile("test")
public class WebSocketContextTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketContextTest.class);

    private final Set<Session> sessions = new HashSet<>();

    public synchronized void addSession(Session session) {
        sessions.add(session);
    }

    public synchronized void removeSession(Session session) {
        sessions.remove(session);
    }

    @PreDestroy
    public synchronized void closeAndRemoveAllSessions() {
        for (Session session : sessions) {
            try {
                session.close(new CloseReason(CloseCodes.UNEXPECTED_CONDITION, ""));
            } catch (Exception e) {
                LOGGER.error(e.toString(), e);
            }
        }
        sessions.clear();
    }
    public Set<Session> getSessions() {
        return this.sessions;
    }
}
