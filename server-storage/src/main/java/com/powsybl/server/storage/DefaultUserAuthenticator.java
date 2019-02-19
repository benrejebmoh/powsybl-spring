/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.server.storage;

import com.powsybl.server.commons.UserAuthenticator;
import com.powsybl.commons.net.UserProfile;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("default")
public class DefaultUserAuthenticator implements UserAuthenticator {

    @Override
    public UserProfile check(String login, String password) {
        return new UserProfile("", "");
    }
}
