package com.powsybl.services.afs.storage;

import com.powsybl.server.commons.UserAuthenticator;
import com.powsybl.commons.net.UserProfile;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("default")
public class UserAuthenticatorMockTempSB implements UserAuthenticator {
    @Override
    public UserProfile check(String login, String password) {
        return new UserProfile("bat", "man");
    }
}
