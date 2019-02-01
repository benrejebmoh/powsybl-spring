package com.powsybl.services.afs.storage;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.powsybl.afs.ws.server.utils.UserAuthenticator;
import com.powsybl.afs.ws.utils.UserProfile;

@Component
@Profile("default")
public class UserAuthenticatorMockTempSB implements UserAuthenticator {
    @Override
    public UserProfile check(String login, String password) {
        return new UserProfile("bat", "man");
    }
}
