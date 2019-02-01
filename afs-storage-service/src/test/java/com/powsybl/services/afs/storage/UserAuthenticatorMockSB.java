package com.powsybl.services.afs.storage;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import com.powsybl.afs.ws.server.utils.UserAuthenticator;
import com.powsybl.afs.ws.utils.UserProfile;

@Configuration
@Profile("test")
public class UserAuthenticatorMockSB implements UserAuthenticator {
    @Override
    public UserProfile check(String login, String password) {
        return new UserProfile("bat", "man");
    }
}
