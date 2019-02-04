package com.powsybl.services.afs.storage;

import com.powsybl.afs.ws.server.utils.sb.UserAuthenticator;
import com.powsybl.commons.net.UserProfile;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("test")
public class UserAuthenticatorMockSB implements UserAuthenticator {
    @Override
    public UserProfile check(String login, String password) {
        return new UserProfile("bat", "man");
    }
}
