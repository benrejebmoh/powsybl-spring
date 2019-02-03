package com.powsybl.services.afs.storage;

import com.powsybl.afs.ws.server.utils.sb.UserAuthenticator;
import com.powsybl.services.utils.UserProfile;
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
