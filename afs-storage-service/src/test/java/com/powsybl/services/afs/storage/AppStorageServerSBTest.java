package com.powsybl.services.afs.storage;


import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;

import javax.servlet.ServletContext;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import static org.junit.Assert.*;
import com.powsybl.afs.storage.AbstractAppStorageTest;
import com.powsybl.afs.storage.ListenableAppStorage;
import com.powsybl.afs.ws.storage.st.RemoteAppStorageSt;
import com.powsybl.afs.ws.storage.st.RemoteListenableAppStorageSt;
import com.powsybl.commons.exceptions.UncheckedUriSyntaxException;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
public class AppStorageServerSBTest extends AbstractAppStorageTest  {

    @LocalServerPort
    private int port;

    @Autowired
    private ServletContext servletContext;

    private URI getRestUri() {
        try {
            String sheme = "http";
            return new URI(sheme + "://localhost:" + port + servletContext.getContextPath());
        } catch (URISyntaxException e) {
            throw new UncheckedUriSyntaxException(e);
        }
    }

    @Override
    protected ListenableAppStorage createStorage() {
        URI restUri = getRestUri();
        RemoteAppStorageSt storage = new RemoteAppStorageSt(AppDataBeanMockSB.TEST_FS_NAME, restUri, "");
        return new RemoteListenableAppStorageSt(storage, restUri);
    }

    @Test
    public void getFileSystemNamesTest() {
        List<String> fileSystemNames = RemoteAppStorageSt.getFileSystemNames(getRestUri(), "");
        assertEquals(Collections.singletonList(AppDataBeanMockSB.TEST_FS_NAME), fileSystemNames);
    }
}
