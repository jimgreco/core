package com.core.infrastructure.io;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.BDDAssertions.then;
import static org.assertj.core.api.BDDAssertions.thenThrownBy;

@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
public class InetAddressUtilsTest {

    @Test
    void toHost1() throws IOException {
        var host = InetAddressUtils.toHost("inet:35.12.1.32");

        then(host).isEqualTo("35.12.1.32");
    }

    @Test
    void toHost2() throws IOException {
        var host = InetAddressUtils.toHost("inet:35.12.1.32:10001");

        then(host).isEqualTo("35.12.1.32");
    }

    @Test
    void toHost3() throws IOException {
        var host = InetAddressUtils.toHost("inet:35.12.1.32:10001:lo0");

        then(host).isEqualTo("35.12.1.32");
    }

    @Test
    void toHostPort1() throws IOException {
        var address = InetAddressUtils.toHostPort("inet:35.12.1.32:10001");

        then(address.getHostName()).isEqualTo("35.12.1.32");
        then(address.getPort()).isEqualTo(10001);
    }

    @Test
    void toHostPort2() throws IOException {
        var address = InetAddressUtils.toHostPort("inet:35.12.1.32:10001:lo0");

        then(address.getHostName()).isEqualTo("35.12.1.32");
        then(address.getPort()).isEqualTo(10001);
    }

    @Test
    void toHostPort_invalid_port_number_throws_IOException() throws IOException {
        thenThrownBy(() -> InetAddressUtils.toHostPort("inet:35.12.1.32:10A01"))
                .isInstanceOf(IOException.class);
    }

    @Test
    void toPort1() throws IOException {
        var port = InetAddressUtils.toPort("inet:35.12.1.32:10001");

        then(port).isEqualTo(10001);
    }

    @Test
    void toPort2() throws IOException {
        var port = InetAddressUtils.toPort("inet::10001");

        then(port).isEqualTo(10001);
    }

    @Test
    void toPort3() throws IOException {
        var port = InetAddressUtils.toPort("inet:35.12.1.32:10001:lo0");

        then(port).isEqualTo(10001);
    }

    @Test
    void toPort_invalid_port_number_throws_IOException() throws IOException {
        thenThrownBy(() -> InetAddressUtils.toPort("inet:35.12.1.32:10A01"))
                .isInstanceOf(IOException.class);
    }

    @Test
    void toInterface1() throws IOException {
        var intf = InetAddressUtils.toInterface("inet:35.12.1.32:10001:lo0");

        then(intf).isEqualTo("lo0");
    }

    @Test
    void toInterface2() throws IOException {
        var intf = InetAddressUtils.toInterface("inet:35.12.1.32::lo0");

        then(intf).isEqualTo("lo0");
    }

    @Test
    void toInterface3() throws IOException {
        var intf = InetAddressUtils.toInterface("inet:35.12.1.32::lo0:foo");

        then(intf).isEqualTo("lo0");
    }
}
