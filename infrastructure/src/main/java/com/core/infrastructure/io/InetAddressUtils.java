package com.core.infrastructure.io;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * Utilities that extracts the host, port, and network interface from an address string.
 * The address string has the following format: {@code inet:host[:port[:interface]]}.
 * The following address strings are all valid:
 * {@code inet:239.100.100.100:20001:eno1}, {@code inet::20001}, {@code inet:239.100.100.100:20001},
 * {@code inet:239.100.100.100::eno1}, {@code inet:239.100.100.100}.
 */
public class InetAddressUtils {

    /**
     * Returns the address string for the specified socket address.
     * The conversion used is, {@code "inet:" + socketAddress.getHostName() + ":" + socketAddress.getPort()}.
     *
     * @param socketAddress the socket address
     * @return the address string
     */
    static String toAddress(SocketAddress socketAddress) {
        if (socketAddress == null) {
            return null;
        } else {
            var inetSocketAddress = (InetSocketAddress) socketAddress;
            return "inet:" + inetSocketAddress.getHostName() + ":" + inetSocketAddress.getPort();
        }
    }

    /**
     * Returns the host for the specified {@code address}.
     *
     * @param address the address
     * @return the host extracted from the address
     * @throws IOException if {@code address} is malformed
     */
    public static String toHost(String address) throws IOException {
        var position = 0;

        // inet
        position += checkInet(address);

        // host
        String host = null;
        for (var i = position; i < address.length(); i++) {
            var lastByte = i == address.length() - 1;
            if (address.charAt(i) == ':' || lastByte) {
                if (lastByte) {
                    i++;
                }
                host = address.substring(position, i);
                break;
            }
        }
        if (host == null) {
            throw new IOException("malformed address: " + address);
        }

        return host;
    }

    /**
     * Returns the socket address for the specified core {@code address}.
     *
     * @param address the address
     * @return the socket address
     * @throws IOException if an I/O error occurs
     */
    public static InetSocketAddress toHostPort(String address) throws IOException {
        try {
            var position = 0;

            // inet
            position += checkInet(address);

            // host
            String host = null;
            for (var i = position; i < address.length(); i++) {
                if (address.charAt(i) == ':') {
                    host = address.substring(position, i);
                    position += host.length() + 1;
                    break;
                }
            }
            if (host == null) {
                throw new IOException("malformed address: " + address);
            }

            // port
            var port = -1;
            for (var i = position; i < address.length(); i++) {
                var lastByte = i == address.length() - 1;
                if (address.charAt(i) == ':' || lastByte) {
                    if (lastByte) {
                        i++;
                    }
                    port = Integer.parseInt(address.substring(position, i));
                    break;
                }
            }
            if (port < 0) {
                throw new IOException("malformed address: " + address);
            }

            return new InetSocketAddress(host, port);
        } catch (NumberFormatException e) {
            throw new IOException("malformed address: " + address);
        }
    }

    static int toPort(String address) throws IOException {
        try {
            var position = 0;

            // inet
            position += checkInet(address);

            // host
            position += skipPart(address, position);

            // port
            var port = -1;
            for (var i = position; i < address.length(); i++) {
                var lastByte = i == address.length() - 1;
                if (address.charAt(i) == ':' || lastByte) {
                    if (lastByte) {
                        i++;
                    }
                    port = Integer.parseInt(address.substring(position, i));
                    break;
                }
            }
            if (port < 0) {
                throw new IOException("malformed address: " + address);
            }

            return port;
        } catch (NumberFormatException e) {
            throw new IOException("malformed address: " + address);
        }
    }

    static String toInterface(String address) throws IOException {
        var position = 0;

        // inet
        position += checkInet(address);

        // host
        position += skipPart(address, position);

        // port
        position += skipPart(address, position);

        // interface
        String theInterface = null;
        for (var i = position; i < address.length(); i++) {
            var lastByte = i == address.length() - 1;
            if (address.charAt(i) == ':' || lastByte) {
                if (lastByte) {
                    i++;
                }
                theInterface = address.substring(position, i);
                break;
            }
        }
        if (theInterface == null) {
            throw new IOException("malformed address: " + address);
        }

        return theInterface;
    }

    private static int checkInet(String address) throws IOException {
        if (address.length() < 5 || address.charAt(0) != 'i' || address.charAt(1) != 'n' 
                || address.charAt(2) != 'e' || address.charAt(3) != 't' || address.charAt(4) != ':') {
            throw new IOException("malformed address: " + address);
        }
        return 5;
    }

    private static int skipPart(String address, int index) {
        for (var i = index; i < address.length(); i++) {
            if (address.charAt(i) == ':') {
                return i - index + 1;
            }
        }
        return address.length() - index;
    }
}
