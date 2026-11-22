package io.github.bojieli.queqiao;

import android.content.Context;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Reads the packed country route set the build copies from the iOS client, the
 * same decoder as CountryRoutes.swift: "QQGO", a version byte, two big-endian
 * counts at offsets 8 and 12, then 5-byte IPv4 and 17-byte IPv6 entries.
 */
final class CountryRoutes {
    static final String CHINA_ASSET = "cn-direct.bin";
    static final String CHINA_CODE = "CN";

    private static final byte[] MAGIC = {0x51, 0x51, 0x47, 0x4F};
    private static final int FORMAT_VERSION = 1;
    private static final int HEADER_SIZE = 16;
    private static final int IPV4_ENTRY_SIZE = 5;
    private static final int IPV6_ENTRY_SIZE = 17;

    private CountryRoutes() {
    }

    static byte[] packedChinaSet(Context context) throws IOException {
        try (InputStream stream = context.getAssets().open(CHINA_ASSET)) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            byte[] chunk = new byte[8192];
            int read;
            while ((read = stream.read(chunk)) != -1) {
                buffer.write(chunk, 0, read);
            }
            return buffer.toByteArray();
        }
    }

    static int blockCount(byte[] packed) throws IOException {
        int[] counts = counts(packed);
        return counts[0] + counts[1];
    }

    static List<RoutePolicy.RouteSpec> decode(byte[] packed) throws IOException {
        int[] counts = counts(packed);
        int expected = HEADER_SIZE + IPV4_ENTRY_SIZE * counts[0] + IPV6_ENTRY_SIZE * counts[1];
        if (packed.length != expected) {
            throw new IOException("The bundled route set should be " + expected + " bytes but is " + packed.length);
        }
        List<RoutePolicy.RouteSpec> prefixes = new ArrayList<>(counts[0] + counts[1]);
        int offset = HEADER_SIZE;
        for (int index = 0; index < counts[0]; index++, offset += IPV4_ENTRY_SIZE) {
            addPrefix(prefixes, packed, offset, 4);
        }
        for (int index = 0; index < counts[1]; index++, offset += IPV6_ENTRY_SIZE) {
            addPrefix(prefixes, packed, offset, 16);
        }
        return prefixes;
    }

    private static void addPrefix(List<RoutePolicy.RouteSpec> prefixes, byte[] packed, int offset, int width) {
        int length = packed[offset + width] & 0xFF;
        if (length > width * 8) {
            return;
        }
        BigInteger network = new BigInteger(1, Arrays.copyOfRange(packed, offset, offset + width));
        prefixes.add(new RoutePolicy.RouteSpec(network, length, width * 8));
    }

    private static int[] counts(byte[] packed) throws IOException {
        if (packed.length < HEADER_SIZE || !Arrays.equals(Arrays.copyOfRange(packed, 0, 4), MAGIC)) {
            throw new IOException("The bundled route set does not carry the expected header");
        }
        if ((packed[4] & 0xFF) != FORMAT_VERSION) {
            throw new IOException("The bundled route set is format version " + (packed[4] & 0xFF)
                    + ", which this build cannot read");
        }
        return new int[] {readBigEndian32(packed, 8), readBigEndian32(packed, 12)};
    }

    private static int readBigEndian32(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 24)
                | ((bytes[offset + 1] & 0xFF) << 16)
                | ((bytes[offset + 2] & 0xFF) << 8)
                | (bytes[offset + 3] & 0xFF);
    }
}
