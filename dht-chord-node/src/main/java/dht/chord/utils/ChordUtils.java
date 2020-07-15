package dht.chord.utils;

import org.apache.commons.codec.digest.DigestUtils;

import java.math.BigInteger;

public final class ChordUtils {

    public static BigInteger keyToChordID(String key, int m) {
        var idSpaceSize = (int) Math.pow(2, m);
        var hash = DigestUtils.sha1Hex(key);
        var numericValue = new BigInteger(hash, 16);
        return numericValue.mod(BigInteger.valueOf(idSpaceSize));
    }

    public static BigInteger hostPortToChordID(String host, int port, int m) {
        return keyToChordID(String.format("%s:%s", host, port), m);
    }

    public static String makeAddress(BigInteger chordID, String host, int port) {
        return String.format("%s@%s:%s", chordID, host, port);
    }

    public static BigInteger extractChordID(String address) {
        return new BigInteger(address.split("@")[0]);
    }

    public static String extractHost(String address) {
        var hostPort = address.split("@")[1];
        return hostPort.split(":")[0];
    }

    public static int extractPort(String address) {
        var hostPort = address.split("@")[1];
        return Integer.parseInt(hostPort.split(":")[1]);
    }

    public static boolean isInIntervalInclusive(BigInteger left, BigInteger value, BigInteger right, int m) {
        return isInInterval(left, value, right, m, true);
    }

    public static boolean isInIntervalExclusive(BigInteger left, BigInteger value, BigInteger right, int m) {
        return isInInterval(left, value, right, m, false);
    }

    private static boolean isInInterval(BigInteger left, BigInteger value, BigInteger right, int m, boolean inclusive) {
        if (left.compareTo(right) == 0) {
            return true;
        }
        var cycleSize = BigInteger.valueOf(2).pow(m);
        var shift = cycleSize.subtract(left);
        var shiftedValue = value.add(shift).mod(cycleSize);
        var shiftedIncluded = right.add(shift).mod(cycleSize);
        var leftCompare = BigInteger.valueOf(0).compareTo(shiftedValue);
        var rightCompare = shiftedValue.compareTo(shiftedIncluded);
        if (inclusive) {
            return leftCompare == -1 && (rightCompare == -1 || rightCompare == 0);
        } else {
            return leftCompare == -1 && rightCompare == -1;
        }
    }
}
