package dht.chord;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class Test {

    public static void main(String[] args) {
        var map = new HashMap<>();
        map.put(12, "qwe");
        map.put(13, "asd");
        System.out.println(map.keySet());

        var list = new ArrayList<>();
        list.add(123);
        list.add(45);
        System.out.println(list.toString());
    }
}
