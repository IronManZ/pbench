package com.tantan.ai.traffic_simulator;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.nio.ByteBuffer;

public class Sampler {

    private int hashId;
    private final BigInteger thousandths;

    public Sampler(int hashId, int thousandths) {
        this.hashId = hashId;
        this.thousandths = BigInteger.valueOf(thousandths);
    }

    public static final byte[] MAX_MD5 = {(byte) 0x7f, (byte)0xff,(byte)0xff,(byte)0xff,
            (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff,
            (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff,
            (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff};

    public static final BigInteger ONE_THOUSAND = new BigInteger("1000");
    public static final BigInteger MAX_MD5_BIGINT = new BigInteger(MAX_MD5);

    public boolean shouldKeep(long user_id) throws Exception {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] concat_user_hash = ByteBuffer.allocate(12).putLong(user_id).putInt(hashId).array();
        md.reset();
        byte[] digested = md.digest(concat_user_hash);
        digested[0] &= 0x7f;
        BigInteger digestedInt = new BigInteger(digested);
        BigInteger c = digestedInt.multiply(ONE_THOUSAND).divide(MAX_MD5_BIGINT);

        return c.compareTo(thousandths) <= 0;
    }

}
