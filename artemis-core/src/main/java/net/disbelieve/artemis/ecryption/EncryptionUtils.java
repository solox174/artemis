package com.comcast.artemis.ecryption;

import com.comcast.apps.crypt.MiniEncrypter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.GeneralSecurityException;

/**
 * Created by kmatth207 on 6/9/2015.
 */
public class EncryptionUtils {
    private static final Logger LOG = LoggerFactory.getLogger(EncryptionUtils.class);
    private static MiniEncrypter miniEncrypter= new MiniEncrypter("nAy&~Xz7j:Mfv/A+");

    public static String encrypt(String data) {
        try {
            return miniEncrypter.encrypt(data);
        } catch (GeneralSecurityException e) {
            LOG.error("Could not ecrypt data", e);
        }
        return null;
    }

    public static String decrypt(String data) {
        try {
            return miniEncrypter.decrypt(data);
        } catch (GeneralSecurityException e) {
            LOG.error("Could not decrypt data", e);
        }
        return null;
    }
}
