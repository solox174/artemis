package net.disbelieve.artemis.security.encryption;

import sun.misc.BASE64Encoder;

import javax.crypto.Cipher;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.security.Key;
import java.security.KeyStore;
import java.util.Enumeration;

/**
 * Created by kmatth002c on 2/2/2015.
 */
public class Encryptor {
    public String encrypt(String plaintext) throws Exception
    {
        int maxKeyLen = Cipher.getMaxAllowedKeyLength("AES");
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("aes-keystore.jck");
            KeyStore keystore = KeyStore.getInstance("JCEKS");
        keystore.load(inputStream, "mystorepass".toCharArray());
        Enumeration<String> en =  keystore.aliases();

        if (!keystore.containsAlias("jceksaes")) {
            throw new RuntimeException("Alias for key not found");
        }
        Key key = keystore.getKey("jceksaes", "mykeypass".toCharArray());
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        cipher.init(Cipher.ENCRYPT_MODE, key);
        byte[] iv = cipher.getIV();
        byte[] plaintextBytes = plaintext.getBytes();
        byte[] cipherTextBytes = cipher.doFinal(plaintextBytes);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
        outputStream.write(iv);
        outputStream.write(cipherTextBytes);
        BASE64Encoder encoder = new BASE64Encoder();

        return encoder.encode(outputStream.toByteArray());
    }
}
