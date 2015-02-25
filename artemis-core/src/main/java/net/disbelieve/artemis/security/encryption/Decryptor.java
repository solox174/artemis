package net.disbelieve.artemis.security.encryption;

import sun.misc.BASE64Decoder;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.security.Key;
import java.security.KeyStore;

/**
 * Created by kmatth002c on 2/2/2015.
 */
public class Decryptor {
    public String decrypt(String ciphertext) throws Exception
    {
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("aes-keystore.jck");
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        KeyStore keystore = KeyStore.getInstance("JCEKS");
        keystore.load(inputStream, "mystorepass".toCharArray());
        Key key = keystore.getKey("jceksaes", "mykeypass".toCharArray());
        byte[] cipherTextBytes;
        byte[] iv = new byte[16];
        byte[] cipherBytes;


        if (!keystore.containsAlias("jceksaes")) {
            throw new RuntimeException("Alias for key not found");
        }
        cipherTextBytes = new BASE64Decoder().decodeBuffer(ciphertext);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(cipherTextBytes);
        byteArrayInputStream.read(iv);
        cipherBytes = new byte[byteArrayInputStream.available()];
        byteArrayInputStream.read(cipherBytes);
        cipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(iv));

        return new String(cipher.doFinal(cipherBytes));
    }
}
