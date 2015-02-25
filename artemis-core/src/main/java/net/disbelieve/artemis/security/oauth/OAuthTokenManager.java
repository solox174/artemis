package net.disbelieve.artemis.security.oauth;

import net.disbelieve.artemis.security.encryption.Decryptor;
import net.disbelieve.artemis.security.encryption.Encryptor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.util.Date;

/**
 * Created by kmatth002c on 2/2/2015.
 */
public class OAuthTokenManager {
    private ObjectMapper om = new ObjectMapper();

    public static void main (String[] args) {
        ObjectMapper om = new ObjectMapper();
        OAuthTokenManager tokenManager = new OAuthTokenManager();
        OAuthResponse response = new OAuthResponse();
        response.setAccessToken("a");
        response.setExpiresIn(new Date());
        response.set("key", "value");
        String back = tokenManager.build(response);
        response = tokenManager.parse(back);
        System.out.println(back+"\n\n");

        try {
            System.out.println(om.writeValueAsString(response));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    public OAuthResponse parse(String token) {
        OAuthResponse response = null;
        Decryptor decryptor = new Decryptor();
        Reader reader = null;

        try {
            reader = new StringReader(decryptor.decrypt(token));
            response = om.readValue(reader, OAuthResponse.class);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return response;
    }

    public String build(OAuthResponse response) {
        Writer writer = new StringWriter();
        String token = null;
        Encryptor encryptor = new Encryptor();

        try {
            om.writeValue(writer, response);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            token = encryptor.encrypt(writer.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

        return token;
    }
}
