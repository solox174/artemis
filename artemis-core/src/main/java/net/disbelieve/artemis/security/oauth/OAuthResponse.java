package net.disbelieve.artemis.security.oauth;


import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;
import java.util.Hashtable;
import java.util.Map;

/**
 * Created by kmatth002c on 2/2/2015.
 */
public class OAuthResponse {
    @JsonProperty("access_token")
    private String accessToken;
    @JsonProperty("token_type")
    private String tokenType;
    @JsonProperty("expires_in")
    private Date expiresIn;
    @JsonProperty("refresh_token")
    private String refreshToken;
    private String scope;
    private Map<String, String> customProperties = new Hashtable<String, String>();

    public String getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }

    public String getTokenType() {
        return tokenType;
    }

    public void setTokenType(String tokenType) {
        this.tokenType = tokenType;
    }

    public Date getExpiresIn() {
        return expiresIn;
    }

    public void setExpiresIn(Date expiresIn) {
        this.expiresIn = expiresIn;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public void setRefreshToken(String refreshToken) {
        this.refreshToken = refreshToken;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    @JsonAnyGetter
    public Map<String, String> any() {
        return customProperties;
    }

    @JsonAnySetter
    public void set(String name, String value) {
        customProperties.put(name, value);
    }
}
