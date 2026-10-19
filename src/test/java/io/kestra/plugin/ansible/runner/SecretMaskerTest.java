package io.kestra.plugin.ansible.runner;

import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class SecretMaskerTest {

    @Test
    void testDoesNotMaskHostKeyCheckingOrValidateCerts() {
        SecretMasker masker = new SecretMasker();
        Map<String, Object> envVars = Map.of(
            "ANSIBLE_HOST_KEY_CHECKING", "False",
            "validate_certs", true,
            "another_setting", false
        );
        masker.addSensitiveValues(envVars);

        String sampleLog = "Host key checking is False, validate_certs is true, and changed is false";
        assertThat(masker.mask(sampleLog), is(sampleLog));
    }

    @Test
    void testMasksRealSensitiveKeys() {
        SecretMasker masker = new SecretMasker();
        Map<String, Object> envVars = Map.of(
            "vault_password", "mySuperSecret123",
            "api_key", "secret-token-value",
            "ssh_private_key", "ssh-rsa-dummy-private-key-data"
        );
        masker.addSensitiveValues(envVars);

        String sampleLog = "Using password mySuperSecret123 and token secret-token-value with key ssh-rsa-dummy-private-key-data";
        String masked = masker.mask(sampleLog);

        assertThat(masked, is("Using password *** and token *** with key ***"));
    }

    @Test
    void testSkipsShortStringsAndNumbers() {
        SecretMasker masker = new SecretMasker();
        Map<String, Object> envVars = Map.of(
            "vault_password", "123",
            "api_key_port", 8080
        );
        masker.addSensitiveValues(envVars);

        String sampleLog = "Port 8080 and pin 123";
        assertThat(masker.mask(sampleLog), is(sampleLog));
    }
}
