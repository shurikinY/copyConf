package com.solanteq.security;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;


/**
 * This class can be used as application for encrypting passwords or for decrypting already encrypted passwords.
 * It uses AES algorithm and Base64 encoding for resulting password
 *
 * @author iisaev
 */
public final class PasswordEncryptor {
    public static final String HELP = "Usage: java -jar <this_jar> <options>"
            + "\nExamples: "
            + "\n\t1) java -jar ./target/app.jar"
            + "\n\t2) java -jar ./target/app.jar -v -d"
            + "\n\t3) java -jar ./target/app.jar -k"
            + "\n\t4) java -jar ./target/app.jar -k -v -t -d"
            + "\nOptions:"
            + "\n'--test', '-t' indicates that the tool is used in a testing environment;"
            + "\n'--key', '-k' generate base64 secret key and encrypt it;"
            + "\n'-d' use default passphrase (no extra passphrase requested from the user), works only with '-t' or '--test' option;"
            + "\n'-v' turns on 'verbose' mode (prints extra messages);"
            + "\n'-h' print this help information.";

    /**
     * Name of system property to hold boolean value: if 'true' - ask user to input pass phrase used to generate secret key for
     * encryption/decryption.
     */
    public static final String SYS_PROP_ASK_PASS_PHRASE = "askPassPhrase";

    /**
     * Name of system property with the first component of the passphrase
     */
    public static final String SYS_PROP_COMPONENT_1 = "component1";

    /**
     * Name of system property with the second component of the passphrase
     */
    public static final String SYS_PROP_COMPONENT_2 = "component2";

    public static final String AES = "AES";

    public static final String AES_CBC = "AES/CBC/PKCS5Padding";

    /**
     * Length (in bytes) of the key used for decryption and decryption.
     */
    private static final int SECRET_KEY_LENGTH = 16;

    /**
     * Internal secret bytes, used for encryption/decryption, set up only once per JVM with user input.
     */
    private static volatile byte[] INTERNAL_SECRET_KEY_BYTES = null;

    /**
     * The variable is true if the user specified tha
     */
    private static boolean ALLOW_EMPTY_PASSPHRASE_INPUT = true;

    static EncryptorReader reader = new StdinEncryptorReader();

    /**
     * Encrypt given password
     *
     * @param password password to encrypt
     *
     * @return encrypted password
     */
    public static String encrypt(final String password) {
        try {
            final byte[] passwordBytes = password.getBytes(StandardCharsets.UTF_8);
            final SecretKeySpec key = new SecretKeySpec(getSecretKeyBytes(), AES);
            final Cipher cipher = Cipher.getInstance(AES_CBC);
            cipher.init(Cipher.ENCRYPT_MODE, key);
            final byte[] ivBytes = cipher.getIV();

            final byte[] encryptedBytes = cipher.doFinal(passwordBytes);
            final String encryptedBase64 = Base64.getEncoder().encodeToString(encryptedBytes);
            final String ivBase64 = Base64.getEncoder().encodeToString(ivBytes);

            return ivBase64 + ";" + encryptedBase64;
        } catch (final Exception e) {
            throw new IllegalArgumentException("Failed to encrypt password!", e);
        }
    }

    /**
     * Decrypt password encrypted by {@link #encrypt(String)}
     *
     * @param encrypted previously encrypted password
     *
     * @return original password
     */
    public static String decrypt(final String encrypted) {
        try {
            if (encrypted.contains(";")) {
                final String[] parts = encrypted.split(";");

                final byte[] ivBytes = Base64.getDecoder().decode(parts[0]);
                final byte[] encryptedBytes = Base64.getDecoder().decode(parts[1]);

                final SecretKeySpec key = new SecretKeySpec(getSecretKeyBytes(), AES);
                final Cipher cipher = Cipher.getInstance(AES_CBC);
                cipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(ivBytes));

                final byte[] decryptedBytes = cipher.doFinal(encryptedBytes);
                return new String(decryptedBytes, StandardCharsets.UTF_8);
            } else {
                // compatibility with previously encrypted passwords
                final byte[] encryptedBytes = Base64.getDecoder().decode(encrypted);

                final SecretKeySpec key = new SecretKeySpec(getSecretKeyBytes(), AES);
                final Cipher cipher = Cipher.getInstance(AES);
                cipher.init(Cipher.DECRYPT_MODE, key);

                final byte[] decryptedBytes = cipher.doFinal(encryptedBytes);
                return new String(decryptedBytes, StandardCharsets.UTF_8);
            }
        } catch (final Exception e) {
            throw new IllegalArgumentException("Failed to decrypt password!", e);
        }
    }

    public static void main(final String[] args) {
        final List<String> options = Arrays.asList(args);
        if (options.contains("-h")) {
            display(HELP);
            return;
        }
        final boolean isTestingEnv = options.contains("-t") || options.contains("--test");
        final boolean isVerbose = options.contains("-v");
        final boolean useDefaultSecretKey = options.contains("-d");
        ALLOW_EMPTY_PASSPHRASE_INPUT = isTestingEnv;

        if (!isTestingEnv && useDefaultSecretKey) {
            fatal("Default passphrase could be used only in a testing environments (see `-t` or `--testing` option).");
        }
        if (useDefaultSecretKey) {
            display("The default passphrase would be used for encryption, which is suitable for test system only.");
        } else {
            System.setProperty(SYS_PROP_ASK_PASS_PHRASE, Boolean.TRUE.toString()); // ask pass phrase unless skipped explicitly
        }

        try {
            if (options.contains("-k") || options.contains("--key")) { // encrypted key generation
                display("Enter length or generated key (default 256): ");
                final int keyLength;
                {
                    final String lengthInput = reader.readString();
                    if (!lengthInput.isBlank()) {
                        keyLength = Integer.parseInt(lengthInput);
                    } else {
                        keyLength = 256;
                    }
                }

                final byte[] keyBytes = new byte[keyLength];
                ThreadLocalRandom.current().nextBytes(keyBytes);

                final String keyString = Base64.getEncoder().encodeToString(keyBytes);
                final String encryptedKey = encrypt(keyString);
                if (isVerbose) {
                    display("Original key: " + keyString);
                }
                display("Encrypted key: " + encryptedKey);
            } else { // creation of encrypted password
                display("Enter your password: ");
                final String psw = reader.readPassword();
                final String encryptedPsw = encrypt(psw);
                if (isVerbose) {
                    display("Original password: " + psw);
                }
                display("Encrypted password: " + encryptedPsw);
            }
        } catch (final Exception e) {
            display(e.getLocalizedMessage());
            display(HELP);
        }
    }

    private static boolean isEmpty(final CharSequence sequence) {
        return (null == sequence) || 0 == sequence.length();
    }

    private static byte[] getSecretKeyBytes() {
        final byte[] defaultSecretKeyBytes = new byte[] {
                49, -78, 103, 115,
                -13, -115, 19, -83,
                -87, 10, -46, 102,
                -40, -63, -79, 47
        };
        if (null == INTERNAL_SECRET_KEY_BYTES) {
            synchronized (PasswordEncryptor.class) {
                if (null == INTERNAL_SECRET_KEY_BYTES) {
                    final String userInputProperty = System.getProperty(SYS_PROP_ASK_PASS_PHRASE);
                    final byte[] secretKeyFromEnv = readFromEnv();
                    final byte[] secretKey;
                    if (secretKeyFromEnv == null && "true".equalsIgnoreCase(userInputProperty)) {
                        secretKey = readInteractive();
                    } else {
                        secretKey = secretKeyFromEnv;
                    }
                    INTERNAL_SECRET_KEY_BYTES = Objects.requireNonNullElse(secretKey, defaultSecretKeyBytes);
                }
            }
        }
        return INTERNAL_SECRET_KEY_BYTES;
    }

    private static byte[] readFromEnv() {
        final var component1 = System.getProperty(SYS_PROP_COMPONENT_1);
        final var component2 = System.getProperty(SYS_PROP_COMPONENT_2);
        if (component1 == null || component2 == null) {
            return null;
        }
        display("Using components specified in the environment variables.");
        final var component1Bytes = component1.getBytes(StandardCharsets.UTF_8);
        final var component2Bytes = component2.getBytes(StandardCharsets.UTF_8);
        if (component1Bytes.length < SECRET_KEY_LENGTH || component2Bytes.length < SECRET_KEY_LENGTH) {
            fatal(String.format("Each component's length must be at least %d bytes.", SECRET_KEY_LENGTH));
        }
        if (component1Bytes.length > SECRET_KEY_LENGTH || component2Bytes.length > SECRET_KEY_LENGTH) {
            display(String.format("Only first %d bytes of the component would be used.", SECRET_KEY_LENGTH));
        }
        return mergeComponents(component1Bytes, component2Bytes);
    }

    private static byte[] readInteractive() {
        if (ALLOW_EMPTY_PASSPHRASE_INPUT) {
            display("Enter first component of the passphrase (or leave blank for defaults): ");
        } else {
            display("Enter first component of the passphrase: ");
        }
        final var component1 = readComponent(ALLOW_EMPTY_PASSPHRASE_INPUT);
        if (component1.length == 0) {
            display("The default passphrase is used, which is suitable for test system only.");
            return null;
        }
        display("Enter second component of the passphrase: ");
        final var component2 = readComponent(false);
        return mergeComponents(component1, component2);
    }

    private static byte[] readComponent(final boolean allowEmpty) {
        while (true) {
            final var component = reader.readPassword();
            if (allowEmpty && (isEmpty(component) || isEmpty(component.trim()))) {
                return new byte[] {};
            }
            final var componentBytes = component.getBytes(StandardCharsets.UTF_8);
            if (componentBytes.length < SECRET_KEY_LENGTH) {
                display(String.format("The component must be at least %d bytes long.\nEnter a new one:", SECRET_KEY_LENGTH));
                continue;
            }

            if (componentBytes.length > SECRET_KEY_LENGTH) {
                display(String.format("Only first %d bytes of the component would be used.", SECRET_KEY_LENGTH));
            }
            return Arrays.copyOf(componentBytes, SECRET_KEY_LENGTH);
        }
    }

    private static byte[] mergeComponents(final byte[] component1, final byte[] component2) {
        final var secretKey = new byte[SECRET_KEY_LENGTH];
        for (int i = 0; i < secretKey.length; i++) {
            secretKey[i] = (byte) (component1[i] ^ component2[i]);
        }
        return secretKey;
    }

    private static void display(final String msg) {
        System.out.println(msg);
    }

    private static void fatal(final String msg) {
        System.err.println(msg);
        System.exit(1);
    }
}
