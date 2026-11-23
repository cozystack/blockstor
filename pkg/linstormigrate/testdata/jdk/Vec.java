// SPDX-License-Identifier: Apache-2.0
//
// Generates the vectors of TestLinstorJDKVectors: javac Vec.java && java Vec

import javax.crypto.*;
import javax.crypto.spec.*;
import java.security.*;
import java.util.Base64;
import java.nio.charset.StandardCharsets;

public class Vec {
    static byte[] pad(byte[] d, int fillFrom) {
        int n = 2 + d.length; int size = (n + 31) / 32 * 32;
        byte[] out = new byte[size];
        out[0] = (byte)(d.length >> 8); out[1] = (byte)d.length;
        System.arraycopy(d, 0, out, 2, d.length);
        for (int i = n; i < size; i++) out[i] = (byte)(fillFrom + i);
        return out;
    }
    static byte[] enc(byte[] key, byte[] iv, byte[] pt) throws Exception {
        Cipher c = Cipher.getInstance("AES/CFB/NoPadding");
        c.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"), new IvParameterSpec(iv));
        byte[] ct = c.doFinal(pt);
        byte[] out = new byte[iv.length + ct.length];
        System.arraycopy(iv, 0, out, 0, iv.length);
        System.arraycopy(ct, 0, out, iv.length, ct.length);
        return out;
    }
    static byte[] seq(int from, int n) { byte[] b = new byte[n]; for (int i = 0; i < n; i++) b[i] = (byte)(from + i); return b; }
    public static void main(String[] a) throws Exception {
        String[] passes = {"jdk-ascii-passphrase", "пароль ключ✓ é"};
        Base64.Encoder b64 = Base64.getEncoder();
        for (String p : passes) {
            byte[] pb = p.getBytes(StandardCharsets.UTF_8);
            // LINSTOR, JclSymmetricKeyCipher.getInstanceWithPassword: each byte to a char.
            char[] chars = new char[pb.length];
            for (int i = 0; i < pb.length; i++) chars[i] = (char) pb[i];
            byte[] salt = seq(0x40, 16);
            SecretKeyFactory f = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
            byte[] kek = f.generateSecret(new PBEKeySpec(chars, salt, 5000, 128)).getEncoded();
            byte[] master = seq(0x10, 16);
            byte[] padded = pad(master, 0xa0);
            byte[] mk = enc(kek, seq(0x60, 16), padded);
            byte[] hash = MessageDigest.getInstance("SHA-512").digest(padded);
            byte[] vol = enc(master, seq(0x70, 16), pad("jdkVolumePassword0123456789A=".getBytes(StandardCharsets.US_ASCII), 0xc0));
            System.out.println(b64.encodeToString(pb) + " " + b64.encodeToString(salt) + " " + b64.encodeToString(mk) + " " + b64.encodeToString(hash) + " " + b64.encodeToString(vol));
        }
    }
}
