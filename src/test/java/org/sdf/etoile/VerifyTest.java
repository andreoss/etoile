package org.sdf.etoile;


import org.junit.Test;

public final class VerifyTest {
    @Test
    public void shouldCheck() {
        Verify.isSerializable(Object.class);
    }
}