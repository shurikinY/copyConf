package com.solanteq.security;

import java.io.Console;
import java.util.Scanner;

public class StdinEncryptorReader implements EncryptorReader {
    private final Console console = System.console();
    private final Scanner stdinScanner = new Scanner(System.in);

    @Override
    public String readString() {
        return stdinScanner.nextLine();
    }

    @Override
    public String readPassword() {
        if (console == null) {
            return stdinScanner.nextLine();
        } else {
            return new String(console.readPassword());
        }
    }
}
