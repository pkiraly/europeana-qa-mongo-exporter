package de.gwdg.metadataqa.similarity;

import java.util.Arrays;

public class Bits {
  boolean[] bits;

  public Bits(boolean[] bits) {
    this.bits = bits;
  }

  public Bits(int i) {
    this.bits = new boolean[i];
  }

  public static Bits create(boolean... bits) {
    return new Bits(bits);
  }

  public static Bits create(int... bits) {
    boolean[] proxy = new boolean[bits.length];
    for (int i = 0; i<bits.length; i++) {
      if (bits[i] == 0)
        proxy[i] = false;
      else if (bits[i] == 1)
        proxy[i] = true;
      else
        throw new IllegalArgumentException("The input array should contain only 1s and 0s.");
    }
    return new Bits(proxy);
  }

  public int length() {
    return bits.length;
  }

  public boolean get(int i) {
    if (i < 0 || i >= bits.length)
      throw new ArrayIndexOutOfBoundsException();
    return bits[i];
  }

  public void set(int i, boolean value) {
    if (i < 0 || i >= bits.length)
      throw new ArrayIndexOutOfBoundsException();
    bits[i] = value;
  }

  public int cardinality() {
    int count = 0;
    for (int i = 0; i<this.length(); i++) {
      if (this.get(i) == true)
        count++;
    }
    return count;
  }

  public Bits compare(Bits other) {
    if (this.length() != other.length())
      throw new IllegalArgumentException("The two objects should have equal size.");
    Bits result = new Bits(this.length());
    for (int i = 0; i<this.length(); i++) {
      result.set(i, (this.get(i) == other.get(i)));
    }
    return result;
  }

  public float ratio() {
    return (float)cardinality() / length();
  }

  public float similarity(Bits other) {
    return this.compare(other).ratio();
  }

  @Override
  public String toString() {
    return Arrays.toString(bits);
  }
}
