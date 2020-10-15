package de.gwdg.metadataqa.similarity;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class BitsTest {

  @Test
  public void constructsFromBoolArray() {
    Bits bits = new Bits(new boolean[]{true, true, false});
    assertEquals("[true, true, false]", bits.toString());
  }

  @Test
  public void constructsFromBoolVarargs() {
    Bits bits = Bits.create(true, true, false);
    assertEquals("[true, true, false]", bits.toString());
  }

  @Test
  public void constructsFromIntVarargs() {
    Bits bits = Bits.create(1, 1, 0);
    assertEquals("[true, true, false]", bits.toString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void constructsFromIntVarargs_expectsError() {
    Bits bits = Bits.create(1, 1, 2);
    assertEquals("[true, true, false]", bits.toString());
  }

  @Test
  public void constructsFromInt() {
    Bits bits = new Bits(3);
    assertEquals("[false, false, false]", bits.toString());
  }

  @Test
  public void length() {
    Bits bits = Bits.create(1, 1, 0);
    assertEquals(3, bits.length());
  }

  @Test
  public void cardinality() {
    Bits bits = Bits.create(1, 1, 0);
    assertEquals(2, bits.cardinality());
  }

  @Test
  public void get() {
    Bits bits = Bits.create(1, 1, 0);
    assertEquals(true, bits.get(0));
    assertEquals(false, bits.get(2));
  }

  @Test
  public void set() {
    Bits bits = Bits.create(1, 1, 0);
    bits.set(1, false);
    bits.set(2, true);
    assertEquals("[true, false, true]", bits.toString());
  }

  @Test
  public void compare() {
    Bits a = Bits.create(1, 1, 0);

    assertEquals("[true, true, true]", a.compare(Bits.create(1, 1, 0)).toString());
    assertEquals("[false, false, false]", a.compare(Bits.create(0, 0, 1)).toString());
    assertEquals("[false, true, true]", a.compare(Bits.create(0, 1, 0)).toString());
  }

  @Test
  public void ratio() {
    assertEquals(1.0, Bits.create(1, 1, 1).ratio(), 0.0001);
    assertEquals(0.6666, Bits.create(1, 1, 0).ratio(), 0.0001);
    assertEquals(0.3333, Bits.create(1, 0, 0).ratio(), 0.0001);
    assertEquals(0.0, Bits.create(0, 0, 0).ratio(), 0.0001);
  }

  @Test
  public void compare_withRatio() {
    Bits a = Bits.create(1, 1, 0);

    assertEquals(1.0, a.compare(Bits.create(1, 1, 0)).ratio(), 0.0001);
    assertEquals(0.6666, a.compare(Bits.create(1, 1, 1)).ratio(), 0.0001);
    assertEquals(0.3333, a.compare(Bits.create(0, 1, 1)).ratio(), 0.0001);
    assertEquals(0.0, a.compare(Bits.create(0, 0, 1)).ratio(), 0.0001);
  }

  @Test
  public void similarity() {
    Bits a = Bits.create(1, 1, 0);

    assertEquals(1.0, a.similarity(Bits.create(1, 1, 0)), 0.0001);
    assertEquals(0.6666, a.similarity(Bits.create(1, 1, 1)), 0.0001);
    assertEquals(0.3333, a.similarity(Bits.create(0, 1, 1)), 0.0001);
    assertEquals(0.0, a.similarity(Bits.create(0, 0, 1)), 0.0001);
  }

  @Test(expected = IllegalArgumentException.class)
  public void similarity_withException() {
    Bits a = Bits.create(1, 1, 0);

    assertEquals(1.0, a.similarity(Bits.create(1, 1, 0, 0)), 0.0001);
  }

  @Test(expected = IllegalArgumentException.class)
  public void compare_withException() {
    Bits a = Bits.create(1, 1, 0);

    assertEquals("[true, true, true]", a.compare(Bits.create(1, 1, 0, 1)).toString());
  }
}
