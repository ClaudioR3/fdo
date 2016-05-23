package esame;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class BibliotecaTest {
	private Biblioteca b;
	private Libro l1;

	@Before
	public void setUp() throws Exception {
		b=new Biblioteca();
		l1=new Libro("Libro 1");
		l1.addAutore(new Autore("Pippo", 1800));
	}

	@Test
	public void testAutore2libriConBibliotecaVuota() {
		assertEquals(0,b.autore2libri().size());
	}
	
	@Test
	public void testAutore2libriConUnLibro() {
		b.addLibro("a1", l1);
		assertEquals(1,b.autore2libri().size());
	}
	
	@Test
	public void testAutore2libriConUnLibroSenzaAutore() {
		b.addLibro("a1", new Libro("Libro Sconosciuto"));
		assertEquals(0,b.autore2libri().size());
	}

}
