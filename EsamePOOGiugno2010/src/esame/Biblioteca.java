package esame;
import java.util.*;

public class Biblioteca {
	private Map<String, Libro> codice2libro;
	public Biblioteca() {
		this.codice2libro = new HashMap<String, Libro>();
	}
	public void addLibro(String codice, Libro libro) {
		this.codice2libro.put(codice, libro);
	}
	public List<Autore> autoriGiovani() {
		List<Autore> autoriGiovani = null;
		// codice omesso
		return autoriGiovani;
	}
	public Map<Autore, Set<Libro>> autore2libri() {
		Map<Autore, Set<Libro>> autore2libri = null;
		// codice omesso
		return autore2libri;
	}
	// altri metodi omessi
}
