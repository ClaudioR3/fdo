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
	
	public Map<Autore, Set<Libro>> autore2libri() {
		Map<Autore, Set<Libro>> autore2libri = new HashMap<Autore,Set<Libro>>();
		for(Libro libro:this.codice2libro.values()){
			for(Autore a:libro.getAutori()){
				if(autore2libri.containsKey(a))
					autore2libri.get(a).add(libro);
				else{
					Set<Libro> libri=new HashSet<Libro>();
					libri.add(libro);
					autore2libri.put(a, libri);
				}
			}
		}
		
		return autore2libri;
	}
	
	public List<Autore> seleziona(Selezionatore selezionatore){
		List<Autore> l=null;
		l=selezionatore.eseguiSelezione((List<Libro>)this.codice2libro.values());
		return l;
	}
}
