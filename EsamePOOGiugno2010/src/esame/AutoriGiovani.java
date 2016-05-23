package esame;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class AutoriGiovani implements Selezionatore{

	@Override
	public List<Autore> eseguiSelezione(List<Libro> libriInBiblioteca) {
		List<Autore> autoriGiovani=new LinkedList<Autore>();
		Set<Autore> autori=new HashSet<Autore>();
		for(Libro libro:libriInBiblioteca){
			autori.addAll(libro.getAutori());
		}
		int annoDiNascitaAutorePiuGiovane=this.AnnoDiNascitaAutorePiuGiovane(autori);
		for(Autore a:autori){
			if(a.getAnnoNascita()==annoDiNascitaAutorePiuGiovane)
				autoriGiovani.add(a);
		}
		return autoriGiovani;
	}

	private int AnnoDiNascitaAutorePiuGiovane(Set<Autore> autori) {
		int anno=-999999999;		//punto di riferimento dove sicuramente, prima di quest'anno, nessuno ha mai scritto
		for(Autore a:autori){
			if(a.getAnnoNascita()>anno)
				anno=a.getAnnoNascita();
		}
		return anno;
	}
	
}
