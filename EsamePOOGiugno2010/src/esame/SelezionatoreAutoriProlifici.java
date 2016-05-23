package esame;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class SelezionatoreAutoriProlifici implements Selezionatore{

	@Override
	public List<Autore> eseguiSelezione(List<Libro> libriInBiblioteca) {
		List<Autore> autoriProlifici=new LinkedList<Autore>();
		Map<Autore, Integer> totLibriScritti=this.TotLibriScritti(libriInBiblioteca);
		int massimoLibriScritti=this.MassimoLibriScritti(totLibriScritti);
		for(Autore a:totLibriScritti.keySet()){
			if(totLibriScritti.get(a)==massimoLibriScritti)
				autoriProlifici.add(a);
		}
		return autoriProlifici;
	}

	private int MassimoLibriScritti(Map<Autore, Integer> totLibriScritti) {
		int max=0;
		for(int i:totLibriScritti.values()){
			if(i>max)
				max=i;
		}
		return max;
	}

	private Map<Autore, Integer> TotLibriScritti(List<Libro> libriInBiblioteca) {
		Map<Autore, Integer> totLibriScritti=new TreeMap<Autore,Integer>(new ComparatoreAutore());
		for(Libro l:libriInBiblioteca){
			for(Autore a:l.getAutori()){
				if(totLibriScritti.containsKey(a))
					totLibriScritti.put(a, totLibriScritti.get(a)+1);
				else
					totLibriScritti.put(a, 1);
			}
		}
		return totLibriScritti;
	}

}
