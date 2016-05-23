package esame;

import java.util.Comparator;

public class ComparatoreAutore implements Comparator<Autore> {

	@Override
	public int compare(Autore a1, Autore a2) {
		return a1.getNome().compareTo(a2.getNome());
	}

}
