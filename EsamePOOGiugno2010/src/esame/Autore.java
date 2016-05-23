package esame;

public class Autore {
	private int annoNascita;
	private String nome;
	
	public Autore(String nome, int annoNascita) {
		this.nome = nome;
		this.annoNascita = annoNascita;
	}
	public String getNome() {
		return this.nome;
	}
	public int getAnnoNascita() {
		return this.annoNascita;
	}
	
	@Override
	public boolean equals(Object o){
		Autore that=(Autore)o;
		return this.getNome()==that.getNome()&&that.getAnnoNascita()==this.getAnnoNascita();
	}
	
	@Override
	public int hashCode(){
		return this.getAnnoNascita()+this.getNome().hashCode();
	}
}