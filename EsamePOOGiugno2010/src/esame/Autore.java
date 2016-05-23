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
}