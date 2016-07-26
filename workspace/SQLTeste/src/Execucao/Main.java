package Execucao;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import Modelo.Mutantes;

public class Main extends Logs {
	public static String urlBDP = "jdbc:mysql://localhost:3306/tpch";
	public static String urlBDT = "jdbc:mysql://localhost:3306/tpch2";
	public static String urlBDR = "jdbc:mysql://localhost:3306/resultados";
	public static String urlBDM = "jdbc:mysql://localhost:3306/mutantes";
	public static String username = "root";
	public static String password = "senha";
	public static String classe = "com.mysql.jdbc.Driver";
	public static int idInstrucao = 101;
	public static int idExperimento = 0;

	public static void main(String[] args) {
		Main m = new Main();
		Main.arquivo = new File("/home/ludmila/workspace/SQLTeste/experimento.log");
		
		try {
			Main.fr = new FileWriter(Main.arquivo);
		} catch (IOException e) {
			Main.registrarErro("Main.java - main() - " + e.getMessage());
			System.exit(1);
		}
		
		m.registrarExecucao('i');
		
		SelecionarTuplas st = new SelecionarTuplas();
		ArrayList<Mutantes> listaMutantes = st.buscarMutantes();
		st.obterResultado(listaMutantes);
		
		m.registrarExecucao('f');
		
		try {
			fr.close();
		} catch (IOException e) {
			Main.registrarErro("Ag.java - main() - " + e.getMessage());
			System.exit(1);
		}
	}
}
