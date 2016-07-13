package Execucao;

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
		
		SelecionarTuplas st = new SelecionarTuplas();
		st.testarMutantes();
	}

}
