package Execucao;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Logs {

	public static File arquivo;
	public static FileWriter fr;
	
	public static void registrarLog(String texto) {
		String mensagem = "[" 
				+ new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new Date()) 
				+ "] " 
				+ texto;
		
		System.out.println(mensagem);
		
		try {
			fr.write(mensagem + "\n");
			fr.flush();
		} catch (IOException e) {
			System.out.println("Erro: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public static void registrarErro(String texto) {
		String mensagem = "[" 
				+ new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new Date()) 
				+ "] *** ERRO *** "
				+ texto;
		
		System.out.println(mensagem);
		
		try {
			fr.write(mensagem + "\n");
			fr.flush();
		} catch (IOException e) {
			System.out.println("Erro: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public void registrarExecucao(char inicio_fim) {
		
		Connection con_bdr = null;
		try {
			con_bdr = DriverManager.getConnection(Main.urlBDR, Main.username, Main.password);
			Statement stm_bdr = con_bdr.createStatement();

			if (inicio_fim == 'i') {
				
				String sqlt = "insert into execucao (data_inicio) values ('"
						+ new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new Date()).toString() + "');";
				stm_bdr.executeUpdate(sqlt);
				
				ResultSet rst_mut = stm_bdr.executeQuery(
						"SELECT LAST_INSERT_ID() as id_experimento from experimentos");
				rst_mut.next();
				Main.idExperimento = rst_mut.getInt(1);
				
			} else {
				String sqlt = "update experimentos set data_fim = '"
						+ new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new Date()).toString()
						+ "' where id_experimento = " + Main.idExperimento + ";";
				stm_bdr.executeUpdate(sqlt);
			}

			stm_bdr.close();
			con_bdr.close();
		} catch (SQLException e) {
			Main.registrarErro(""+ e.getMessage());
			System.exit(1);
		}
	}
}
