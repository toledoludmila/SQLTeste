package Execucao;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
	
	public static void registrarExecucao(char inicio_fim) {
		Connection con_bdr = null;
		PreparedStatement stm_bdr = null;
		
		try {
			con_bdr = DriverManager.getConnection(Main.urlBDR, Main.username, Main.password);
			con_bdr.setAutoCommit(false);

			if (inicio_fim == 'i') {
				String sqlExecI = "insert into execucao (e_idsql , data_inicio, observacao) values (" 
						+ Main.idInstrucao +",'" + new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new Date()).toString() 
						+ "', 'execucao com time out 90s');";
				registrarLog(sqlExecI);
				stm_bdr = con_bdr.prepareStatement(sqlExecI);
				stm_bdr.executeUpdate();
				con_bdr.commit();
				stm_bdr.close();
				
				stm_bdr = con_bdr.prepareStatement("SELECT LAST_INSERT_ID() as id_execucao from execucao;");
				ResultSet rst_bdr = stm_bdr.executeQuery();
				rst_bdr.next();
				Main.idExperimento = rst_bdr.getInt(1);
				stm_bdr.close();
				registrarLog("Logs.java - registrarExecucao() - Execuçao N: "
						+ Main.idExperimento);
			} else {
				String sqlExecF = "update execucao set data_fim = '"
						+ new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new Date()).toString()
						+ "' where id_execucao = " + Main.idExperimento + ";";
				registrarLog(sqlExecF);
				stm_bdr = con_bdr.prepareStatement(sqlExecF);
				stm_bdr.executeUpdate();
				con_bdr.commit();
				stm_bdr.close();
			}

			con_bdr.close();
		} catch (SQLException e) {
			registrarErro("Logs.java - registrarExecucao() -"+ e.getMessage());
			System.exit(1);
		}
	}
}