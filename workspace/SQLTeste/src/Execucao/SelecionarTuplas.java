package Execucao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
//import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class SelecionarTuplas {
	
	public void testarMutantes(){
		ArrayList<String> listaMutantes = new ArrayList<String>();
		Connection con_bdr = null;		
		
		try {
			Class.forName(Main.classe);
			con_bdr = DriverManager.getConnection(Main.urlBDR, Main.username, Main.password);
			Statement stm_bdr = con_bdr.createStatement();
			
			ResultSet rst_bdr = stm_bdr.executeQuery("Select * from mutantes where ID_SQLORIGINAL = " + Main.idInstrucao);
			while (rst_bdr.next()){
				int idMutante = rst_bdr.getInt("ID_SQL");
				String sql_mutante = rst_bdr.getString("SQL");
				listaMutantes.add(sql_mutante);
				stm_bdr.executeUpdate("update execucao set E_IDSQL = "+ idMutante);
			}
			con_bdr.close();
		} catch (ClassNotFoundException e) {
			System.out.println("SelecionarTuplas.java - testarMutantes - con_bdr erro: " + e.getMessage());
			System.exit(1);
		} catch (SQLException e) {
			System.out.println("SelecionarTuplas.java - testarMutantes - con_bdr erro:" + e.getMessage());
			System.exit(1);
		}
		
		for(int i =0; i<listaMutantes.size(); i++){
			Connection con_bdp = null;
			try {
				Class.forName(Main.classe);
				con_bdp = DriverManager.getConnection(Main.urlBDP, Main.username, Main.password);
				Statement stm_bdp = con_bdp.createStatement();
				
				String sqlMutante = listaMutantes.get(i);
				String sqlM = sqlMutante + " into outfile '/tmp/mysql/mutante"+i+"';";
				stm_bdp.executeQuery(sqlM);
				
				
				
			} catch (ClassNotFoundException e) {
				System.out.println("SelecionarTuplas.java - testarMutantes - con_bdp erro: " + e.getMessage());
				System.exit(1);
			} catch (SQLException e) {
				System.out.println("SelecionarTuplas.java - testarMutantes - con_bdp erro: " + e.getMessage());
				try {
					con_bdp.close();
				} catch (SQLException e1) {
					System.out.println("SelecionarTuplas.java - testarMutantes - catch do con_bdp.close() erro:" + e1.getMessage());
					System.exit(1);
				}
			}
		}
	}

}
