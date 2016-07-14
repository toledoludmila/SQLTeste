package Execucao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
			Statement stm_bdrs = con_bdr.createStatement();
			
			ResultSet rst_bdr = stm_bdr.executeQuery("Select * from mutantes where id_sqloriginal = " 
					+ Main.idInstrucao);
			int countRst=0;
			
			while (rst_bdr.next()){
				countRst++;
				Main.registrarLog("SelecionarTuplas.java - testarMutantes() - entrou resultset "
						+ countRst + " veses");
				Main.idMutante = rst_bdr.getInt("id_sql");
				String sql_mutante = rst_bdr.getString("sql");
				listaMutantes.add(sql_mutante);
			}
			stm_bdrs.executeUpdate("insert into resultado (r_idexecucao, r_idsql, r_idsqlmutante) "
					+ "values ("+ Main.idExperimento +", "+ Main.idInstrucao +", "+Main.idMutante+");");
			stm_bdrs.executeUpdate("update execucao set e_idsql = "+ Main.idInstrucao);
			
			stm_bdr.close();
			con_bdr.close();
			Main.registrarLog("SelecionarTuplas.java - testarMutantes() - fechou conexao");
		} catch (ClassNotFoundException e) {
			Main.registrarErro("SelecionarTuplas.java - testarMutantes() - Class con_bdr erro: " + e.getMessage());
			System.exit(1);
		} catch (SQLException e) {
			Main.registrarErro("SelecionarTuplas.java - testarMutantes() - con_bdr erro:" + e.getMessage());
			System.exit(1);
		}
		
		for(int i =0; i<listaMutantes.size(); i++){
			Connection con_bdp = null;
			Connection con_bdrs = null;
			try {
				Class.forName(Main.classe);
				con_bdp = DriverManager.getConnection(Main.urlBDP, Main.username, Main.password);
				con_bdrs = DriverManager.getConnection(Main.urlBDR, Main.username, Main.password);
				Statement stm_bdp = con_bdp.createStatement();
				Statement stm_bdr = con_bdrs.createStatement();
				
				String sqlMutante = listaMutantes.get(i);
				//String sqlM = sqlMutante + " into outfile '/tmp/mysql/mutante"+i+"';";
				stm_bdp.setQueryTimeout(60);
				ResultSet rst_bdp= stm_bdp.executeQuery(sqlMutante);
				
				ResultSetMetaData rsmd = rst_bdp.getMetaData(); 
				int countColunas = rsmd.getColumnCount();
				for (int c=1; c<=countColunas; c++){
					while (rst_bdp.next()){
						String nomeColuna = rsmd.getColumnName(c).toUpperCase();
						int idColuna = rst_bdp.getInt(c);
						
						stm_bdr.executeUpdate("update resultado set "+ nomeColuna +" = "+idColuna
								+" where r_idexecucao ="+ Main.idExperimento +";");
					}
				}
				con_bdrs.close();
				con_bdp.close();
			} catch (ClassNotFoundException e) {
				Main.registrarErro("SelecionarTuplas.java - testarMutantes - Classe con_bdp erro: " + e.getMessage());
				System.exit(1);
			} catch (SQLException e) {
				Main.registrarErro("SelecionarTuplas.java - testarMutantes - con_bdp erro: " + e.getMessage());
				try {
					con_bdrs.close();
					con_bdp.close();
				} catch (SQLException e1) {
					Main.registrarErro("SelecionarTuplas.java - testarMutantes - catch do con_bdp.close() erro:" + e1.getMessage());
					System.exit(1);
				}
			}
		}
	}

}
