package Execucao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

import Modelo.Mutantes;

public class SelecionarTuplas {
	
	public ArrayList<Mutantes> buscarMutantes() {
		ArrayList<Mutantes> listaMutantes = new ArrayList<Mutantes>();
		
		Connection con_bdr = null;
		PreparedStatement stm_bdr = null;
		ResultSet rst_bdr = null;
		
		try {
			Class.forName(Main.classe);
			
			con_bdr = DriverManager.getConnection(Main.urlBDR, Main.username, 
					Main.password);
			con_bdr.setAutoCommit(false);
			
			stm_bdr = con_bdr.prepareStatement(
					"Select * from mutantes where id_sqloriginal = " 
					+ Main.idInstrucao);
			rst_bdr = stm_bdr.executeQuery();
			
			while (rst_bdr.next()) {
				Mutantes mut = new Mutantes();
				mut.setIdMutante(rst_bdr.getInt("id_sqlmutante"));
				mut.setSqlMut(rst_bdr.getString("sql"));
				
				listaMutantes.add(mut);
				/*Logs.registrarLog("SelecionarTuplas.java - buscarMutantes() - Mutante adicionado: " +
						mut.getIdExecucao() + ", " + mut.getIdMutante() + " " + mut.getSqlMut());*/
			}
			rst_bdr.close();
			stm_bdr.close();
			
			con_bdr.close();
		} catch (ClassNotFoundException e) {
			Logs.registrarErro("SelecionarTuplas.java - buscarMutantes() - catch - ClassNotFoundException: " + e.getMessage());
			System.exit(1);
		} catch (SQLException e) {
			Logs.registrarErro("SelecionarTuplas.java - buscarMutantes() - catch - SQLException:" + e.getMessage());
			System.exit(1);
		} finally {
			try {
				if (rst_bdr != null) {
					rst_bdr.close();
				}
				if (stm_bdr != null) {
					stm_bdr.close();
				}
				if (con_bdr != null) {
					con_bdr.close();
				}
			} catch (SQLException e) {
				Logs.registrarErro("SelecionarTuplas.java - buscarMutantes() - finally - SQLException:" + e.getMessage());
			}
		}
		
		Logs.registrarLog("Quantidade de mutantes: " + listaMutantes.size());
		
		return listaMutantes;
	}
	
	public void obterResultado(ArrayList<Mutantes> listaMutantes) {
		Connection con_bdp = null;
		PreparedStatement stm_bdp = null;
		Connection con_bdr = null;
		PreparedStatement stm_bdr = null;
		
		//abrindo conexoes
		try {
			Class.forName(Main.classe);
			
			con_bdp = DriverManager.getConnection(Main.urlBDP, Main.username, Main.password);
			con_bdp.setAutoCommit(false);
			
			con_bdr = DriverManager.getConnection(Main.urlBDR, Main.username, Main.password);
			con_bdr.setAutoCommit(false);
		} catch (ClassNotFoundException | SQLException e) {
			Logs.registrarErro("SelecionarTuplas.java - obterResultado() - Abrindo conexões - ClassNotFoundException | SQLException: " + e.getMessage());
			System.exit(1);
		}
		
		for (int i = Main.idResultado; i < listaMutantes.size(); i++) {
			Logs.registrarLog(" i = "+ i +" Mutante " + listaMutantes.get(i).getIdMutante() + ":");
			
			ResultSet rst_bdp = null;
			
			try {
				String sqlMutante = listaMutantes.get(i).getSqlMut();
				sqlMutante = sqlMutante.toLowerCase();
				stm_bdp = con_bdp.prepareStatement(sqlMutante);
				stm_bdp.setQueryTimeout(90);
				rst_bdp = stm_bdp.executeQuery();
				
				ResultSetMetaData rsmd = rst_bdp.getMetaData();
				int countColunas = rsmd.getColumnCount();
				
				int linhas = 0;
				while (rst_bdp.next()) {
					String colunas = "";
					String idColunas = "";
					
					for (int c = 1; c <= countColunas; c++) {
						String nomeColuna = rsmd.getColumnName(c).toUpperCase();
						int idTupla = rst_bdp.getInt(c);
						
						if (colunas == "") {
							colunas = nomeColuna;
							idColunas = idTupla + "";
						} else {
							colunas = colunas + ", " + nomeColuna;
							idColunas = idColunas + ", " + idTupla;
						}
					}
					
					linhas++;
					
					String sql = "insert into chaves (mutante, linha, "
							+ colunas + ") values (" 
							+ listaMutantes.get(i).getIdMutante() + ", " 
							+ linhas + ", " + idColunas + ");";
					stm_bdr = con_bdr.prepareStatement(sql);
					stm_bdr.executeUpdate();
					con_bdr.commit();
					stm_bdr.close();
				}
				rst_bdp.close();
				stm_bdp.close();
				
				Logs.registrarLog("Resultado mutante_" + listaMutantes.get(i).getIdMutante() + " Total de linhas: " + linhas);
				
				if (linhas == 0) {
					stm_bdr = con_bdr.prepareStatement(
							"insert into resultado (r_idexecucao, r_idsqlmutante, resultado) "
							+ "values (" + Main.idExperimento + ", " 
							+ listaMutantes.get(i).getIdMutante() + ", 'Resultado vazio');");
					stm_bdr.executeUpdate();
					con_bdr.commit();
					stm_bdr.close();
					
					Logs.registrarLog("Mutante" + listaMutantes.get(i).getIdMutante() + ": resultado vazio");
				} else {
					if (stm_bdr != null) {
						stm_bdr.close();
					}
					stm_bdr = con_bdr.prepareStatement(
							"insert into resultado (r_idexecucao, r_idsqlmutante, resultado) "
							+ "values (" + Main.idExperimento + ", " 
							+ listaMutantes.get(i).getIdMutante() + ", '"
							+ linhas + " linhas');");		
					stm_bdr.executeUpdate();
					con_bdr.commit();
					stm_bdr.close();
				}
			} catch (SQLException e) {
				Logs.registrarErro("SelecionarTuplas.java - obterResultado() - catch e - SQLException: " + e.getMessage());
				
				try {
					if (rst_bdp != null) {
						rst_bdp.close();
					}
					if (stm_bdp != null) {
						stm_bdp.close();
					}
					if (stm_bdr != null) {
						stm_bdr.close();
					}
					
					stm_bdr = con_bdr.prepareStatement(
							"insert into resultado (r_idexecucao, r_idsqlmutante, time_out, resultado) "
							+ "values (" + Main.idExperimento + ", " 
							+ listaMutantes.get(i).getIdMutante() + ", '1', 'resultado vazio');");
					stm_bdr.executeUpdate();
					con_bdr.commit();
					stm_bdr.close();
					
					Logs.registrarLog("Mutante_" + listaMutantes.get(i).getIdMutante() + ": Time Out");
				} catch (SQLException e1) {
					Logs.registrarErro("SelecionarTuplas.java - obterResultado() - catch e1 - SQLException:" + e1.getMessage());
				}
			}
			
			//chamando o gargage collector
			System.gc();
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				Logs.registrarErro("SelecionarTuplas.java - obterResultado() - InterruptedException:" + e.getMessage());
			}
		}
		
		//fechando conexoes
		try {
			con_bdp.close();
			con_bdr.close();
		} catch (SQLException e) {
			Logs.registrarErro("SelecionarTuplas.java - obterResultado() - Fechando conexoes - SQLException:" + e.getMessage());
			System.exit(1);
		}
	}	
}