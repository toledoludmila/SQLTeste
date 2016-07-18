package Execucao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ReduzirBanco {
		
	public void inserirTupla(String nomeColuna, int idColuna){
		String sql = "";
		String nomeTabela = "";
		
		if (nomeColuna.equalsIgnoreCase("C_CUSTKEY")){
			nomeTabela = "customerx";
			//sql = "select * from " + nomeTabela + "where " + nomeColuna + "=" + idColuna ;
		}else if(nomeColuna.equalsIgnoreCase("L_ID")){
			nomeTabela = "lineitemx";
			//sql = "select * from " + nomeTabela + "where " + nomeColuna + "=" + idColuna ;
		}else if(nomeColuna.equalsIgnoreCase("N_ID")){
			nomeTabela = "nationx";
			//sql = "select * from " + nomeTabela + "where " + nomeColuna + "=" + idColuna ;
		}else if(nomeColuna.equalsIgnoreCase("O_ID")){
			nomeTabela = "ordersx";
			//sql = "select * from " + nomeTabela + "where " + nomeColuna + "=" + idColuna ;
		}else if(nomeColuna.equalsIgnoreCase("P_PARTKEY")){
			nomeTabela = "partx";
			//sql = "select * from " + nomeTabela + "where " + nomeColuna + "=" + idColuna ;
		}else if(nomeColuna.equalsIgnoreCase("S_SUPPKEY")){
			nomeTabela = "supplierx";
			//sql = "select * from " + nomeTabela + "where " + nomeColuna + "=" + idColuna ;
		}else if(nomeColuna.equalsIgnoreCase("PS_ID")){
			nomeTabela = "partsuppx";
			//sql = "select * from " + nomeTabela + "where " + nomeColuna + "=" + idColuna ;
		}else if(nomeColuna.equalsIgnoreCase("R_ID")){
			nomeTabela = "regionx";
			//sql = "select * from " + nomeTabela + "where " + nomeColuna + "=" + idColuna ;
		}
		
		if(! verificarExistencia(nomeTabela, nomeColuna, idColuna)){
			try{
	        	Class.forName(Main.classe);
	        	Connection con_bdp = DriverManager.getConnection(Main.urlBDP, Main.username, Main.password);
	        	Connection con_bdt = DriverManager.getConnection(Main.urlBDT, Main.username, Main.password);
	        	Statement stm_bdp = con_bdp.createStatement();
	        	Statement stm_bdt = con_bdt.createStatement();
	        	stm_bdp.executeQuery(sql);
			} catch (SQLException e){
	            Main.registrarErro("ReduzirBanco. java - verificarExistencia() - con_bdt: " + e.getMessage());
	        } catch (ClassNotFoundException e) {
	        	Main.registrarErro("ReduzirBanco. java - verificarExistencia() - classe: " + e.getMessage());
			} 
		}
		
	}

		
		public boolean verificarExistencia(String nomeTabela, String nomeColuna, int idColuna){
	    
	        Connection con_bdt = null;
	        String sql = "SELECT EXISTS(SELECT * FROM "+ nomeTabela +" WHERE "+ nomeColuna +" = "+ idColuna +");";
	        
	        try{
	        	Class.forName(Main.classe);
				con_bdt = DriverManager.getConnection(Main.urlBDT, Main.username, Main.password);
				Statement stm_bdt = con_bdt.createStatement();
	            
				ResultSet resultado = stm_bdt.executeQuery(sql);
	            resultado.next();
	            
	            return resultado.getBoolean(1);
	            
	        } catch (SQLException e){
	            Main.registrarErro("ReduzirBanco. java - verificarExistencia() - con_bdt: " + e.getMessage());
	            
	            return false;
	            
	        } catch (ClassNotFoundException e) {
	        	Main.registrarErro("ReduzirBanco. java - verificarExistencia() - classe: " + e.getMessage());
				
				return false;
			} 
	    }
	}


