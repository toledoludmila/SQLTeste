package Execucao;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class ReduzirBanco {
		
	public void inserirTupla(String nomeColuna, int idColuna) throws SQLException{
		//String sql = "";
		String nomeTabela = "";
		
		if (nomeColuna.equalsIgnoreCase("C_CUSTKEY")){
			nomeTabela = "customer";
			//sql = "select * from " + nomeTabela + "where " + nomeColuna + "=" + idColuna ;
		}else if(nomeColuna.equalsIgnoreCase("L_ID")){
			nomeTabela = "lineitem";
			//sql = "select * from " + nomeTabela + "where " + nomeColuna + "=" + idColuna ;
		}else if(nomeColuna.equalsIgnoreCase("N_ID")){
			nomeTabela = "nation";
			//sql = "select * from " + nomeTabela + "where " + nomeColuna + "=" + idColuna ;
		}else if(nomeColuna.equalsIgnoreCase("O_ID")){
			nomeTabela = "orders";
			//sql = "select * from " + nomeTabela + "where " + nomeColuna + "=" + idColuna ;
		}else if(nomeColuna.equalsIgnoreCase("P_PARTKEY")){
			nomeTabela = "part";
			//sql = "select * from " + nomeTabela + "where " + nomeColuna + "=" + idColuna ;
		}else if(nomeColuna.equalsIgnoreCase("S_SUPPKEY")){
			nomeTabela = "supplier";
			//sql = "select * from " + nomeTabela + "where " + nomeColuna + "=" + idColuna ;
		}else if(nomeColuna.equalsIgnoreCase("PS_ID")){
			nomeTabela = "partsupp";
			//sql = "select * from " + nomeTabela + "where " + nomeColuna + "=" + idColuna ;
		}else if(nomeColuna.equalsIgnoreCase("R_ID")){
			nomeTabela = "region";
			//sql = "select * from " + nomeTabela + "where " + nomeColuna + "=" + idColuna ;
		}
		
		//if(! verificarExistencia(nomeTabela, nomeColuna, idColuna)){
			try{
	        	Class.forName(Main.classe);
	        	Connection con_bdt = DriverManager.getConnection(Main.urlBDT, Main.username, Main.password);
	        	Statement stm_bdt = con_bdt.createStatement();
	        	stm_bdt.executeQuery("insert into "+ nomeTabela +" ("+ nomeColuna +") values ("+ idColuna+");");
			} catch (SQLException e){
	            Main.registrarErro("ReduzirBanco. java - verificarExistencia() - con_bdt: " + e.getMessage());
	        } catch (ClassNotFoundException e) {
	        	Main.registrarErro("ReduzirBanco. java - verificarExistencia() - classe: " + e.getMessage());
			} 
		//}
		
	}

		
		public boolean verificarExistencia(String nomeTabela, String nomeColuna, int idColuna) throws SQLException{
	    
	        Connection con_bdt = null;
	        String sql = "SELECT EXISTS(SELECT 1 FROM "+ nomeTabela +" WHERE "+ nomeColuna +" = "+ idColuna +");";
	        
	        try{
	        	Class.forName(Main.classe);
				con_bdt = DriverManager.getConnection(Main.urlBDT, Main.username, Main.password);
				Statement stm_bdt = con_bdt.createStatement();
	            Main.registrarLog("verificar existencia - sql: "+ sql);
				ResultSet resultado = stm_bdt.executeQuery(sql);
	            resultado.next();
	            boolean resultB = resultado.getBoolean(1);
	            
	            con_bdt.close();
	            
	            return resultB;
	            
	        } catch (SQLException e){
	            Main.registrarErro("ReduzirBanco. java - verificarExistencia() - con_bdt: " + e.getMessage());
	            con_bdt.close();
	            return false;
	            
	        } catch (ClassNotFoundException e) {
	        	Main.registrarErro("ReduzirBanco. java - verificarExistencia() - classe: " + e.getMessage());
	        	
				return false;
			} 
	    }
	

		
		
}


