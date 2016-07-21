package Execucao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
//import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import Modelo.Mutantes;

public class SelecionarTuplas {
	
	public ArrayList<Mutantes> buscarMutantes(){
		ArrayList<Mutantes> listaMutantes = new ArrayList<Mutantes>();
		Connection con_bdr = null;		
		
		try {
			Class.forName(Main.classe);
			con_bdr = DriverManager.getConnection(Main.urlBDR, Main.username, Main.password);
			Statement stm_bdr = con_bdr.createStatement();
			Statement stm_bdrs = con_bdr.createStatement();
			
			ResultSet rst_bdr = stm_bdr.executeQuery("Select * from mutantes where id_sqloriginal = " 
					+ Main.idInstrucao);
			
			while (rst_bdr.next()){
				Mutantes mut = new Mutantes();
				mut.setIdMutante(rst_bdr.getInt("id_sqlmutante"));
				mut.setSqlMut(rst_bdr.getString("sql"));
				stm_bdrs.executeUpdate("insert into resultado (r_idexecucao, r_idsqlmutante) "
						+ "values ("+ Main.idExperimento +", " + mut.getIdMutante() +");");	
				
				ResultSet rst = stm_bdrs.executeQuery("select LAST_INSERT_ID() as id_resultado from resultado;");
				rst.next();
				mut.setIdExecucao(rst.getInt(1));
				listaMutantes.add(mut);
				//Main.registrarLog("id Mutante: "+ Main.idMutante +"\n Sql: "+ mut.getSqlMut() + "\n id execucao: "+ mut.getIdExecucao());
			}						
			stm_bdr.close();
			stm_bdrs.close();
			con_bdr.close();
			Main.registrarLog("SelecionarTuplas.java - buscarMutantes()- fechou conexao");
		} catch (ClassNotFoundException e) {
			Main.registrarErro("SelecionarTuplas.java - buscarMutantes() - Class con_bdr erro: " + e.getMessage());
			System.exit(1);
		} catch (SQLException e) {
			Main.registrarErro("SelecionarTuplas.java - buscarMutantes() - con_bdr erro:" + e.getMessage());
			System.exit(1);
		}
		return listaMutantes;
	}
	public void obterResultado(ArrayList<Mutantes> listaMutantes){ 
		
		for(int i =0; i<listaMutantes.size(); i++){
			Connection con_bdp = null;
			Connection con_bdr = null;
			try {
				Class.forName(Main.classe);
				con_bdp = DriverManager.getConnection(Main.urlBDP, Main.username, Main.password);
				con_bdr = DriverManager.getConnection(Main.urlBDR, Main.username, Main.password);
				Statement stm_bdp = con_bdp.createStatement();
				Statement stm_bdr = con_bdr.createStatement();
				
				String sqlMutante = listaMutantes.get(i).getSqlMut();
				stm_bdp.setQueryTimeout(90);
				stm_bdp.executeQuery(sqlMutante);
				//Main.registrarLog("sql mutante: "+ sqlMutante + "\nid experimento: "+ listaMutantes.get(i).getIdExecucao());
				ResultSet rst_bdp = stm_bdp.executeQuery(sqlMutante);
				
				ResultSetMetaData rsmd = rst_bdp.getMetaData(); 
				int countColunas = rsmd.getColumnCount();
				int linhas = 0;
											
				if(!rst_bdp.next()){
					stm_bdr.executeUpdate("update resultado set resultado = 'Resultado vazio' where id_resultado = "
							+ listaMutantes.get(i).getIdExecucao() +";");
					Main.registrarLog("Mutante_"+ i +": resultado vazio");
				}else{
					while (rst_bdp.next()){	
						String colunas = "";
						String idColunas ="";
						for (int c=1; c<=countColunas; c++){
							String nomeColuna = rsmd.getColumnName(c).toUpperCase();
							int idTupla = rst_bdp.getInt(c);
							
							if(colunas == ""){
								colunas = nomeColuna;
								idColunas = idTupla+ "";
							}else{
								colunas = colunas + ", " + nomeColuna;
								idColunas = idColunas + ", " + idTupla;
							}
							
						}
						linhas++;
						String sql = "insert into chaves (mutante, linha, "
								+ colunas + ") values ("+listaMutantes.get(i).getIdMutante() 
								+", " + linhas + ", " + idColunas + ");";
						stm_bdr.executeUpdate(sql);
					}
					stm_bdr.executeUpdate("update resultado set resultado = '"
							+ linhas + "' where id_resultado = "+ listaMutantes.get(i).getIdExecucao() +";");
					
					Main.registrarLog("Resultado mutante_"+ i +" Total de linhas: "+ linhas);
				}
				con_bdr.close();
				con_bdp.close();
			} catch (ClassNotFoundException e) {
				Main.registrarErro("SelecionarTuplas.java - obterResultado() - Classe con_bdp erro: " + e.getMessage());
				System.exit(1);
			} catch (SQLException e) {
				Main.registrarErro("SelecionarTuplas.java - obterResultado() - con_bdp erro: " + e.getMessage());
				try {
					Class.forName(Main.classe);
					con_bdr = DriverManager.getConnection(Main.urlBDR, Main.username, Main.password);
					Statement stm_bdr = con_bdr.createStatement();
					stm_bdr.executeUpdate("update resultado set time_out = 1 where id_resultado = "+ listaMutantes.get(i).getIdExecucao() + ";");
					stm_bdr.executeUpdate("update resultado set resultado = 'Resultado Vazio' where id_resultado = "+ listaMutantes.get(i).getIdExecucao() + ";");
					Main.registrarLog("Mutante_"+ i +": Time Out");
					con_bdr.close();
					con_bdp.close();
				} catch (SQLException | ClassNotFoundException e1) {
					Main.registrarErro("SelecionarTuplas.java - obterResultado() - catch do con_bdp.close() erro:" + e1.getMessage());
					System.exit(1);
				}
			}
		}
		//return listaResultados;
	}	
}