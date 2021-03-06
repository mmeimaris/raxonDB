package com.athena.imis.runnables;

import gnu.trove.map.hash.THashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PushbackReader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import com.athena.imis.models.BigCharacteristicSet;

public class RelationalLoader {

	public static Map<String, Integer> propertiesSet = new THashMap<String, Integer>();
	public static Map<Integer, String> revPropertiesSet = new THashMap<Integer, String>();
	public static Map<String, Integer> intMap = new THashMap<String, Integer>();
	public static Map<Integer, String> revIntMap = new THashMap<Integer, String>();
	
	
	public static void main(String[] args) {
		
		
		System.out.println("Starting time: " + new Date().toString());
		int batchSize = Integer.parseInt(args[3]);
		Connection c = null;
	      Statement stmt = null;
	      try {
	         Class.forName("org.postgresql.Driver");

	         c = DriverManager
	        		 .getConnection("jdbc:postgresql://"+args[0]+":5432/", args[4], args[5]);
	         
	         Statement cre = c.createStatement();
	         cre.executeUpdate("DROP DATABASE IF EXISTS "+args[2]+" ;");	         
	         
	         cre.executeUpdate("CREATE DATABASE "+args[2]+" ;");
	         cre.close();
	         c.close();
	         c = DriverManager
	        		 .getConnection("jdbc:postgresql://"+args[0]+":5432/" + args[2], args[4], args[5]);			         			        				
	         
	         System.out.println("Opened database successfully");
	         
	      } catch ( Exception e ) {
	         System.err.println( e.getClass().getName()+": "+ e.getMessage() );
	         System.exit(0);
	      }	      
	     
	     
		int next = 0;
		//LLongArray l = LArrayJ.newLLongArray(model.size());
							
		
		//final ArrayList<int[]> array = new ArrayList<int[]>();
		
		/*for(int i = 0; i < array.size(); i++){
			int[] ar = new int[4];
			ar[3] = -1;
			array.add(i, ar);
		}*/
		
		
		int propIndex = 0, nextInd = 0;
		long start = System.nanoTime();
		
		int triplesParsed2 = 0;
		
		FileInputStream is;
		try {
			is = new FileInputStream(args[1]);
			NxParser nxp = new NxParser();
			//RdfXmlParser nxp = new RdfXmlParser(); 
			//nxp.parse(is, "http://ex");
			nxp.parse(is);
			for (Node[] nx : nxp){
				triplesParsed2++;
				//if(triplesParsed2 == 10000) break;
			  // prints the subject, eg. <http://example.org/>
			  //System.out.println(nx[0] + " " + nx[1] + " " + nx[2]);
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		
		
		
	    System.out.println("triplesParsed: " + triplesParsed2);
	    System.out.println(System.nanoTime()-start);
	    final int[][] array = new int[triplesParsed2][4];
	   
	    String s, p, o;
	    	   
	   
	    try {
	    	int[] ar ;
			is = new FileInputStream(args[1]);
			NxParser nxp = new NxParser();
			//RdfXmlParser nxp = new RdfXmlParser(); 
			//nxp.parse(is, "http://ex");
			nxp.parse(is);
			for (Node[] nx : nxp){
				//triplesParsed2++;
				//if(triplesParsed2-- == 0) break;
			  // prints the subject, eg. <http://example.org/>
			  //System.out.println(nx[0] + " " + nx[1] + " " + nx[2]);
				s = nx[0].toString();
				p = nx[1].toString();
				o = nx[2].toString();
				if(!propertiesSet.containsKey(p)){							
				    
		    		revPropertiesSet.put(propIndex, p);
		    		propertiesSet.put(p, propIndex++);	    		
			    	
		    	}
				
		    	if(!intMap.containsKey(s)){		    				    
		    	
		    		//revIntMap.put(nextInd, s);
		    		intMap.put(s, nextInd++);
		    		
		    	}
				
		    	if(!intMap.containsKey(o)){		   			   
		    	
		    		//if(triple.getObject().isURI())
		    		//revIntMap.put(nextInd, o);
		    		intMap.put(o, nextInd++);
		    		//else
		    			//intMap.put(o, Integer.MAX_VALUE);
		    		
		    	}
		            
		    	ar = new int[4];
				ar[0] = intMap.get(s);//spLong;
				ar[1] = propertiesSet.get(p);//spLong;
				ar[2] = intMap.get(o);//spLong;			
				ar[3] = -1;
				//array.add(next, ar);
				array[next++] = ar;
				//next++;
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 	
	  
	    long end = System.nanoTime();
		
				
		System.out.println("piped: " + (end-start));
		
		StringBuilder sb2 = new StringBuilder();
		CopyManager cpManager2;
		System.out.println("Adding keys to dictionary. " + new Date().toString());
		try {			
			stmt = c.createStatement();
		    stmt.executeUpdate("CREATE TABLE IF NOT EXISTS dictionary (id INT, label INT); ");
		    stmt.close();		       
		        
			
			cpManager2 = ((PGConnection)c).getCopyAPI();
			PushbackReader reader2 = new PushbackReader( new StringReader(""), 20000 );
			
			Iterator<Map.Entry<String, Integer>> keyIt = intMap.entrySet().iterator();
			int iter = 0;
			while(keyIt.hasNext())
			{
				Entry<String, Integer> nextEntry = keyIt.next();
			    sb2.append(nextEntry.getValue()).append(",")		      
			      .append(nextEntry.getKey().hashCode()).append("\n");
			    if (iter++ % batchSize == 0)
			    {
			      reader2.unread( sb2.toString().toCharArray() );
			      cpManager2.copyIn("COPY dictionary FROM STDIN WITH CSV", reader2 );
			      sb2.delete(0,sb2.length());
			    }
			    keyIt.remove();
			}
			reader2.unread( sb2.toString().toCharArray() );
			cpManager2.copyIn("COPY dictionary FROM STDIN WITH CSV", reader2 );
			reader2.close();
		} catch (SQLException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Done adding keys to dictionary. " + new Date().toString());
		
		Arrays.sort(array, new Comparator<int[]>() {
		    public int compare(int[] s1, int[] s2) {
		        if (s1[0] > s2[0])
		            return 1;    // tells Arrays.sort() that s1 comes after s2
		        else if (s1[0] < s2[0])
		            return -1;   // tells Arrays.sort() that s1 comes before s2
		        else {
		            return 0;
		        }
		    }
		});
		end = System.nanoTime();
		System.out.println("sorting: " + (end-start));
		
		int previousSubject = Integer.MIN_VALUE;
		
		TIntHashSet properties = new TIntHashSet();
		
		HashMap<BigCharacteristicSet, Integer> ucs = new HashMap<>();

		int csIndex = 0;
		/*for(int i = 0; i < l.size(); i++){
			long t = l.apply((long)i);*/
		int previousStart = 0;
		BigCharacteristicSet cs = null;
		int[] t ;
		int subject ;
		int prop ;
		
		Map<Integer, Integer> dbECSMap = new THashMap<Integer, Integer>(array.length/10);
		//Map<Integer, BigCharacteristicSet> rucs = new HashMap<Integer, BigCharacteristicSet>();
		
		for(int i = 0; i < array.length; i++){
			t = array[i];
			subject = t[0];
			prop = t[1];
			
			if(i > 0 && previousSubject != subject){
									
				cs = new BigCharacteristicSet(properties, true);					
				if(!ucs.containsKey(cs)){
					
					dbECSMap.put(previousSubject, csIndex);
					//rucs.put(csIndex, cs);
					for(int j = previousStart; j < i; j++)
						array[j][3] = csIndex;
					ucs.put(cs, csIndex++);
					
					
				}
				else{
					dbECSMap.put(previousSubject, ucs.get(cs));
					//array[i-1][3] = ucs.get(cs);
					for(int j = previousStart; j < i; j++)
						array[j][3] = ucs.get(cs);
				}
				previousStart = i;
				properties.clear();
			}
			if(!properties.contains(prop))
				properties.add(prop);
			previousSubject = subject;
		}
		
		
		if(!properties.isEmpty()){
			cs = new BigCharacteristicSet(properties, true);
			if(!ucs.containsKey(cs)){				
				for(int j = previousStart; j < array.length; j++)
					array[j][3] = csIndex;
				dbECSMap.put(previousSubject, csIndex);				
				ucs.put(cs, csIndex);
				
			}
			else{
				for(int j = previousStart; j < array.length; j++)
					array[j][3] = ucs.get(cs);				
				dbECSMap.put(previousSubject, ucs.get(cs));
			}
			
		}
		end = System.nanoTime();
		System.out.println("ucs time: " + (end-start));
		start = System.nanoTime();
		Arrays.sort(array, new Comparator<int[]>() {
		    public int compare(int[] s1, int[] s2) {
		        if (s1[3] > s2[3])
		            return 1;    // s1 comes after s2
		        else if (s1[3] < s2[3])
		            return -1;   // s1 comes before s2
		        else {			          
		            return 0;
		        }
		    }
		});	
		
		ArrayList<int[]> tripleListFull = new ArrayList<int[]>();
				
		Map<Integer, int[][]> csMapFull = new HashMap<Integer, int[][]>();		
		csMapFull.clear();
		csIndex = array[0][3];		
		int[][] resultFull ;
		for(int i = 0; i < array.length; i++){
			
			t = array[i];
			
			if(csIndex != t[3]){
				//System.out.println(csIndex);
				//long[] result = tripleList.stream().mapToLong(k -> k).toArray();
				resultFull = new int[tripleListFull.size()][3];
				for(int ir = 0; ir < tripleListFull.size(); ir++){
					resultFull[ir] = tripleListFull.get(ir);
				}
				
				csMapFull.put(csIndex, resultFull);
				
				tripleListFull = new ArrayList<int[]>();
			}
			csIndex = t[3];
	
			tripleListFull.add(t);
			
		}		
		
		resultFull = new int[tripleListFull.size()][3];
		for(int i = 0; i < tripleListFull.size(); i++){
			resultFull[i] = tripleListFull.get(i);
		}
					
		csMapFull.put(csIndex, resultFull);
		
		end = System.nanoTime();
		System.out.println("ucs2 time: " + (end-start));		
		System.out.println("csMapFull size: " + csMapFull.size());

				
		HashSet<String> csPairs = new HashSet<String>();
		HashMap<String, Set<Integer>> csPairProperties = new HashMap<String, Set<Integer>>();		
		HashMap<Integer, int[]> csProps = new HashMap<Integer, int[]>(); 
						
		CopyManager cpManager;
		try {
			cpManager = ((PGConnection)c).getCopyAPI();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return;
		}
		
		//init refs
		int idx, min;
		PushbackReader reader ;
		StringBuilder createTableQuery ;
		HashSet<Integer> propertiesMap ;
		HashMap<Integer, HashMap<Integer, HashSet<Integer>>> spoValues ;
				
		
		//for(Integer nextCS : csMapFull.keySet()){
		Iterator<Map.Entry<Integer, int[][]>> it = csMapFull.entrySet().iterator();
		
//		}
//		Iterator<Integer> it = csMapFull.keySet().iterator();
		int nextCS;
		while(it.hasNext()){
			Entry<Integer, int[][]> nextEntry = it.next();
			nextCS = nextEntry.getKey();
			int[][] triplesArray = nextEntry.getValue();
			System.out.println("Next CS: " + nextCS);
			Set<Integer> multiValuedProperties = new HashSet<Integer>();
			String stringHash ;
			Set<String> spSet = new HashSet<String>();
			for(int[] tripleNext : triplesArray){
				stringHash = tripleNext[0] + "_" + tripleNext[1];
				if(spSet.contains(stringHash))
					multiValuedProperties.add(tripleNext[1]);
				spSet.add(stringHash);
			}
			createTableQuery = new StringBuilder();
			createTableQuery.append("CREATE TABLE IF NOT EXISTS cs_" + nextCS + " (s INT, ");
			
			propertiesMap = new HashSet<Integer>();
			
			//for(int[] tripleNext : csMapFull.get(nextCS)){
			
			for(int[] tripleNext : triplesArray){
				propertiesMap.add(tripleNext[1]);
			}
			String cs_properties_query = "CREATE TABLE IF NOT EXISTS cs_schema (id INT, properties integer[]); INSERT INTO cs_schema (id, properties) VALUES ";
			cs_properties_query += "( " + nextCS + ", "; 
			
			int[] props ;
			
			
			props = new int[propertiesMap.size()];
			int propIdx = 0;
			ArrayList<Integer> sortedProperties = new ArrayList<Integer>(propertiesMap);
			Collections.sort(sortedProperties);
			for(int property : sortedProperties){
				props[propIdx++] = property;
				if(!multiValuedProperties.contains(property))
					createTableQuery.append("p_"+property + " INT, ");
				else{
					createTableQuery.append("p_"+property + " INT[], ");
				}
					
			}
			
			csProps.put(nextCS, props);
			cs_properties_query += "ARRAY" + Arrays.toString(props) + ") ";
			
			createTableQuery.deleteCharAt(createTableQuery.length()-2);
			createTableQuery.append(')');
			createTableQuery.append(';');
			
			
			
			try{				
				//c.setAutoCommit(false);
				stmt = c.createStatement();
		        stmt.executeUpdate(createTableQuery.toString());
		        stmt.close();		       
		        
			} catch (Exception e){
				e.printStackTrace();
				return ;
			}
			
			if(!multiValuedProperties.isEmpty()){
				String multiValued = "CREATE TABLE IF NOT EXISTS multi_valued (cs int, p int); ";
				String multiValuedValues = "";
				for(Integer mp : multiValuedProperties){
					multiValuedValues += "("+nextCS+", "+mp+"), ";
				}
				multiValuedValues = multiValuedValues.substring(0, multiValuedValues.length()-2) + "; ";
				try{				
					//c.setAutoCommit(false);
					stmt = c.createStatement();
			        stmt.executeUpdate(multiValued+" INSERT INTO multi_valued (cs, p) VALUES " + multiValuedValues+"; ");
			        stmt.close();		       
			        
				} catch (Exception e){
					e.printStackTrace();
					return ;
				}
			}
			
			//createTableQuery = new StringBuilder();
			//createTableQuery.append(" INSERT INTO cs_" + nextCS + " VALUES (");
			
			//for(int property : sortedProperties){
				//createTableQuery.append("p_"+property + ", ");
				//createTableQuery.append("?, ");
			//}
			//createTableQuery.deleteCharAt(createTableQuery.length()-2);			
			//createTableQuery.append(") ");
			
			StringBuilder sb = new StringBuilder();
			
			
			try{
			//PreparedStatement insert = c.prepareStatement(createTableQuery.toString());
				
				
				HashMap<Integer, HashSet<Integer>> poValues ;
				HashSet<Integer> oValues ;
				int[] valueArray ;
				spoValues = new HashMap<Integer, HashMap<Integer, HashSet<Integer>>>();
				System.out.println("# of Triples: " + triplesArray.length);
				
				//let's try sorting to speed things up
				Arrays.sort(triplesArray, new Comparator<int[]>() {
				    public int compare(int[] s1, int[] s2) {
				        if (s1[0] > s2[0])
				            return 1;    // s1 comes after s2
				        else if (s1[0] < s2[0])
				            return -1;   // s1 comes before s2
				        else {	
				        		return 0;
				        }
				    }
				});
				int prevSubject = triplesArray[0][0], previousProperty = triplesArray[0][1];
				sb.append(prevSubject).append(',');
				poValues = new HashMap<Integer, HashSet<Integer>>();
				int rowBatch = 0;
				
				for(int[] tripleNext : triplesArray){
										
					if(prevSubject != tripleNext[0]){
						//wrap up and go to next subject							
						
						for(int nextProperty : sortedProperties){									
							if(poValues.containsKey(nextProperty)){
								if(poValues.get(nextProperty).size() > 1){
									//multisb.append(prevSubject).append(",").append(nextProperty).append(",");
									valueArray = new int[poValues.get(nextProperty).size()];
									idx = 0;
									min = Integer.MAX_VALUE;
									List<Integer> multiString = new ArrayList<Integer>();
									for(Integer nextObject : poValues.get(nextProperty)){
										valueArray[idx++] = nextObject;
										//min = Math.min(min, nextObject);												
										multiString.add(nextObject);												
										
									}
									//sb.append(min).append(",");
									int[] integerArray = multiString.stream().mapToInt(i->i).toArray();
									String arrS = Arrays.toString(integerArray).replace('[', '{').replace(']', '}');
									sb.append("\""+arrS+"\"").append(",");
									
								}
								else{
									for(Integer nextObject : poValues.get(nextProperty)){
										if(multiValuedProperties.contains(nextProperty)){
											sb.append("\"{"+nextObject+"}\"").append(",");
										}
										else
											sb.append(nextObject).append(",");
									}
								}
							}
							else{
								sb.append("null").append(",");
							}
							
																	   
						}						
						sb.deleteCharAt(sb.length()-1);
						sb.append("\n");
					    if (rowBatch++ % batchSize == 0)
					    {
					      reader = new PushbackReader( new StringReader(""), sb.length() );
					      reader.unread( sb.toString().toCharArray() );
					      cpManager.copyIn("COPY cs_" + nextCS + " FROM STDIN WITH CSV NULL AS 'null'", reader );
					      sb.delete(0,sb.length());
					      if (rowBatch++ % 1000000 == 0)
					    	  System.out.println("Next checkpoint: " + rowBatch);
					    }
						poValues = new HashMap<Integer, HashSet<Integer>>();						
						sb.append(prevSubject).append(',');
					}
					oValues = poValues.getOrDefault(tripleNext[1], new HashSet<Integer>());
					oValues.add(tripleNext[2]);			
					if(dbECSMap.containsKey(tripleNext[2])){
						//System.out.println("1"  + rucs.get(dbECSMap.get(tripleNext[2])));
						//System.out.println("2" + csToPathMap.get(rucs.get(dbECSMap.get(tripleNext[2]))));
						if(dbECSMap.containsKey(tripleNext[2])){
							csPairs.add(""+nextCS +"_"+dbECSMap.get(tripleNext[2]));
							Set<Integer> ecsProp = csPairProperties.getOrDefault(""+nextCS +"_"+dbECSMap.get(tripleNext[2]), new HashSet<Integer>());
							ecsProp.add(tripleNext[1]) ;
							csPairProperties.put(""+nextCS +"_"+dbECSMap.get(tripleNext[2]), ecsProp) ;
						}
						
					}
					poValues.put(tripleNext[1], oValues);
					
					prevSubject = tripleNext[0];
						
				}
				
				for(int nextProperty : sortedProperties){
					if(poValues.containsKey(nextProperty)){
						if(poValues.get(nextProperty).size() > 1){
							
							valueArray = new int[poValues.get(nextProperty).size()];
							idx = 0;
							min = Integer.MAX_VALUE;
							List<Integer> multiString = new ArrayList<Integer>();
							for(Integer nextObject : poValues.get(nextProperty)){
								valueArray[idx++] = nextObject;
								//min = Math.min(min, nextObject);												
								multiString.add(nextObject);												
								
							}										
								//insert?									
							//sb.append(min).append(",");
							int[] integerArray = multiString.stream().mapToInt(i->i).toArray();
							String arrS = Arrays.toString(integerArray).replace('[', '{').replace(']', '}');
							sb.append("\""+arrS+"\"").append(",");
							
						}
						else{
							for(Integer nextObject : poValues.get(nextProperty)){
								if(multiValuedProperties.contains(nextProperty)){
									sb.append("\"{"+nextObject+"}\"").append(",");
								}
								else
									sb.append(nextObject).append(",");
							}
						}
					    
					}
					else{																																		
						sb.append("null").append(",");
					}
					
															   
				}	
				//last line
				sb.deleteCharAt(sb.length()-1);
				sb.append("\n");
			    
			    reader = new PushbackReader( new StringReader(""), sb.length() );
			    reader.unread( sb.toString().toCharArray() );
			    cpManager.copyIn("COPY cs_" + nextCS + " FROM STDIN WITH CSV NULL AS 'null'", reader );
			    sb.delete(0,sb.length());
			    
			    //create gin indexes
			    for(Integer mp : multiValuedProperties){
					//multiValuedValues += "("+nextPathIndex+", "+mp+"), ";
			    	String ginIndexS = " CREATE INDEX cs"+nextCS +"_p"+mp+"_gin ON cs_"+nextCS +" USING gin (p_"+mp+") ;";
			    	try{				
						//c.setAutoCommit(false);
						stmt = c.createStatement();
				        stmt.executeUpdate(ginIndexS);
				        stmt.close();	       
				        
					} catch (Exception e){
						e.printStackTrace();
					}
				}
								
					
				it.remove();
				System.out.println("Removed CS from cs Map.");
				
				//System.out.println("# of subjects: " + spoValues.keySet().size());
				for(int nextSubject : spoValues.keySet()){
					if(true) break;
					//cssMap.put(nextSubject, nextCS);
					//int rowIdx = 1;
					createTableQuery.append("( "+nextSubject + ", ");
					sb.append(nextSubject).append(',');
					poValues = spoValues.get(nextSubject);
					//ArrayList<Integer> sortedProperties = new ArrayList<Integer>(propertiesMap);
					//Collections.sort(sortedProperties);
					for(int nextProperty : sortedProperties){
						if(poValues.get(nextProperty).size() > 1){
							valueArray = new int[poValues.get(nextProperty).size()];
							idx = 0;
							min = Integer.MAX_VALUE;
							for(Integer nextObject : poValues.get(nextProperty)){
								valueArray[idx++] = nextObject;	
								min = Math.min(min, nextObject);
							}
							//createTableQuery.append("ARRAY" + Arrays.toString(valueArray) + ", ");
							//createTableQuery.append("" + min + ", ");
							//insert.setInt(rowIdx++, min);
							sb.append(min).append(",");
						      //.append(timestamps[i]).append("',")
						      //.append(values[i]).append("\n");
						}
						else{
							for(Integer nextObject : poValues.get(nextProperty)){
								//createTableQuery.append("" + nextObject + ", ");
								//insert.setInt(rowIdx++, nextObject);
								sb.append(nextObject).append(",");
							}						
						}
						
																   
					}
					//createTableQuery.deleteCharAt(createTableQuery.length()-2);
					//createTableQuery.append("), ");
					//insert.addBatch();
					  //if (rowBatch++ % batchSize == 0) { insert.executeBatch(); }
					sb.deleteCharAt(sb.length()-1);
					sb.append("\n");
				    if (rowBatch++ % batchSize == 0)
				    {
				      reader = new PushbackReader( new StringReader(""), sb.length() );
				      reader.unread( sb.toString().toCharArray() );
				      cpManager.copyIn("COPY cs_" + nextCS + " FROM STDIN WITH CSV", reader );
				      sb.delete(0,sb.length());
				      if (rowBatch++ % 1000000 == 0)
				    	  System.out.println("Next checkpoint: " + rowBatch);
				    }
				}
				//insert.executeBatch();
			}
			catch (Exception e){
				e.printStackTrace();
			}
			
			
			//createTableQuery.deleteCharAt(createTableQuery.length()-2);
			//System.out.println(createTableQuery.toString());
			
			//if(true) continue;
			try{				
				//c.setAutoCommit(false);
//				stmt = c.createStatement();
//		        stmt.executeUpdate(createTableQuery.toString());
//		        stmt.close();
		        Statement stmt2 = c.createStatement();
				stmt2.executeUpdate(cs_properties_query);
				stmt2.close();
		        
			} catch (Exception e){
				e.printStackTrace();
			}

	         
			
		}
		System.out.println(csPairs.size());
		StringBuilder ecsQuery = new StringBuilder();
		ecsQuery.append("CREATE TABLE IF NOT EXISTS ecs_schema (id INT, css INT, cso INT, css_properties int[], cso_properties int[]); ");
		ecsQuery.append("INSERT INTO ecs_schema (id, css, cso, css_properties, cso_properties) VALUES ");
		idx = 0;
		for(String csPair : csPairs){
			String[] split = csPair.split("_");
			ecsQuery.append(" ("+(idx++)+", "+ split[0] + ", " + split[1] + ", "
					+ "ARRAY"+Arrays.toString(csProps.get(Integer.parseInt(split[0])))+", "
					+ "ARRAY"+Arrays.toString(csProps.get(Integer.parseInt(split[1]))) +") ");
			if(idx < csPairs.size())
				ecsQuery.append(", ");
			else
				ecsQuery.append("; ");
			
			Set<Integer> props = csPairProperties.get(csPair) ;
			for(Integer nextProp : props){
				
				String index = " CREATE INDEX IF NOT EXISTS cs_"+split[0]+"_p"+nextProp+" ON cs_"+split[0]+" (p_"+nextProp+") " ;
				
				
				//System.out.println(index);
				try{				
					//c.setAutoCommit(false);
					stmt = c.createStatement();
			        stmt.executeUpdate(index);
			        stmt.close();	       
			        
				} catch (Exception e){
					e.printStackTrace();
				}
			}
			String index = " CREATE INDEX IF NOT EXISTS cs_"+split[1]+"_s ON cs_"+split[1]+" (s) " ;
			try{				
				//c.setAutoCommit(false);
				stmt = c.createStatement();
		        stmt.executeUpdate(index);
		        stmt.close();	       
		        
			} catch (Exception e){
				e.printStackTrace();
			}
			
			//idx++;
		}
		//System.out.println(ecsQuery);
		try{				
			//c.setAutoCommit(false);
			stmt = c.createStatement();
	        stmt.executeUpdate(ecsQuery.toString());
	        stmt.close();	       
	        
		} catch (Exception e){
			e.printStackTrace();
		}
		
		String propertiesSetQuery = "CREATE TABLE IF NOT EXISTS propertiesSet (id INT, uri TEXT) ; "
				+ "INSERT INTO propertiesSet (id, uri) VALUES ";
		int propCount = 0;
		for(int nextProp : revPropertiesSet.keySet()){
			propertiesSetQuery += "(" + nextProp + ", '" + revPropertiesSet.get(nextProp) + "') ";
			if(propCount < revPropertiesSet.size()-1)
				propertiesSetQuery += ", ";
			else
				propertiesSetQuery += "; ";
			propCount++;
		}
		//System.out.println(propertiesSetQuery);
		try{				
			//c.setAutoCommit(false);
			stmt = c.createStatement();
	        stmt.executeUpdate(propertiesSetQuery.toString());
	        stmt.close();	       
	        
		} catch (Exception e){
			e.printStackTrace();
		}
		//List<List<Integer>> sortECSList = new List<Integer>(cs);
		
		
		
		try {
			c.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Ending time: " + new Date().toString());
		if(true) return;
//		Map<BigExtendedCharacteristicSet, long[]> ecsMap = new THashMap<BigExtendedCharacteristicSet, long[]>();
//		
//		start = System.nanoTime();
//		//HashMap<Integer, ArrayList<Long>> hash = new HashMap<Integer, ArrayList<Long>>();
//		Map<Integer, ArrayList<int[]>> hash = new THashMap<>();
//		List<int[]> resList = null ;
//		ArrayList<int[]> def = null;
//		BigExtendedCharacteristicSet ecs = null;
//		
//		Map<Integer, long[]> ecsLongArrayMap = new HashMap<Integer, long[]>();
//		
//		Map<Integer, BigExtendedCharacteristicSet> ruecs = new HashMap<Integer, BigExtendedCharacteristicSet>();
//		
//		Map<BigExtendedCharacteristicSet, Integer> uecs = new HashMap<BigExtendedCharacteristicSet, Integer>();
//		
//		int ecsIndex = 0;
//		HashMap<BigCharacteristicSet, HashSet<BigExtendedCharacteristicSet>> sCSToECS = new HashMap<BigCharacteristicSet, HashSet<BigExtendedCharacteristicSet>>();
//		HashMap<BigCharacteristicSet, HashSet<BigExtendedCharacteristicSet>> oCSToECS = new HashMap<BigCharacteristicSet, HashSet<BigExtendedCharacteristicSet>>();
//		HashSet<BigExtendedCharacteristicSet> d;
//		for(Integer cs1 : csMapFull.keySet()){
//			
//			hash.clear();
//			
//			for(int lon[] : csMapFull.get(cs1)){
//				def = hash.getOrDefault(lon[2], new ArrayList<int[]>());
//				def.add(lon);
//				hash.put(lon[2], def);	
//				
//			}
//			
//			for(Integer cs2 : csMapFull.keySet()){
//				
//				//if(vis.contains(cs2)) continue;
//				resList = join(hash, csMapFull.get(cs2));
//				if(resList.isEmpty()) continue;
//				
//				ecs = new BigExtendedCharacteristicSet(rucs.get(cs1), rucs.get(cs2));
//				d = sCSToECS.getOrDefault(rucs.get(cs1), new HashSet<BigExtendedCharacteristicSet>());
//				d.add(ecs);
//				sCSToECS.put(rucs.get(cs1), d);
//				d = oCSToECS.getOrDefault(rucs.get(cs2), new HashSet<BigExtendedCharacteristicSet>());
//				d.add(ecs);
//				oCSToECS.put(rucs.get(cs2), d);
//				result = resList;
//				Arrays.sort(result);
//				//ecsMap.put(ecs, result);
//				ecsLongArrayMap.put(ecsIndex, result);
//				uecs.put(ecs, ecsIndex);
//				ruecs.put(ecsIndex++, ecs);
//				
//			
//			}
//			
//			resList.clear();
//			for(long lon : csMap.get(cs1)){
//				if(!filter.contains(lon)){
//					resList.add(lon);
//					
//				}
//			}
//			if(!resList.isEmpty()){
//				ecs = new BigExtendedCharacteristicSet(rucs.get(cs1), null);
//				result = resList.stream().mapToLong(k -> k).toArray();
//				//ecsMap.put(ecs, result);
//				Arrays.sort(result);
//				ecsLongArrayMap.put(ecsIndex, result);
//				uecs.put(ecs, ecsIndex);
//				ruecs.put(ecsIndex++, ecs);
//				d = sCSToECS.getOrDefault(rucs.get(cs1), new HashSet<BigExtendedCharacteristicSet>());
//				d.add(ecs);
//				sCSToECS.put(rucs.get(cs1), d);			
//				//tot += result.length;
//			}
//			filter.clear();
//			//vis.add(cs1);
//			//visitedECS.add(cs1);
//		}
//		end = System.nanoTime();
//		System.out.println("ecs new: " + (end-start));
//		
//		System.out.println("ecsMap: " + ecsLongArrayMap.size());
//		System.out.println("orphans: " + tot);
//		System.out.println("filter: " + filter.size());
//		Map<BigExtendedCharacteristicSet, HashSet<BigExtendedCharacteristicSet>> ecsLinks = new HashMap<BigExtendedCharacteristicSet, HashSet<BigExtendedCharacteristicSet>>(); 
//		for(BigCharacteristicSet cs1 : oCSToECS.keySet()){
//	 			if(sCSToECS.containsKey(cs1)){
//	 				for(BigExtendedCharacteristicSet e1 : oCSToECS.get(cs1)){
//	 					for(BigExtendedCharacteristicSet e2 : sCSToECS.get(cs1)){
//	 						d = ecsLinks.getOrDefault(e1, new HashSet<BigExtendedCharacteristicSet>());
//	 						d.add(e2);
//	 						ecsLinks.put(e1, d); 						
//	 					}
//	 				}
//	 				
//	 			}
//	 		}
//		tot = 0;
//		for(BigExtendedCharacteristicSet e : ecsLinks.keySet()){
//			tot += ecsLinks.get(e).size();
//		}
//		System.out.println("total ecs links: " + tot);
//		tot = 0;
//		for(Integer ecs1 : ecsLongArrayMap.keySet()){
//			tot += ecsLongArrayMap.get(ecs1).length;
//		}
//		System.out.println("tot " + tot);
//		//System.out.println("notot " + notot);
//		
//		System.out.println("p " + propIndex);
//		//System.out.println("s " + neg);
//		System.out.println("UCS: " + ucs.size());
//		start = System.nanoTime();
//		Map<Integer, int[]> propIndexMap = new HashMap<Integer, int[]>(); 
//				//new HashMap<Integer, HashMap<Integer,Integer>>();
//		for(Integer e : ecsLongArrayMap.keySet()){
//				//propIndexMap.put(e, new HashMap<Integer, Integer>());					
//				long[] larr = ecsLongArrayMap.get(e);
//				int[] parr = new int[propertiesSet.size()];
// 			for(String property : propertiesSet.keySet()){
// 				int ps = propertiesSet.get(property);
// 				int pstart = indexOfProperty(larr, ps);
// 				if(pstart < 0) {
// 					parr[ps] = -1;
// 					continue;
// 				}
// 				parr[ps] = pstart;
// 				//propIndexMap.get(e).put(ps, pstart);
// 			}
// 			propIndexMap.put(e, parr);
//		}
//		
//		Map<String, Integer> propertiesSetBack = new HashMap<String, Integer>();
//		for(String pr : propertiesSet.keySet()){
//			propertiesSetBack.put(pr, propertiesSet.get(pr));
//		}
//		end = System.nanoTime();
//		System.out.println("prop indexes : " + (end-start));
//		for(Integer ci : csMap.keySet()){
//			result = csMap.get(ci);
//			Arrays.sort(result);
//			csMap.put(ci, result);
//		}
//		
//	}
//	
//	public static THashSet<int[]> filter = new THashSet<int[]>();
//	public static List<int[]> result = new ArrayList<int[]>();
//	public static TIntHashSet visited = new TIntHashSet();
//	private static List<int[]> join(Map<Integer, ArrayList<int[]>> hash, int[][] ls2) {
//		
//		result.clear();
//		visited.clear();
//		int psub = ls2[0][0];
//		int sub ;
//		for(int i = 1; i < ls2.length; i++){
//		
//			sub = ls2[i][0];
//			//if()
//			//if(!visited.contains(sub) && hash.containsKey(sub)){
//			if(sub != psub && hash.containsKey(psub)){
//			
//				result.addAll(hash.get(psub));
//				
//				filter.addAll(hash.get(psub));
//				//visited.add(sub);
//			}
//			psub = sub;
//			
//		}
//		if(hash.containsKey(psub)){
//			
//			result.addAll(hash.get(psub));
//			
//			filter.addAll(hash.get(psub));
//			
//		}
//		
//		return result;
//		
//	}
//	
//	public static int indexOfProperty(long[] a, int key) {
//		 int lo = 0;
//	        int hi = a.length - 1;
//	        int firstOccurrence = Integer.MIN_VALUE;
//	        while (lo <= hi) {
//	        	int mid = lo + (hi - lo) / 2;
//	        	
//	            int s = (int)((a[mid] >> 54)  & 0x3ff);
//	          
//	        	 if (s == key) {
//	                 // key found and we want to search an earlier occurrence
//	                 firstOccurrence = mid;	                 
//	                 hi = mid - 1;	                 
//	             } else if (s < key) {
//	            	 lo = mid + 1;
//	             } else {
//	            	 hi = mid - 1;
//	             }
//	            
//	        }
//	        if (firstOccurrence != Integer.MIN_VALUE) {
//	            return firstOccurrence;
//	        }
//
//	        return -1;
//   }

	}
}
