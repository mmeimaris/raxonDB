package com.athena.imis.runnables;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jena.graph.Triple;

import com.athena.imis.models.DirectedGraph;
import com.athena.imis.models.NewCS;
import com.athena.imis.models.SQLTranslator;

public class RelationalQuerySimple {

	
	public static Connection c ;
	
	public static void main(String[] args) {
		
		
		 c = null;
	      Statement stmt = null;
	      try {
	         Class.forName("org.postgresql.Driver");
	         c = DriverManager
	        		 .getConnection("jdbc:postgresql://"+args[0]+":5432/"+args[1],
	        		            //"mmeimaris", "dirtymarios");
	        				 //"postgres", "postgres");
	        				 args[2], args[3]);
	            /*.getConnection("jdbc:postgresql://"+args[0]+":5432/testbatch",
	            "mmeimaris", "dirtymarios");*/
	         System.out.println("Opened database successfully");

	         
	      } catch ( Exception e ) {
	         System.err.println( e.getClass().getName()+": "+ e.getMessage() );
	         System.exit(0);
	      }	      
	     
		ResultSet rs;
		long time = 0;
		
		double execTime = 0d, planTime = 0d;
		
		HashMap<String, Integer> propMap = new HashMap<String, Integer>();
		try{
			Statement st = c.createStatement();				
			
			String propertiesSetQuery = " SELECT id, uri FROM propertiesset ;";
			ResultSet rsProps = st.executeQuery(propertiesSetQuery);
			
			while(rsProps.next()){
				propMap.put(rsProps.getString(2), rsProps.getInt(1));
			}
			rsProps.close();
			st.close();
			
			st = c.createStatement();				
			
			
			
			Map<NewCS, Set<Integer>> multiValuedCSProps = new HashMap<NewCS, Set<Integer>>();
			Map<Integer, NewCS> realCSIds = new HashMap<Integer, NewCS>();
			String multiValuedQuery = " SELECT cs, p, properties FROM multi_valued INNER JOIN cs_schema ON cs=id ;";
			ResultSet rsMulti = st.executeQuery(multiValuedQuery);
			while(rsMulti.next()){
				Array a = rsMulti.getArray(3);
				Integer[] arr = (Integer[])a.getArray();
				NewCS arrCS = new NewCS(arr);
				Set<Integer> thisCS = multiValuedCSProps.getOrDefault(arrCS, new HashSet<Integer>());
				
				thisCS.add(rsMulti.getInt(2));
				multiValuedCSProps.put(arrCS, thisCS) ;
				realCSIds.put(rsMulti.getInt(1), arrCS);
			}
			rsMulti.close();
			st.close();
			st = c.createStatement();	
			
			
			//System.out.println("propMap: " + propMap.toString()) ;
			Queries queries = new Queries();
			for(String sparql : queries.geonames_queries){
				
				int res = 0;
				StringBuilder union = new StringBuilder();
				execTime = 0d;
				planTime = 0d;
				SQLTranslator sqlTranslator = new SQLTranslator();
		
				System.out.println("SPARQL QUERY: " + sparql);
				sqlTranslator.setSparql(sparql);
				
				sqlTranslator.setPropertyMap(propMap);
				
				sqlTranslator.parseSPARQL();
				
				Map<NewCS, List<NewCS>> csJoinMap = sqlTranslator.getCsJoinMap();
				
				Set<NewCS> csSet = sqlTranslator.getCsSet();
									
				Map<NewCS, Set<String>> csMatches = new HashMap<NewCS, Set<String>>();
				
				Map<NewCS, Set<String>> csQueryMatches = new HashMap<NewCS, Set<String>>();
				
				Set<NewCS> undangled = new HashSet<NewCS>();
				//get ecs from db for each pair of SO joins
				for(NewCS nextCSS : csJoinMap.keySet()){
					
					if(csJoinMap.get(nextCSS) == null) 
						continue;
					for(NewCS nextCSO : csJoinMap.get(nextCSS)){
						
						undangled.add(nextCSO);
						
						String schema = " SELECT DISTINCT * FROM ecs_schema as e "
								+ "WHERE e.css_properties @> ARRAY" + nextCSS.getAsList().toString() 
								+ " AND e.cso_properties @> ARRAY" + nextCSO.getAsList().toString();							
						st = c.createStatement();
						System.out.println(schema);
						ResultSet rsS = st.executeQuery(schema);
						
						while(rsS.next()){
							Set<String> css_matches = csQueryMatches.getOrDefault(nextCSS, new HashSet<String>());
							css_matches.add(rsS.getString(2));
							//if(csMatches.containsKey(nextCSS))
							csQueryMatches.put(nextCSS, css_matches);
							
							Set<String> cso_matches = csQueryMatches.getOrDefault(nextCSO, new HashSet<String>());
							cso_matches.add(rsS.getString(3));
							csQueryMatches.put(nextCSO, cso_matches);
						}
						rsS.close();
						st.close();						
						for(NewCS nextCS : csQueryMatches.keySet()){
							
							if(!csMatches.containsKey(nextCS)){
								
								csMatches.put(nextCS, csQueryMatches.get(nextCS));
								
							}
							else{								
								Set<String> c = csQueryMatches.get(nextCS);
								
								c.retainAll(csMatches.get(nextCS));
								
								csMatches.put(nextCS, c) ;
							}
							
						}
						csQueryMatches.clear();
					}				
					
				}			
				for(NewCS nextCSS : csSet){
					
					if(csJoinMap.get(nextCSS) != null || undangled.contains(nextCSS))
						continue;
					//System.out.println("Dangling: " + nextCSS.toString());													
					String schema = " SELECT DISTINCT * FROM ecs_schema as e "
							+ "WHERE e.css_properties @> ARRAY" + nextCSS.toString() ;											
					st = c.createStatement();
					System.out.println(schema);
					ResultSet rsS = st.executeQuery(schema);
					
					while(rsS.next()){
						Set<String> css_matches = csMatches.getOrDefault(nextCSS, new HashSet<String>());
						css_matches.add(rsS.getString(2));
						csMatches.put(nextCSS, css_matches);					
					}
					rsS.close();
					st.close();
				}
								
				for(NewCS nextCS : csMatches.keySet()){
					nextCS.setMatches(csMatches.get(nextCS));					
				}
				
				//prepare WHERE clause
				String finalQuery = "";
				String where = " WHERE ";
				
				//sqlTranslator.getObjectMap() ;
				
				List<String> resList = new ArrayList<String>();
				
				Map<NewCS, List<Triple>> csRestrictions = sqlTranslator.getCsRestrictions();
				
				//generate aliases for CSs
				Map<NewCS, String> csAliases = new HashMap<NewCS, String>();
				
				int csAliasIdx = 0; for(NewCS nextCS : csMatches.keySet()){csAliases.put(nextCS, "c"+(csAliasIdx++));}
				
				Map<String, String> csIdAliases = new HashMap<String, String>();
				
				csAliasIdx = 0; 
				for(NewCS nextCS : csMatches.keySet()){
					for(String nextCSId : nextCS.getMatches())
						csIdAliases.put(nextCSId, "c"+(csAliasIdx));
					csAliasIdx++;
				}
				System.out.println("Prefetching matches...");
				for(NewCS nextCS : csMatches.keySet()){
					
					Set<Integer> isCovered = new HashSet<Integer>();
					if(csRestrictions.containsKey(nextCS)){
						
						List<Triple> restrictions = csRestrictions.get(nextCS);
						
						for(Triple nextRes : restrictions){
							
							String restriction = "NULL";
							//we need the CS alias here
							
							if(nextRes.getObject().isURI()){
								restriction = csAliases.get(nextCS) + ".p_" + 
										propMap.get("<" + nextRes.getPredicate().toString()+">") + " = " + 
										sqlTranslator.getObjectMap().get("<" + nextRes.getObject().toString()+">");
							}
							else if(nextRes.getObject().isLiteral()){
								restriction = csAliases.get(nextCS) + ".p_" + 
										propMap.get("<" + nextRes.getPredicate().toString()+">") + " = " + 
										sqlTranslator.getObjectMap().get(nextRes.getObject().toString());
							}
							
							isCovered.add(propMap.get("<" + nextRes.getPredicate().toString()+">"));
							
							if(!where.equals(" WHERE "))
								where += " AND ";
							where += restriction ;
							
						}
					}
					else if(sqlTranslator.getSubjectMap().containsKey(nextCS)){
						String restriction = csAliases.get(nextCS) + ".s = " + 
								sqlTranslator.getSubjectMap().get(nextCS);
						
						if(!where.equals(" WHERE "))
							where += " AND ";
						
						where += restriction ;
					}
					//else {						
												
						
						continue;
					//}
					
				}
				
				System.out.println(where);
				
				//find permutations of cs
				List<List<String>> csAsList = new ArrayList<List<String>>();
				List<NewCS> csMatchesOrdered = new ArrayList<NewCS>(csMatches.keySet());
				
				Map<NewCS, Integer> csListIndexMap = new HashMap<NewCS, Integer>();
				
				int csIndexMap = 0;
				for(NewCS nextCS : csMatchesOrdered){
					csAsList.add(new ArrayList<String>(nextCS.getMatches()));
					csListIndexMap.put(nextCS, csIndexMap++);
				}
				//System.out.println("CS list index map: " + csListIndexMap.toString());
				List<List<String>> perms = cartesianProduct(csAsList);
				
				Map<NewCS, List<Triple>> vars = sqlTranslator.getCsVars();
				//System.out.println("vars " + vars.toString()) ;
				List<String> csProjectionsOrdered = new ArrayList<String>();
				//System.out.println("CS MATCHES ORDERED " + csMatchesOrdered.toString());
				for(NewCS nextCS : csMatchesOrdered){
				
					//System.out.println("NEXT CS: " + nextCS.toString());
					//System.out.println("CS PROJECTIONS : " + csProjectionsOrdered.toString());
					String csProjection = "";
					csProjection += csAliases.get(nextCS) + ".s";
					//System.out.println("csProjection: " + csProjection);
					csProjectionsOrdered.add(csProjection);
					csProjection = "";
//					if(!vars.containsKey(nextCS)) {
//						csProjection += csAliases.get(nextCS) + ".s";
//						csProjectionsOrdered.add(csProjection);
//						continue;
//					}
					if(!vars.containsKey(nextCS)) {continue;}
					List<Triple> nextCSVars = vars.get(nextCS);
					if(!csProjection.equals(""))
						csProjection += ", ";
					//System.out.println(nextCSVars.toString());
					
					for(Triple t : nextCSVars){
						csProjection += csAliases.get(nextCS) + ".p_"+propMap.get("<" + t.getPredicate().toString()+">") + ", ";
					}
					csProjection = csProjection.substring(0, csProjection.length()-2);
					csProjectionsOrdered.add(csProjection);
				}
				//System.out.println(csProjectionsOrdered.toString());
				//are there any joins? 
				String noJoins = ""; boolean joinsExist = false;
				
				for(NewCS nextCSS : csJoinMap.keySet()){
					if(csJoinMap.get(nextCSS) != null){
						joinsExist = true;
						break;
					}
				}
				
				
				//Map<List<Integer>, List<Integer>> permToListMap = new HashMap<List<Integer>, List<Integer>>();
				
				
				//how do we build a query graph representation?
				DirectedGraph<NewCS> queryGraph = sqlTranslator.getQueryGraph();				
								
				if(!joinsExist){
					//one query for each permutation
					System.out.println("# of permutations (no joins):" + perms.size());
					for(List<String> nextPerm : perms){
															
						int idx = 0;
						
						List<Integer> asIntList = new ArrayList<Integer>();
						for(String nextCS : nextPerm){
							asIntList.add(Integer.parseInt(nextCS));
						}
						idx = 0;
						for(Integer nextCS : asIntList){
							
							String varList = csProjectionsOrdered.get(idx++);
							if(!noJoins.equals("")){
								noJoins += " UNION ALL ";			
							}
							if(!where.equals(" WHERE "))
								noJoins += " (SELECT " + varList + " FROM cs_" + nextCS + " AS " + csIdAliases.get(nextCS+"")+ " " + where + ") ";
							else
								noJoins += " (SELECT " + varList + " FROM cs_" + nextCS + " AS " + csIdAliases.get(nextCS+"")+ ") ";
						}
						idx = 0;
						String templateQ = noJoins ;
//						if(!where.equals(" WHERE "))
//							templateQ += where ;	
						//int idx = 0;
						//System.out.println("nextPerm: " + nextPerm.toString()) ;						
						for(String nextIntString : nextPerm) {
							NewCS nextCSToTransform = csMatchesOrdered.get(idx++);
							templateQ = templateQ.replaceAll("cs_ AS "+csAliases.get(nextCSToTransform), "cs_"+nextIntString+" AS "+csAliases.get(nextCSToTransform));
							
							//regexes for multivalued properties
							
							//pattern for literal values
							String patternString1 = "("+csAliases.get(nextCSToTransform)+".p_[0-9]* = [0-9][0-9]*)";							
					        Pattern pattern = Pattern.compile(patternString1);
					        Matcher matcher = pattern.matcher(templateQ);
					        int nextInt = Integer.parseInt(nextIntString);
					        while(matcher.find()) {
					            //System.out.println("found: " + matcher.group(1));
					        	String nextProperty =  matcher.group(1);//matcher.group(1).replaceAll(csAliases.get(nextCSToTransform)+".p_","");
					        	Pattern patternProp = Pattern.compile(".p_([0-9]*)");
						        Matcher matcherProp = patternProp.matcher(nextProperty);
						        int intProp ;
						        if(matcherProp.find()){
						        	intProp = Integer.parseInt(matcherProp.group(1));
						        	if(multiValuedCSProps.containsKey(realCSIds.get(nextInt))
						        			&& multiValuedCSProps.get(realCSIds.get(nextInt)).contains(intProp)){
						        		
						        		//System.out.println("is contained in multivar");
						        		
						        		String restr = matcher.group(1);
						        		//System.out.println("matcher group 1: " + restr);
						        		restr = restr.split(" = ")[1] + " = ANY("+restr.split(" = ")[0]+")";
						        		//restr += "] ";
						        		templateQ = templateQ.replaceAll(matcher.group(1), restr);//matcher.replaceFirst(restr);        		
						        		//System.out.println("replaced: " + templateQ);
						        	}
						        }
					        	//System.out.println(realCSIds.get(nextInt));
					        	
					        	
					        }
					        
					        //pattern for .s equalities
					        String patternString2 = "("+csAliases.get(nextCSToTransform)+".p_[0-9]* = c[0-9]*.s)";							
					        Pattern pattern2 = Pattern.compile(patternString2);
					        Matcher matcher2 = pattern2.matcher(templateQ);
					        nextInt = Integer.parseInt(nextIntString);
					        while(matcher2.find()) {
					            //System.out.println("found: " + matcher.group(1));
					        	String nextProperty =  matcher2.group(1);//matcher.group(1).replaceAll(csAliases.get(nextCSToTransform)+".p_","");
					        	Pattern patternProp = Pattern.compile(".p_([0-9]*)");
						        Matcher matcherProp = patternProp.matcher(nextProperty);
						        int intProp ;
						        if(matcherProp.find()){
						        	intProp = Integer.parseInt(matcherProp.group(1));
						        	if(multiValuedCSProps.containsKey(realCSIds.get(nextInt))
						        			&& multiValuedCSProps.get(realCSIds.get(nextInt)).contains(intProp)){
						        		
						        		String restr = matcher2.group(1);
						        		//System.out.println("matcher group 1: " + templateQ);
						        		String[] split = restr.split(" = ");
						        		restr = split[1] + " = ANY("+split[0]+")";
						        		templateQ = templateQ.replaceAll(matcher2.group(1), restr);//matcher.replaceFirst(restr);   
						        		templateQ = templateQ.replaceAll("AND "+split[0]+" IS NOT NULL", "");
						        		templateQ = templateQ.replaceAll(""+split[0]+" IS NOT NULL", "");
						        		//System.out.println("replaced: " + templateQ);
						        	}
						        }
					        	//System.out.println(realCSIds.get(nextInt));
					        	
					        	
					        }
							
						}
						noJoins = templateQ ;
						
											
					}	
					
					finalQuery = noJoins;// + where ;
					
					//one query is enough
					//System.out.println(finalQuery);
					try{
				    	
				    	Statement st2 = c.createStatement();
				    	String explain = "EXPLAIN ANALYZE " ;
				    	//explain = "" ;
				    	finalQuery = explain + finalQuery;
				    	//System.out.println("\t" + finalQuery);
				    	
				    	//templateQ = templateQ.replaceAll("p_0 = 20", "p_0 = 22");
				    	ResultSet rs2 = st2.executeQuery(finalQuery); //
				    	long start = System.nanoTime();
					    while (rs2.next())
						{				    	
						    //System.out.println(rs2.getString(1));
						    if(rs2.getString(1).contains("Execution time: ")){
						    	String exec = rs2.getString(1).replaceAll("Execution time: ", "").replaceAll("ms", "").trim();
						    	execTime += Double.parseDouble(exec);
						    	
						    }
						    else if(rs2.getString(1).contains("Planning time: ")){
						    	String plan = rs2.getString(1).replaceAll("Planning time: ", "").replaceAll("ms", "").trim();
						    	execTime += Double.parseDouble(plan);
						    	planTime += Double.parseDouble(plan);
						    	
						    }					   
						    
						    res++;
						}
					    rs2.close();
					    //System.out.println(execTime);
					    time += System.nanoTime() - start;
					    System.out.println(res);
//					    System.out.println("time: " + time);
//					    System.out.println("execTime: " + execTime + " ms ");
//					    System.out.println("planTime: " + planTime + " ms ");
				    	
					}
					catch(SQLException e){
						e.printStackTrace();
					}
				}
				else{
					
					Set<NewCS> graphRoots = queryGraph.findRoots() ;
					
					if(graphRoots.isEmpty()) // no roots, must be a cyclic query -- let's iterate through every node!
						graphRoots = queryGraph.getmGraph().keySet();
					
					Stack<NewCS> stack = new Stack<NewCS>();								
					
					Map<NewCS, LinkedHashSet<NewCS>> joinQueues = new HashMap<NewCS, LinkedHashSet<NewCS>>();
					
					for(NewCS root : graphRoots){
						
						stack.push(root);
						
						Set<NewCS> visited = new HashSet<NewCS>();
						
						LinkedHashSet<NewCS> currentQueue = joinQueues.getOrDefault(root, new LinkedHashSet<NewCS>()) ;
						
						while(!stack.isEmpty()){
							
							NewCS currentCS = stack.pop();												
							
							//System.out.println("next popped : " + currentCS.toString());
							
							visited.add(currentCS);
							
							if(queryGraph.isSink(currentCS))
								continue;
							
							currentQueue.add(currentCS);
							
							for(NewCS child : queryGraph.edgesFrom(currentCS)){
								
								//if(!visited.contains(child)){
								if(!visited.contains(child))
									stack.push(child);
									
									if(joinQueues.containsKey(child)){
										currentQueue = joinQueues.get(child) ;
										currentQueue.add(currentCS) ;
										joinQueues.remove(currentCS);
										joinQueues.remove(child);
										joinQueues.put(currentCS, currentQueue);
										joinQueues.put(child, currentQueue);
									}
									else{
										currentQueue.add(child);
										joinQueues.remove(currentCS);
										joinQueues.put(currentCS, currentQueue);
										joinQueues.put(child, currentQueue);
									}
								//}
								
							}
							
							
						}
					}
					//System.out.println("\n");
					//System.out.println(joinQueues.toString());
					Set<LinkedHashSet<NewCS>> uniqueQueues = new HashSet<LinkedHashSet<NewCS>>();
					int qsize = 0;
					for(NewCS nextCS : joinQueues.keySet()){
						uniqueQueues.add(joinQueues.get(nextCS));
						qsize = joinQueues.get(nextCS).size();
					}					
					
					System.out.println("Fetching paths...");
					String paths = "select DISTINCT ";
					for(int i = 1; i < qsize+1; i++){
						paths += " e"+i+".css";
						if(i < qsize){
							paths += ",";
						}
					}
					paths += " from ecs_schema as e1 "; 
					for(int i = 1; i < qsize; i++){
						paths += "inner join ecs_schema as e"+(i+1)+" on e"+i+".cso  = e"+(i+1)+".css ";
						
					}
					System.out.println(paths);
					st = c.createStatement();
					ResultSet rsPaths = st.executeQuery(paths);
					Set<List<Integer>> pathSet = new HashSet<List<Integer>>();
					while(rsPaths.next()){
						List<Integer> pathList = new ArrayList<Integer>();
						for(int i = 1; i < qsize+1; i++){
							pathList.add(rsPaths.getInt(i));
						}				
						pathSet.add(pathList);
					}
					System.out.println("PathSet: " + pathSet.toString());
					rsPaths.close();
					st.close();
					
					String varList = "";
					//System.out.println(" projections " + csProjectionsOrdered.toString()) ;
					for(int ig = 0; ig < csProjectionsOrdered.size(); ig++){
						varList += csProjectionsOrdered.get(ig) + ", ";
					}
					varList = varList.substring(0, varList.length()-2) ;
					Map<NewCS, Set<NewCS>> reverseJoinMap = new HashMap<NewCS, Set<NewCS>>();
					
					//reverse the join map...
					for(NewCS key : csJoinMap.keySet()){
						if(!csJoinMap.containsKey(key) || csJoinMap.get(key) == null) continue;
						for(NewCS nextValue : csJoinMap.get(key)){
							
							Set<NewCS> values = reverseJoinMap.getOrDefault(nextValue, new HashSet<NewCS>());
							values.add(key) ;
							reverseJoinMap.put(nextValue, values) ;				
							
						}
						
					}
					
					
					
					for(LinkedHashSet<NewCS> nextQueue : uniqueQueues){

						List<NewCS> qAsList = new ArrayList<NewCS>(nextQueue) ;
						int jid = 0;
						String nextQS = "";
						
						HashSet<NewCS> visited = new HashSet<NewCS>();
						
						for(int i = 0; i < qAsList.size(); i++){
						
							NewCS nextCS = qAsList.get(i);
							//System.out.println("next CS: " + nextCS + " alias " + csAliases.get(nextCS));
							visited.add(nextCS) ;
							
							//NewCS toJoinCS = qAsList.get(i+1);
							
							if(jid++ == 0){
								nextQS = " SELECT " + varList + " FROM cs_ AS " +csAliases.get(nextCS) + " " ;  
							}
							else {
								if(reverseJoinMap.get(nextCS) != null) {
									//System.out.println("here!") ;
									nextQS += " INNER JOIN cs_ AS " + csAliases.get(nextCS) + " ON " ;
									
									for(NewCS nextReverseJoin : reverseJoinMap.get(nextCS)){
										if(visited.contains(nextReverseJoin)){
											List<NewCS> joinKey = new ArrayList<NewCS>();
											
											joinKey.add(nextReverseJoin);
											
											joinKey.add(nextCS);
											
											//List<Triple> joinProps = sqlTranslator.getCsJoinProperties().get(joinKey);
											
											List<Integer> joinProps = new ArrayList<Integer>();
											//System.out.println("joinKey: " + joinKey.toString());
											for(Triple nextJoinTriple : sqlTranslator.getCsJoinProperties().get(joinKey)){
											
												joinProps.add(propMap.get("<" + nextJoinTriple.getPredicate().toString()+">"));
											
											}
											
											for(int j = 0; j < joinProps.size(); j++){
												
												nextQS += csAliases.get(nextReverseJoin)+".p_"+joinProps.get(j) + " = " + csAliases.get(nextCS)+".s AND ";												
												
												
											}	
											//nextQS = nextQS.substring(0, nextQS.length()-4);
											
										}										
									}
									nextQS = nextQS.substring(0, nextQS.length()-4);
								}
								
								else if(csJoinMap.get(nextCS) != null) {
									//System.out.println("here2!") ;
									//if(!visited.contains(nextCS))
									nextQS += " INNER JOIN cs_ AS " + csAliases.get(nextCS) + " ON " ;
									
									for(NewCS nextReverseJoin : csJoinMap.get(nextCS)){
										if(visited.contains(nextReverseJoin)){
											List<NewCS> joinKey = new ArrayList<NewCS>();
											
											joinKey.add(nextCS);
											
											joinKey.add(nextReverseJoin);
											
											//List<Triple> joinProps = sqlTranslator.getCsJoinProperties().get(joinKey);
											
											List<Integer> joinProps = new ArrayList<Integer>();
											
											for(Triple nextJoinTriple : sqlTranslator.getCsJoinProperties().get(joinKey)){
											
												joinProps.add(propMap.get("<" + nextJoinTriple.getPredicate().toString()+">"));
											
											}
											
											for(int j = 0; j < joinProps.size(); j++){	
												
												nextQS += csAliases.get(nextCS)+".p_"+joinProps.get(j) + " = " + csAliases.get(nextReverseJoin)+".s AND ";												
																								
											}
																																	
										}										
									}
									nextQS = nextQS.substring(0, nextQS.length()-4);
								}
								
							}
						}
						//System.out.println(nextQS);
						//System.out.println("REAL IDS: " + realCSIds.toString());
						Iterator<List<String>> permIt = perms.iterator();
						List<Integer> nextPermInt ;						
						while(permIt.hasNext()){						
							nextPermInt = new ArrayList<Integer>();
							for(String nextPermCS : permIt.next()){
								nextPermInt.add(Integer.parseInt(nextPermCS));
							}							
							//boolean isContained = false;
							//System.out.println("nextPerm " + nextPermInt.toString());
							if(!pathSet.contains(nextPermInt)){
								permIt.remove();
							}
//							for(List<Integer> nextPathList : pathSet){
//								
//								if(nextPathList.containsAll((nextPermInt))){
//									isContained = true;
//									break;
//								}
//							}
							/*if(!isContained){
								permIt.remove();
							}*/
						}
						System.out.println("Number of permutations: " + perms.size());
						for(List<String> nextPerm : perms){
							/*String nextPermInt = "";
							for(String nextPermCS : nextPerm){
								nextPermInt += nextPermCS+"_";
							}
							nextPermInt = nextPermInt.substring(0, nextPermInt.length()-1);
							boolean isContained = false;
							for(String nextPathList : pathSet){
								
								if(nextPathList.contains((nextPermInt))){
									isContained = true;
									break;
								}
							}
							if(!isContained){
//								System.out.println("IS NOT CONTAINED!!!");
//								System.out.println(nextPermInt);
								continue;
							}*/
							String templateQ = nextQS ;
							if(!where.equals(" WHERE "))
								templateQ += where ;	
							int idx = 0;
							//System.out.println("nextPerm: " + nextPerm.toString()) ;						
							for(String nextIntString : nextPerm) {
								NewCS nextCSToTransform = csMatchesOrdered.get(idx++);
								templateQ = templateQ.replaceAll("cs_ AS "+csAliases.get(nextCSToTransform), "cs_"+nextIntString+" AS "+csAliases.get(nextCSToTransform));
								
								//regexes for multivalued properties
								
								//pattern for literal values
								String patternString1 = "("+csAliases.get(nextCSToTransform)+".p_[0-9]* = [0-9][0-9]*)";							
						        Pattern pattern = Pattern.compile(patternString1);
						        Matcher matcher = pattern.matcher(templateQ);
						        int nextInt = Integer.parseInt(nextIntString);
						        while(matcher.find()) {
						            //System.out.println("found: " + matcher.group(1));
						        	String nextProperty =  matcher.group(1);//matcher.group(1).replaceAll(csAliases.get(nextCSToTransform)+".p_","");
						        	Pattern patternProp = Pattern.compile(".p_([0-9]*)");
							        Matcher matcherProp = patternProp.matcher(nextProperty);
							        int intProp ;
							        if(matcherProp.find()){
							        	intProp = Integer.parseInt(matcherProp.group(1));
							        	//System.out.println("found222: " + matcher.group(1));
							        	if(multiValuedCSProps.containsKey(realCSIds.get(nextInt))
							        			&& multiValuedCSProps.get(realCSIds.get(nextInt)).contains(intProp)){
							        		
							        		//System.out.println("is contained in multivar");
							        		
							        		String restr = matcher.group(1);
							        		//System.out.println("matcher group 1: " + restr);
							        		restr = restr.split(" = ")[1] + " = ANY("+restr.split(" = ")[0]+")";
							        		//restr += "] ";
							        		templateQ = templateQ.replaceAll(matcher.group(1), restr);//matcher.replaceFirst(restr);        		
							        		//System.out.println("replaced: " + templateQ);
							        	}
							        }
						        	//System.out.println(realCSIds.get(nextInt));
						        	
						        	
						        }
						        
						        //pattern for .s equalities
						        String patternString2 = "("+csAliases.get(nextCSToTransform)+".p_[0-9]* = c[0-9]*.s)";							
						        Pattern pattern2 = Pattern.compile(patternString2);
						        Matcher matcher2 = pattern2.matcher(templateQ);
						        nextInt = Integer.parseInt(nextIntString);
						        while(matcher2.find()) {
						            //System.out.println("found: " + matcher.group(1));
						        	String nextProperty =  matcher2.group(1);//matcher.group(1).replaceAll(csAliases.get(nextCSToTransform)+".p_","");
						        	Pattern patternProp = Pattern.compile(".p_([0-9]*)");
							        Matcher matcherProp = patternProp.matcher(nextProperty);
							        int intProp ;
							        if(matcherProp.find()){
							        	intProp = Integer.parseInt(matcherProp.group(1));
							        	if(multiValuedCSProps.containsKey(realCSIds.get(nextInt))
							        			&& multiValuedCSProps.get(realCSIds.get(nextInt)).contains(intProp)){
							        		
							        		String restr = matcher2.group(1);
							        		//System.out.println("matcher group 1: " + templateQ);
							        		String[] split = restr.split(" = ");
							        		restr = split[1] + " = ANY("+split[0]+")";
							        		templateQ = templateQ.replaceAll(matcher2.group(1), restr);//matcher.replaceFirst(restr);   
							        		templateQ = templateQ.replaceAll("AND "+split[0]+" IS NOT NULL", "");
							        		templateQ = templateQ.replaceAll(""+split[0]+" IS NOT NULL", "");
							        		//System.out.println("replaced: " + templateQ);
							        	}
							        }
						        	//System.out.println(realCSIds.get(nextInt));
						        	
						        	
						        }
								
							}
							
							//System.out.println("nextQS: " + templateQ );//+ " " + where) ;
							union.append(templateQ).append(" UNION ALL ");
							//if(true) continue ;
							try{
						    	
						    	Statement st2 = c.createStatement();
						    	String explain = "EXPLAIN ANALYZE " ;
						    	//explain = "" ;
						    	if(!where.equals(" WHERE ")){
						    		templateQ = explain + templateQ ;//+ " " + where;	
								}
						    	else{
						    		templateQ = explain + templateQ + " ";
						    	}
						    	//System.out.println("\t" + templateQ);
						    	
						    	//templateQ = templateQ.replaceAll("p_0 = 20", "p_0 = 22");
						    	ResultSet rs2 = st2.executeQuery(templateQ); //
						    	long start = System.nanoTime();
							    while (rs2.next())
								{				    	
								    //System.out.println(rs2.getString(1));
								    if(rs2.getString(1).contains("Execution time: ")){
								    	String exec = rs2.getString(1).replaceAll("Execution time: ", "").replaceAll("ms", "").trim();
								    	execTime += Double.parseDouble(exec);
								    	
								    }
								    else if(rs2.getString(1).contains("Planning time: ")){
								    	String plan = rs2.getString(1).replaceAll("Planning time: ", "").replaceAll("ms", "").trim();
								    	execTime += Double.parseDouble(plan);
								    	planTime += Double.parseDouble(plan);
								    	
								    }					   
								    
								    res++;
								}
							    rs2.close();
							    //System.out.println(execTime);
							    time += System.nanoTime() - start;
							    //System.out.println(res);
//							    System.out.println("time: " + time);
//							    System.out.println("execTime: " + execTime + " ms ");
//							    System.out.println("planTime: " + planTime + " ms ");
						    	
							}
							catch(SQLException e){
								e.printStackTrace();
							}
							//System.out.println() ;
						}
												
					}												
													
				}		
				if(union.length() >= 12)
					union.delete(union.length()-11, union.length());
				System.out.println("execTime: " + execTime + " ms ");
				System.out.println("planTime: " + planTime + " ms ");
				System.out.println(union);
				
				try{
					if(union.length() < 8) continue;
			    	execTime = 0d;
			    	planTime = 0d;
			    			
			    	Statement st2 = c.createStatement();
			    	String explain = "EXPLAIN ANALYZE " ;
			    	String templateQ = "";
			    	//explain = "" ;
			    	if(!where.equals(" WHERE ")){
			    		templateQ = explain + union.toString() ;//+ " " + where;	
					}
			    	else{
			    		templateQ = explain + union.toString() + " ";
			    	}
			    	//System.out.println("\t" + templateQ);
			    	
			    	//templateQ = templateQ.replaceAll("p_0 = 20", "p_0 = 22");
			    	ResultSet rs2 = st2.executeQuery(templateQ); //
			    	long start = System.nanoTime();
				    while (rs2.next())
					{				    	
					    //System.out.println(rs2.getString(1));
					    if(rs2.getString(1).contains("Execution time: ")){
					    	String exec = rs2.getString(1).replaceAll("Execution time: ", "").replaceAll("ms", "").trim();
					    	execTime += Double.parseDouble(exec);
					    	
					    }
					    else if(rs2.getString(1).contains("Planning time: ")){
					    	String plan = rs2.getString(1).replaceAll("Planning time: ", "").replaceAll("ms", "").trim();
					    	execTime += Double.parseDouble(plan);
					    	planTime += Double.parseDouble(plan);
					    	
					    }					   
					    
					    res++;
					}
				    rs2.close();
				    System.out.println("UNION execTime: " + execTime + " ms ");
					System.out.println("UNION planTime: " + planTime + " ms ");
			    	
				}
				catch(SQLException e){
					e.printStackTrace();
				}
				//if(true) return ;
			}
			
			
		}catch (Exception e){
			e.printStackTrace();
			return ;
		}
				
	}
	
	
	/*public static Set<Set<String>> cartesianProduct(Set<String>... sets) {
	    if (sets.length < 2)
	        throw new IllegalArgumentException(
	                "Can't have a product of fewer than two sets (got " +
	                sets.length + ")");

	    return _cartesianProduct(0, sets);
	}
	
	private static Set<Set<String>> _cartesianProduct(int index, Set<String>... sets) {
	    Set<Set<String>> ret = new HashSet<Set<String>>();
	    if (index == sets.length) {
	        ret.add(new HashSet<String>());
	    } else {
	        for (String obj : sets[index]) {
	            for (Set<String> set : _cartesianProduct(index+1, sets)) {
	                set.add(obj);
	                ret.add(set);
	            }
	        }
	    }
	    return ret;
	}*/

	private static HashMap<List<NewCS>, Integer> updateCardinality(List<NewCS> next,
			Map<NewCS, Integer> csSizes, HashMap<List<NewCS>, Integer> pathCosts) {
		int newCardinality = 0;
		
		for(NewCS innerCS : next){
			
			newCardinality += csSizes.get(innerCS);
			
		}
		pathCosts.put(next, newCardinality) ;
		
		return pathCosts;
		
	}


	protected static <T> List<List<T>> cartesianProduct(List<List<T>> lists) {
	    List<List<T>> resultLists = new ArrayList<List<T>>();
	    if (lists.size() == 0) {
	        resultLists.add(new ArrayList<T>());
	        return resultLists;
	    } else {
	        List<T> firstList = lists.get(0);
	        List<List<T>> remainingLists = cartesianProduct(lists.subList(1, lists.size()));
	        for (T condition : firstList) {
	            for (List<T> remainingList : remainingLists) {
	                ArrayList<T> resultList = new ArrayList<T>();
	                resultList.add(condition);
	                resultList.addAll(remainingList);
	                resultLists.add(resultList);
	            }
	        }
	    }
	    return resultLists;
	}
}
