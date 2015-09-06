import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;


	    public class VOLATILITY extends EvalFunc<Double>
	    {
	        public Double exec(Tuple input) throws IOException 
	        {
	        	if (input == null || input.size() == 0)
	               return null;
	            try
	            {
	        	   DataBag bag = (DataBag)input.get(1);
	        	   HashMap<String,ArrayList<Double>> hm=new HashMap<String,ArrayList<Double>>();
	               Iterator it = bag.iterator();
	               
	               while (it.hasNext())
	               {
	            	   ArrayList<Double> al=new ArrayList<Double>();
	            	   Tuple tuple = (Tuple)it.next();
	                   // Don't count nulls or empty tuples
	                   if (tuple != null && tuple.size() > 0 && tuple.get(0) != null)
	                   {
	                	   String date=(String)tuple.get(1);
	                	   String yearMonth=date.substring(0, 7);
	                	   if(!hm.containsKey(yearMonth))
	                	   {
	                		   String aaj=date.substring(8, date.length());
	                		   double aajdouble=Double.parseDouble(aaj);
	                		   al.add(0,Double.parseDouble(((String)tuple.get(2))));
	                		   al.add(1,aajdouble);
	                		   al.add(2,Double.parseDouble(((String)tuple.get(2))));
	                		   al.add(3,aajdouble);
	                		   hm.put(yearMonth, al);
	                	   }
	                	   else
	                	   {
	                		   al=hm.get(yearMonth);
	                		   String aaj=date.substring(8, date.length());
	                		   double aajdouble=Double.parseDouble(aaj);
	                		   if(aajdouble<al.get(1))
	                		   {
	                			   al.set(1,aajdouble);
	                			   al.set(0,Double.parseDouble(((String)tuple.get(2))));
	                		   }
	                		   if(aajdouble>al.get(3))
	                		   {
	                			   al.set(3,aajdouble);
	                			   al.set(2,Double.parseDouble(((String)tuple.get(2))));  
	                			   
	                		   }
	                	   }
	                	}
	               }
	               if(hm.size()==0 ||hm.size()==1)
	            	   return 0.0;
	               ArrayList<Double> calmonth=new ArrayList<Double>();
	               Iterator hmi=hm.entrySet().iterator();
	               double total=0;
	               double mean=0;
	               while(hmi.hasNext())
	               {
	            	   Map.Entry m=(Map.Entry)hmi.next();
	            	   ArrayList<Double> val=new ArrayList<Double>();
	            	   val=(ArrayList<Double>) m.getValue();
	            	   double sub=val.get(2)-val.get(0);
	            	   sub/=val.get(0);
	            	   calmonth.add(sub);
	               }
	               
	               for(int i=0;i<calmonth.size();i++)
	               {
	            	  total+=calmonth.get(i);
	               }
	               
	               total=total/(double)calmonth.size();
	               
	               for(int i=0;i<calmonth.size();i++)
	               {
	            	   mean+=(calmonth.get(i)-total)*(calmonth.get(i)-total);
	               }
	               mean=mean/(double)(calmonth.size()-1);
	               double volality=Math.sqrt(mean);
	               return volality;
	            }
	            catch(Exception e)
	            {
	               throw WrappedIOException.wrap("Caught exception processing input row ", e);
	            }
	       }
	   }
