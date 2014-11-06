package util;


public class Main 
{
	public static void main(String[] args_)
	{
		if(args_.length > 1)
		{
			String algorithmName = args_[0];
			String[] argsParsed = new String[args_.length - 1];
					
			System.arraycopy( args_, 1, argsParsed, 0, args_.length - 1 );
			
			switch(algorithmName)
			{
			case "CRACKER" : 
			{
				cracker.CrackerMainJava.main(argsParsed);
				break;
			}
			case "CCF" : 
			{
				ccf.CcfMainJava.main(argsParsed);
				break;
			}
			case "CCMR" : 
			{
				ccmr.CcmrMainJava.main(argsParsed);
				break;
			}
			case "CCMRMEM" : 
			{
				ccmrmem.CcmrMainJava.main(argsParsed);
				break;
			}
			case "PEGASUS" : 
			{
				hashMin.HashMinMainJava.main(argsParsed);
				break;
			}
			case "HASHTOMIN" : 
			{
				hashToMin.HashToMinMainJava.main(argsParsed);
				break;
			}
			case "GRAPHXBUILTIN" : 
			{
				graphXBuiltInCC.GraphXBuiltInCCJava.main(argsParsed);
				break;
			}
			case "DIAMETER" : 
			{
				crackerDiameter.DiameterMainJava.main(argsParsed);
				break;
			}
			default : 
			{
				System.out.println("ERROR: Algorithm name not recognized");
				break;
			}
			}
			
		} else
		{
			System.out.println("ERROR Command input must be: command algorithmName configFile");
		}
	}
}
