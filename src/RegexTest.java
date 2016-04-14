import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexTest{
	public static void main(String[]args){
		String line = args[0];
		String avgPatternStr = "^avg\\(R[1-9][0-9]*\\)$";
		String maxPatternStr  = "^max\\(R[1-9][0-9]*\\)$";
		Pattern avgPattern = Pattern.compile(avgPatternStr);
		Pattern maxPattern = Pattern.compile(maxPatternStr);
		Matcher avgMatcher = avgPattern.matcher(line);
		Matcher maxMatcher = maxPattern.matcher(line);
		if(avgMatcher.lookingAt()){
			System.out.println("avg");
		}else if(maxMatcher.lookingAt()){
			System.out.println("max");
		}
	}
}
