package com.dp.nebula.wormhole.common.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class StringUtil {
	private static final Logger logger = Logger.getLogger(StringUtil.class);
	
	private static final String VARIABLE_PATTERN = "(\\$)\\{(\\w+)\\}";
	
	private StringUtil(){
	}
	
	public static String replaceEnvironmentVariables(String text) {
		Pattern pattern = Pattern.compile(VARIABLE_PATTERN);
		Matcher matcher = pattern.matcher(text);
		
		while(matcher.find()){
			logger.info("replace " + matcher.group(2) + 
					" with " + System.getenv(matcher.group(2)));
			
			text = StringUtils.replace(text, matcher.group(),
					StringUtils.defaultString(System.getenv(matcher.group(2)), matcher.group()));
		}
		return text;
	}

    public static String replaceVariables(String text,Map<String,String> variableMap) {
        Pattern pattern = Pattern.compile(VARIABLE_PATTERN);
        Matcher matcher = pattern.matcher(text);

        while(matcher.find()){
            logger.info("replace " + matcher.group(2) +
                    " with " + variableMap.get(matcher.group(2).replace("CAL_","")));

            text = org.apache.commons.lang.StringUtils.replace(text, matcher.group(),
                    org.apache.commons.lang.StringUtils.defaultString(variableMap.get(matcher.group(2).replace("CAL_","")), matcher.group()));
        }
        return text;
    }
}
