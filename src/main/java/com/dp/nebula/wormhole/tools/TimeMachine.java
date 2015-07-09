package com.dp.nebula.wormhole.tools;

import org.elasticsearch.common.joda.time.DateTime;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zcfrank1st on 7/3/15.
 */
public class TimeMachine {
    // origin ##{YYYYMMDD}    ##{YYYYMMDD_P?D}
    private static String DATE_PATTERN_COMMON = "(##\\{YYYYMMDD_P)(\\d+)(D\\})";
    private static String DATE_PATTERN_NOW = "(##\\{YYYYMMDD\\})";

    private static String HOUR_PATTERN_COMMON = "(##\\{HH_P)(\\d+)(H\\})";
    private static String HOUR_PATTERN_NOW = "(##\\{HH\\})";


    private static String DATE_HOLDER = "${date}";

    public static void main(String[] args) throws IOException {
        if(args.length == 1) {
            replaceTimePattern(args[0]);
        } else {
            //path, date
            replaceHolder(args[0], args[1]);
        }
    }

    private static void replaceHolder (String pathname, String date) throws IOException {
        File f = new File(pathname);
        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
        StringBuilder buffer = new StringBuilder();
        String line;
        while ((line = in.readLine()) != null){
            buffer.append(line);
            buffer.append("\n");
        }
        String content = buffer.toString();

        String newContent = content.replace(DATE_HOLDER, date);
        replaceOriginFile(pathname, newContent);
    }

    private static void replaceTimePattern(String pathname) throws IOException {
        File f = new File(pathname);
        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
        StringBuilder buffer = new StringBuilder();
        String line;

        Pattern nowDatePattern = Pattern.compile(DATE_PATTERN_NOW);
        Pattern commonDatePattern = Pattern.compile(DATE_PATTERN_COMMON);
        Pattern nowPattern = Pattern.compile(HOUR_PATTERN_NOW);
        Pattern commonHourPattern = Pattern.compile(HOUR_PATTERN_COMMON);

        while ((line = in.readLine()) != null){
            buffer.append(line);
            buffer.append("\n");
        }
        String content = buffer.toString();

        Matcher nowDateMatcher = nowDatePattern.matcher(content);
        Matcher commonDateMatcher = commonDatePattern.matcher(content);
        Matcher nowMatcher = nowPattern.matcher(content);
        Matcher commonHourMatcher = commonHourPattern.matcher(content);

        nowDateMatcher.find();
        String newContent = content.replaceFirst(DATE_PATTERN_NOW, new DateTime().toString("yyyy-MM-dd"));

        while (nowDateMatcher.find()) {
            newContent = newContent.replaceFirst(DATE_PATTERN_NOW, new DateTime().toString("yyyy-MM-dd"));
        }
        while (commonDateMatcher.find()) {
            int preDay = Integer.parseInt(commonDateMatcher.group(2));
            newContent = newContent.replaceFirst(DATE_PATTERN_COMMON ,new DateTime().minusDays(preDay).toString("yyyy-MM-dd"));
        }

        while (nowMatcher.find()) {
            newContent = newContent.replaceFirst(HOUR_PATTERN_NOW, new DateTime().toString("yyyy-MM-dd:::hh").split(":::")[1]);
        }

        while (commonHourMatcher.find()) {
            int preHour = Integer.parseInt(commonHourMatcher.group(2));
            newContent = newContent.replaceFirst(HOUR_PATTERN_COMMON, new DateTime().minusHours(preHour).toString("yyyy-MM-dd:::hh").split(":::")[1]);
        }
        in.close();

        if (!newContent.equals(content))
            replaceOriginFile(pathname, newContent);
    }

    private static void replaceOriginFile(String path, String content) throws FileNotFoundException {
        PrintWriter printWriter = new PrintWriter(path);
        printWriter.write(content.toCharArray());
        printWriter.flush();
        printWriter.close();
    }
}
