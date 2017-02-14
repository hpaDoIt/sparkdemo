package com.hpa.spark.cep.common.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by hpa on 2017/2/8.
 */
public class ParamsUtil {
    private static final char PARAM_START_FLAG = '-'; //参数开始标记
    private static  char PARAM_SPLIT_FLAG [] = new char[]{'\t' ,' '};//参数间隔标记
    private static  char PARAM_QUOTE_FLAG  = '"';//引号

    private char paramsCharArray [];
    private String mainParam = null;
    private String startMainParam = null;
    private String endMainParam = null;

    private ParamPairParseIm paramPairParse;
    private ParamOnlyParseIm paramOnlyParse;


    private Map<String, String> paramsMap = new HashMap<String, String>();
    private String info;

    public ParamsUtil(String[] inputs) {
        this(inputs,null);
    }

    public ParamsUtil(String[] inputs, String info){
        this(arrayToString(inputs, " "), info);
    }

    public ParamsUtil(String input, String info){
        this.info = info;
        if(input != null && input.trim().length() > 0){
            this.paramsCharArray = input.toCharArray();
            this.init();
            this.parse();

            if(this.hasParam("h") && info != null){

                showHelp();
                System.exit(0);
                //throw new Exception();
            }
        }
    }

    public void showHelp(){

        if(info != null){

            System.out.println("[使用帮助]");
            System.out.println(info);
        }
    }

    /**
     * 用split字符串将inputs的元素拼接成一个字符串
     * @param inputs
     * @param split
     * @return
     */
    public static String arrayToString(String[] inputs, String split){
        if(inputs != null && split != null){
            StringBuilder result = new StringBuilder();

            for(String c : inputs){
                result.append(c).append(split);
            }
            if(result.length() > split.length()){
                return result.substring(0, result.length() - split.length());
            }
        }
        return null;
    }

    private void init(){

        this.paramPairParse = new ParamPairParse();
        this.paramOnlyParse = new ParamOnlyParse();
        Arrays.sort(PARAM_SPLIT_FLAG);

    }
    /**
     * 字符是参数间字符吗
     * @return
     */
    public static boolean isParamSplit(char c){
        return Arrays.binarySearch(PARAM_SPLIT_FLAG, c) >= 0;
    }


    /**
     * 字符是参数的开始吗
     * @return
     */
    public static boolean isParamStart(char c){
        return c == PARAM_START_FLAG;
    }

    /**
     * 字符是引号吗
     * @return
     */
    public static boolean isQuote(char c){
        return PARAM_QUOTE_FLAG == c;
    }

    private void parse(){
        if(paramsCharArray != null){
            int len = paramsCharArray.length - 1;
            int mark = 0;
            char c;
            while(true){
                c = paramsCharArray[mark];
                //System.out.println(mark);
                if(isParamStart(c)){
                    //System.out.println("pair");
                    mark = this.paramPairParse.parse(paramsCharArray, mark + 1, paramsMap);
                }else if (isParamSplit(c)) {
                    //System.out.println("normal");
                }else {
                    //System.out.println("param");
                    //是单个参数
                    StringBuilder param = new StringBuilder();

                    mark = this.paramOnlyParse.parse(paramsCharArray, mark, param);

                    if(this.startMainParam == null && paramsMap.size() == 0){
                        this.startMainParam = param.toString().trim();
                    }else {
                        this.endMainParam = param.toString().trim();
                    }
                }
                mark++;
                if(mark > len){
                    break;
                }
            }

//			System.out.println(paramsCharArray);
//			System.out.println(paramsMap);
//			System.out.println("["+startMainParam + "]");
//			System.out.println("["+endMainParam + "]");
        }
    }

    /**
     * 是否有指定参数
     * @param key
     * @return
     */
    public boolean hasParam(String key){
        return paramsMap.containsKey(key);
    }

    public Map<String, String> getParamsMap(){
        return paramsMap;
    }
}

interface ParamPairParseIm{

    public int parse(char[] paramsCharArray , int start , Map<String, String> collector);
}

interface ParamOnlyParseIm{

    public int parse(char[] paramsCharArray , int start , StringBuilder collector);
}

/**
 * 解析键值对的参数
 * @author fengbingjian
 *
 */
class ParamPairParse implements ParamPairParseIm{
    public int parse(char[] paramsCharArray, int start,
                     Map<String, String> collector) {
        StringBuilder key = new StringBuilder();
        StringBuilder value = new StringBuilder();
        char last = '"';
        boolean keyFlag = true;
        int mark = start;
        char cur;
        boolean breakFlag = false;
        boolean quoteValue = false;
        boolean quoteEnd = true;
        int len = paramsCharArray.length - 1;
        while (true) {
            cur = paramsCharArray[mark];
            if(!quoteValue && ParamsUtil.isParamStart(cur) &&ParamsUtil.isParamSplit(last)){
                //回退一个位置，并退出
                mark--;
                breakFlag = true;
            }else if(ParamsUtil.isParamSplit(cur)){
                //碰到了空格或\t,如果在""间，需要特殊处理
                if(quoteValue){
                    //是在""中
                    if(keyFlag){
                        key.append(cur);
                    }else {
                        value.append(cur);
                    }
                }else {
                    //不是""中时，如果正在读取value部分，说明碰到了结尾
                    if(keyFlag || value.length() == 0){
                        keyFlag = false;
                    }else {
                        breakFlag = true;
                    }
                }
            }else {
                if(ParamsUtil.isQuote(cur)){
                    if(!keyFlag){
                        quoteValue = true;
                        if(!quoteEnd){
                            quoteEnd = true;
                            breakFlag = true;
                        }else {
                            quoteEnd = false;
                        }
                    }
                }else {
                    //普通字符
                    if(keyFlag){
                        //在算key
                        key.append(cur);
                    }else {
                        value.append(cur);
                    }
                }
            }
            if(breakFlag || mark >= len ){
                if(key.length() > 0){
                    if(!quoteEnd){
                        System.out.println("警告:" + key.toString() + "\"配对不正确");
                    }
                    collector.put(key.toString().trim(), value.toString().trim());
                }
                //System.out.println(key + "," + value);
                return mark;
            }else {
                mark++;
                last = cur;
            }
        }
    }
}


/**
 * 解析单个参数
 * @author fengbingjian
 *
 */
class ParamOnlyParse implements ParamOnlyParseIm{

    public int parse(char[] paramsCharArray, int start,
                     StringBuilder collector) {
        char last = '"';
        boolean keyFlag = false;
        int mark = start;
        char cur;
        boolean breakFlag = false;
        boolean quoteValue = false;
        boolean quoteEnd = true;
        int len = paramsCharArray.length - 1;

        while (true) {
            cur = paramsCharArray[mark];
            if(!quoteValue && ParamsUtil.isParamStart(cur) &&ParamsUtil.isParamSplit(last)){
                //回退一个位置，并退出
                mark--;
                breakFlag = true;
            }else if(ParamsUtil.isParamSplit(cur)){
                //碰到了空格或\t,如果在""间，需要特殊处理
                if(quoteValue){
                    //是在""中
                    if(keyFlag){
                        collector.append(cur);
                    }else {
                        collector.append(cur);
                    }
                }else {
                    //不是""中时，如果正在读取value部分，说明碰到了结尾
                    if(keyFlag || collector.length() == 0){
                        keyFlag = false;
                    }else {
                        breakFlag = true;
                    }
                }
            }else {
                if(ParamsUtil.isQuote(cur)){
                    if(!keyFlag){
                        quoteValue = true;
                        if(!quoteEnd){
                            quoteEnd = true;
                            breakFlag = true;
                        }else {
                            quoteEnd = false;
                        }
                    }
                }else {
                    //普通字符
                    if(keyFlag){
                        //在算key
                        collector.append(cur);
                    }else {
                        collector.append(cur);
                    }
                }
            }
            if(breakFlag || mark >= len ){
                //System.out.println(key + "," + value);
                return mark;
            }else {
                mark++;
                last = cur;
            }
        }
    }
}