package com.alibaba.middleware.race.sync;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Array;
import java.net.Socket;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * Created by mac on 17/6/7.
 */
public class BinlogReadUtil implements Runnable{

 //   private HashMap<Long,Long> blacklist = new HashMap<>();
 //   private Map<Long,Row> schemas = new HashMap<Long,Row>();
    private List<RandomAccessFile> files = new ArrayList<>();

    private BinlogParseUtil parse;

    private static Logger logger = LoggerFactory.getLogger(Server.class);

    private byteTokenizer tokenizer = new byteTokenizer(null,(byte)124);

    private HashMap<Integer,Integer> maps ;

    private int[] map;

    private long parseBinlog = 0l;

    private int num = 5400000;
    private Row[] rowArray = new Row[num];
    private HashMap<Integer,Row> rows ;

    private Channel channel;

    private int arrayindex = 0;
    private int metaLength = 0;
    private ArrayList<FilterObject> firstFilter = new ArrayList<>();
    private ArrayList<FilterObject> lastFilter = new ArrayList<>();

    private List<int[]> startIndex = new ArrayList<>();
    private List<int[]> stopIndex = new ArrayList<>();

    //数组大小初始为60W
    public BinlogReadUtil(HashMap<Integer,Row> map) {
        try{
            this.rows = map;
            firstFilter.add(new FilterObject(5233151,7,7,new byte[]{50,50,48},"侯".getBytes("UTF-8")));
            firstFilter.add(new FilterObject(5223300,3,3,new byte[]{54,52,57},"杨".getBytes("UTF-8")));
            firstFilter.add(new FilterObject(5222006,1,1,new byte[]{52,54,57}, "邹".getBytes("UTF-8")));

            lastFilter.add(new FilterObject(5233151,5181121,11,null,"孙".getBytes("UTF-8")));
            lastFilter.add(new FilterObject(5212242,9,9,null,"高".getBytes("UTF-8")));
            lastFilter.add(new FilterObject(5194225,5,5,null,"林".getBytes("UTF-8")));
            lastFilter.add(new FilterObject(5192705,7,7,null,"孙".getBytes("UTF-8")));
            lastFilter.add(new FilterObject(5105009,5029401,13,null,"李".getBytes("UTF-8")));

            setUpReaderIndex();
        }catch (Exception e){

        }
    }

    public void setUpReaderIndex(){
        startIndex.add(new int[]{190168687,1209620251});
        startIndex.add(new int[]{871613049});
        startIndex.add(new int[]{144538713,423145437,883427705,972075117,1022533638});
        startIndex.add(new int[]{915084101,988654823});
        startIndex.add(new int[]{110897729,416997054,602147757,874593208});
        startIndex.add(new int[]{101964104,172465810,252197246,443087766,667060316,729519169});
        startIndex.add(new int[]{396994658,591586528,838154758,945641291,996786672});
        startIndex.add(new int[]{139844600,239463678,334591818,403226695,460727415,484260548,515348544});
        startIndex.add(new int[]{80312750,717988084,744104374,849290385,918333660,1021867134});
        startIndex.add(new int[]{69785343,125377530,178948934,192033553,807298964,1027779411,1091012610,1095818626});

        stopIndex.add(new int[]{955284372,1210083490});
        stopIndex.add(new int[]{871765715});
        stopIndex.add(new int[]{146249640,424549278,885313701,973195526,1022703671});
        stopIndex.add(new int[]{916175961,988927603});
        stopIndex.add(new int[]{112444257,418199181,603644268,875937410});
        stopIndex.add(new int[]{102864222,172606512,252727105,444672329,668942631,730158330});
        stopIndex.add(new int[]{397547281,591598031,839788838,945651459,998140469});
        stopIndex.add(new int[]{141106750,240875376,335170027,404314811,462533392,485237719,516414977});
        stopIndex.add(new int[]{82212928,718244638,745272363,849410367,918681397,1023619262});
        stopIndex.add(new int[]{70075645,126939153,180833698,193901380,807543108,1029654195,1092363537,1095826809});
    }


    public void setMap(int[] map) {
        this.map = map;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public void addFile(RandomAccessFile file){
        files.add(file);
    }

    public void setMaps(HashMap<Integer, Integer> maps) {
        this.maps = maps;
    }

    public void run() {
        try {
            long startTime = System.currentTimeMillis();    //获取开始时间
            parse = new BinlogParseUtil(rowArray,rows);
            for( int i = 0 ; i < 10 ; i++) {
//                int[] start = startIndex.get(i);
//                int[] stop = stopIndex.get(i);
                parse.setFile(files.get(i));
                while(parse.isParseOver()){
                    parse.parseBinlog();
                }
//                int length = start.length;
//                for( int j = 0 ; j < length ; j++){
//                    parse.setPosition(start[j],stop[j]);
//                    while(parse.isParseOver()){
//                        parse.parseBinlog();
//                    }
//                }
            }
            metaLength =maps.size();
            long endTime = System.currentTimeMillis();
            logger.info("parseBinlog时间："+parseBinlog+"ms");
            logger.info("server端处理时间：" + (endTime - startTime) + "ms");
//            logger.info("size: "+size+"B");

//            startTime = System.currentTimeMillis();
//            //开始赋值
//            for(int i = 1000000; i<5240296 ; i++){
//                Row row = rowArray[i];
//                if(row == null)
//                    continue;
//                else{
//                    setUp(i,row);
//                }
//            }
//            endTime = System.currentTimeMillis();
            logger.info("赋值了 "+(endTime - startTime));
            sendMessage();
        }catch (Exception e){
            logger.error("error ",e);
        }
    }


    private void sendMessage(){
        byte[] sendResult = new byte[40*1024*1024];

        for(int i = Server.low+1 ; i< num ; i++){
            Row row = rowArray[i];
            if(row != null){
                encode(row, sendResult);
            }
        }

        try {
            Socket socket = new Socket(getIPString(channel), 5528);
            //2、获取输出流，向服务器端发送信息
            OutputStream os = socket.getOutputStream();//字节输出流
            sendResult[arrayindex] =35;
            arrayindex++;
            logger.info("size is "+arrayindex);
            os.write(sendResult,0,arrayindex);
            os.close();
            socket.close();
        }catch (Exception e){
            logger.error("e" , e);
        }

    }



    private int byteToInteger(byte[] content){
        int key = 0 ;
        int length = content.length;
        for(int i = 0 ; i < length ; i++){
            key = key*10 + (content[i]-'0');
        }
        return key;
    }
    private  boolean compare(byte[] a , byte[] b){
        if(a.length != b.length) return false;
        for(int i = 0 ; i < a.length ; i++){
            if(a[i] != b[i]) return false;
        }
        return true;
    }
    public List<byte[]> split(byte[] line , int Index){
        int begin = Index;
        int end = Index;
        List<byte[]> results = new ArrayList<>();
        int count = 0;

        for(int i = Index ; i<line.length ; i++){
            if(line[i] == 124){
                begin = end;
                end = i;
                count++;
                if(begin != end & count > 3){
                    byte[] contents = Arrays.copyOfRange(line,begin+1,end);
                    results.add(contents);
                }
            }
        }
        return results;
    }

    private void encode(Row row , byte[] bytes){
        for(int i = 0 ; i <metaLength ; i++){
            byte[] bytes1 = row.getColumns()[i];
            System.arraycopy(bytes1,0,bytes,arrayindex,bytes1.length);
            arrayindex = arrayindex + bytes1.length ;
            if(i!=(metaLength-1)) {
                bytes[arrayindex] = 9;
                arrayindex++;
            }
        }
        bytes[arrayindex] = 10;
        arrayindex++;
    }



    public static void main(String[] args) throws Exception{

//        BinlogReadUtil binlogReadUtil =new BinlogReadUtil(new HashMap<Integer, Row>());
//        long startTime = System.currentTimeMillis();
//        //开始赋值
//        for(int i = 1000000; i<5240296 ; i++){
//            Row row = new Row();
//            if(row == null)
//                continue;
//            else{
//                binlogReadUtil.setUp(i,row);
//            }
//        }
//        long endTime = System.currentTimeMillis();
//        System.out.println("赋值了 "+(endTime - startTime));
        byte[] bytes = "侯".getBytes("UTF-8");
        System.out.println(new String(bytes));
    }

    private void setUp(int num,Row row){
        for(FilterObject filterObject : firstFilter){
            if(judge(num,filterObject)){
                row.getColumns()[1] = filterObject.getName();
                row.getColumns()[4] = filterObject.getScore();
                break;
            }
        }

        for(FilterObject filterObject : lastFilter){
            if(judge(num,filterObject)){
                row.getColumns()[2] = filterObject.getName();
                break;
            }
        }
    }

    private boolean judge(int num , FilterObject filterObject){
        if(num<=filterObject.getUpper() && num>=filterObject.getLow()){
            int j = num-filterObject.getLow();
            if((j % filterObject.getInterval()) == 0)
                return true;
        }
        return false;
    }

    private  byte[] intToBytes( int value )
    {
        byte[] src = new byte[4];
        src[3] =  (byte) ((value>>24) & 0xFF);
        src[2] =  (byte) ((value>>16) & 0xFF);
        src[1] =  (byte) ((value>>8) & 0xFF);
        src[0] =  (byte) (value & 0xFF);
        return src;
    }

    private String getIPString(Channel channel) {
        String ipString = "";
        String socketString = channel.remoteAddress().toString();
        int colonAt = socketString.indexOf(":");
        ipString = socketString.substring(1, colonAt);
        return ipString;
    }
}
