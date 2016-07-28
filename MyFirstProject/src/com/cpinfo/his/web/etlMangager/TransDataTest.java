package com.cpinfo.his.web.etlMangager;

import com.cpinfo.his.web.etlMangager.db.DBOperator;
import com.cpinfo.his.web.etlMangager.utils.UUIDGenerator;

import javax.swing.*;
import javax.swing.Timer;
import javax.swing.border.TitledBorder;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * �����ϴ����������ķ���
 */

public class TransDataTest {
    public static final String sigleJGs="340000002148,340000002150,340000002176,485957459,340000002180,48599188-4,340000002184,340000002162,340000002158,340000002182";
    public static JTextArea area;  //��ʾ��
    public static JTextField hourText;//ʱ
    public static JTextField minuteText;//��
    public static JButton jb_input;//����
    public  static int hour=03;
    public static int minute=30;
    private PrintWriter log;
    int dataCount=10;
    public TransDataTest() {
        area=getTextArea();
    }

    /**
     * �������ڿ��ӻ�
     * @return
     */
    public  JTextArea getTextArea(){
        JFrame frame = new JFrame("��ע�⣬����ֻ�ǲ������������������ύ��");
        frame.setSize(800, 600); // ���ô�С
        frame.setAlwaysOnTop(true); // ��������������
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE); // Ĭ�Ϲرղ���
        frame.setIconImage(new ImageIcon("images/icon.jpg").getImage()); // ���ô����ͼ��
        frame.setLocationRelativeTo(null); // ���ô����ʼλ��
        frame.setLayout(new BorderLayout());
        JLabel jPanel=new JLabel();
        jPanel.setBackground(Color.pink);
        JTextArea jTextArea=new JTextArea();
        JScrollPane jScrollPane=new JScrollPane(jTextArea);
        jScrollPane.setBorder(new TitledBorder("�����ϴ���Ϣ"));
        jTextArea.setEditable(false);
        jPanel.setLayout(new BorderLayout(1,3));
        jPanel.add(jScrollPane);
        frame.getContentPane().add(jPanel,BorderLayout.CENTER);
        frame.setVisible(true); // ��ʾ����
        return jTextArea;
    }

    public void dataTrans(int dataCount){
        area.setText("��ʼ�ϴ�����\n");
        area.selectAll();
        area.setCaretPosition(area.getSelectedText().length());
        area.requestFocus();
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        area.append("��ʼ����"+sdf.format(new Date())+"\n");
        TransData(dataCount);        //  ��ȡ��������
        area.append("�ϴ�����");
    }
    public void TransData(int rownum){
        DBOperator db= null;
        DBOperator mdb=null;
//         rownum=20;
        try {
            db = new DBOperator();
            mdb = new DBOperator("middledb");
            String sql="select u.tableename,u.tablename,u.tableid from uploadtable u where u.tableename is not null and u.isshow='Y'  order by u.no ";
            List<Map> tableList=db.find(sql);
            //��ȡ����Ҫ�ɼ��ı���
            /**�������б���һ��ȡ��ṹ*/
            for(int i=0;i<tableList.size();i++){
                Map tableListMap=tableList.get(i);
                checkData(tableListMap, db, mdb, rownum);  //ȥִ�����ݼ��
            }
        } catch (Exception e) {
            e.printStackTrace();
            area.append("�쳣��Ϣ:"+e.getMessage());
        }finally{
            db.freeCon();
            mdb.freeCon();
        }
        area.append("����ִ������");

    }
    /**
     *    ������ݸ�ʽ����
     * @param tableListMap       *
     * @param db
     * @param mdb
     * @param rownum
     * @throws Exception
     */
    private  void checkData(Map tableListMap,DBOperator db,DBOperator mdb,int rownum)  {
        try{
            String tablename=tableListMap.get("tablename").toString();
//        area.append("�ϴ�������"+tablename+"\n");
            String tableid=tableListMap.get("tableid").toString();
            String tableename=tableListMap.get("tableename").toString();
            String sql="select u.columname,u.columename,u.columlength,u.columtype,u.columisnull,u.dictnekey,u.columispk,u.rowdate from uploadtableinfo u where u.tableid=?";
            List<Map> tableInfoList=db.find(sql,new Object[]{tableid});    //��ȡ�ñ���ֶ���Ϣ
            String addSql="";
            String wenStr="";
            String pkStr="";
            String upStr="";
            int pksize=0;
            String dodate=null;
            /**���������ֶΣ���ȡ�ֶ���Ϣ*/
            for(int j=0;j<tableInfoList.size();j++){
                Map colMap=tableInfoList.get(j);
                String columename=((String)colMap.get("columename"));
                String columispk=((String)colMap.get("columispk"));
                String rowdate=((String)colMap.get("rowdate"));
                if("Y".equals(columispk)){
                    if(pkStr.equals("")){
                        pkStr=" where "+columename+"=?";
                    }else{
                        pkStr=pkStr+" and "+columename+"=?";
                    }
                    pksize++;
                }
                if("Y".equals(rowdate)){
                    dodate=columename;
                }
                addSql=addSql+columename+",";
                wenStr=wenStr+"?,";
                upStr=upStr+columename+"=?,";
            }
            upStr=upStr+"uploaddate=sysdate";
            if("Pt_Information".equals(tableename)){
                upStr=upStr+",empiid=null";
            }
            sql="select * from "+tableename+" where uploadflag ='0'   and jgdm is not null   and rownum<='"+rownum+"'";
            String kematchSql="select e.centervalue,e.centername from dictcompare e where e.jgdm=? and e.centerkey=? and e.hosvalue=?";
            String nekySql="select s.contents from bas_dicts s where s.nekey=? and s.nevalue=?";
            List<Map> list=mdb.find(sql);
            area.append(tablename+"  ����ȡ"+list.size()+"����¼\n");
            log("-------------------------"+tablename+"  ����ȡ"+list.size()+"����¼---------------------------");   //д����־
//            while(list.size()>0){
            List<Object[]> errobjs=new ArrayList<Object[]>();
            List<Object[]> updateobjs=new ArrayList<Object[]>();
            Map<Map,List<String>> mainErrorMaps=new HashMap<Map,List<String>>();  //��Ŵ�����Ϣ��ӳ��
            /**������ѯ���ļ�¼���������з���*/
            for(int j=0;j<list.size();j++){
                Map tmap=list.get(j);             //��ȡһ����¼
                area.append((j+1)+":"+tablename+"  ����У�����ݡ�����"+tmap+"\n");
                boolean errorflag=true;
                List<String> errorInfoList =new ArrayList<String>();
                Object[] obj=new Object[tableInfoList.size()];        //�洢insert�������ֵ������
                Object[] pkobj=new Object[pksize];        //�洢����ֵ������
                int k=0;int pkct=0;String isfalse="";
                /**��������ÿһ��*/
                for(k=0;k<tableInfoList.size();k++){
                    Map colMap=tableInfoList.get(k);
                    String columname=(String)colMap.get("columname");
                    if(null!=columname)columname=columname.trim();
                    String columename=(String)colMap.get("columename");
                    if(null!=columename)columename=columename.trim();
                    String columlength=colMap.get("columlength")==null?"":colMap.get("columlength").toString();
                    if(null!=columlength)columlength=columlength.trim();
                    String columtype=(String)colMap.get("columtype");
                    if(null!=columtype)columtype=columtype.trim().toUpperCase();
                    String columisnull=(String)colMap.get("columisnull");
                    if(null!=columisnull)columisnull=columisnull.trim().toUpperCase();
                    String dictnekey=(String)colMap.get("dictnekey");
                    if(null!=dictnekey)dictnekey=dictnekey.trim();
                    String columispk=(String)colMap.get("columispk");
                    if(null!=columispk)columispk=columispk.trim().toUpperCase();
                    if("Y".equals(columispk)){        //�ж��Ƿ�����
                        pkobj[pkct]=tmap.get(columename);
                        pkct++;
                    }
                    if(tmap.get(columename)==null||"".equals(tmap.get(columename).toString().trim())){
                        if("Y".equals(columisnull)){
                            if("sfzh".equals(columename)){
                                isfalse="Ϊ��";
                            }else{
                                String uuid=new UUIDGenerator().generate().toString();
                                errobjs.add(new Object[]{uuid,tmap.get("jgdm"),tmap.get("yqjgdm"),tableid,tablename,tmap.get("createdate"),tmap.get("dataid"),"002",columname+"("+columename+")Ϊ��",tmap.get(dodate)});
                                errorInfoList.add(columname+"("+columename+")Ϊ��") ;
                                errorflag=false;    //�ж��Ƿ����
                                updateobjs.add(new Object[]{uuid,tmap.get("dataid")});
                            }
                        }else{
                            obj[k]=tmap.get(columename);
                            continue;
                        }
                    }else{
                        if("DATE".equals(columtype)){
                            obj[k]=tmap.get(columename);
                            continue;
                        }else{
                            String dateStr=tmap.get(columename).toString().trim();
                            if("INTEGER".equals(columtype)){
                                try{
                                    int data= Integer.parseInt(dateStr);
                                    obj[k]=data;
                                    continue;
                                }catch(Exception e){
                                    String uuid=new UUIDGenerator().generate().toString();
                                    errobjs.add(new Object[]{uuid,tmap.get("jgdm"),tmap.get("yqjgdm"),tableid,tablename,tmap.get("createdate"),tmap.get("dataid"),"004",columname+"("+columename+")������INTEGER����",tmap.get(dodate)});
                                    errorInfoList.add(columname+"("+columename+")������INTEGER����") ;
                                    errorflag=false;    //�ж��Ƿ����
                                    updateobjs.add(new Object[]{uuid,tmap.get("dataid")});

                                }
                            }else if("NUMBER".equals(columtype)){
                                try{
                                    double data= Double.parseDouble(dateStr);
                                    obj[k]=data;
                                    continue;
                                }catch(Exception e){
                                    String uuid=new UUIDGenerator().generate().toString();
                                    errobjs.add(new Object[]{uuid,tmap.get("jgdm"),tmap.get("yqjgdm"),tableid,tablename,tmap.get("createdate"),tmap.get("dataid"),"004",columname+"("+columename+")������DOUBLE����",tmap.get(dodate)});
                                    updateobjs.add(new Object[]{uuid,tmap.get("dataid")});
                                    errorInfoList.add(columname+"("+columename+")������DOUBLE����") ;
                                    errorflag=false;    //�ж��Ƿ����
                                }
                            }

                            if(!"".equals(dictnekey)&&dictnekey!=null){
                                String[] nekeyS=dictnekey.split(",");
                                int n=0;
                                for(n=0;n<nekeyS.length;n++){
                                    String nekey=nekeyS[n];
                                    List<Map> tlist=new ArrayList<Map>();
                                    String jgdm=(String)tmap.get("jgdm");
                                    if(sigleJGs.indexOf(jgdm)==-1){
                                        jgdm="0000";
                                    }
                                    tlist=db.find(kematchSql,new Object[]{jgdm,nekey,dateStr});
                                    if(tlist.size()>0&&tlist.get(0).get("centervalue")!=null){
                                        dateStr=tlist.get(0).get("centervalue").toString();
                                        break;
                                    }else{
                                        List<Map> tlist2=new ArrayList<Map>();
                                        tlist2=db.find(nekySql,new Object[]{nekey,dateStr});
                                        if(tlist2.size()>0){
                                            break;
                                        }
                                    }
                                }
                                if(n>=nekeyS.length){
                                    String uuid=new UUIDGenerator().generate().toString();
                                    errobjs.add(new Object[]{uuid,tmap.get("jgdm"),tmap.get("yqjgdm"),tableid,tablename,tmap.get("createdate"),tmap.get("dataid"),"005",columname+"("+columename+")δ���ֵ�ƥ��",tmap.get(dodate)});
                                    updateobjs.add(new Object[]{uuid,tmap.get("dataid")});
                                    errorInfoList.add(columname+"("+columename+")δ���ֵ�ƥ��") ;
                                    errorflag=false;    //�ж��Ƿ����
                                }

                            }
                            if(!"".equals(columlength)&&columlength!=null){
                                int length=dateStr.getBytes().length;
                                if(length> Double.parseDouble(columlength)){
                                    String uuid=new UUIDGenerator().generate().toString();
                                    errobjs.add(new Object[]{uuid,tmap.get("jgdm"),tmap.get("yqjgdm"),tableid,tablename,tmap.get("createdate"),tmap.get("dataid"),"003",columname+"("+columename+")���ݳ��ȹ���",tmap.get(dodate)});
                                    updateobjs.add(new Object[]{uuid,tmap.get("dataid")});
                                    errorInfoList.add(columname+"("+columename+")���ݳ��ȹ���") ;
                                    errorflag=false;    //�ж��Ƿ����
                                }
                            }
                            if("sfzh".equals(columename)){
                                Pattern idNumPattern = Pattern.compile("(\\d{14}[0-9a-zA-Z])|(\\d{17}[0-9a-zA-Z])");
                                Matcher idNumMatcher = idNumPattern.matcher(dateStr);
                                if(!idNumMatcher.matches()){
                                    isfalse="��֤����";
                                    errorInfoList.add("���֤������֤����");
                                }
                            }
                            obj[k]=dateStr;
                            continue;
                        }
                    }
                }
                if(errorflag){
                    area.append(" ���ͨ�� \n");
                }else{
                    mainErrorMaps.put(tmap,errorInfoList);
                    area.append(tmap+"\n:"+errorInfoList+"\n");
                }
            }
            Set<Map.Entry<Map,List<String>>>  errSet=  mainErrorMaps.entrySet()  ;
            //�������д�����Ϣ
            for(Map.Entry entry:errSet){
                log(entry.getKey()+" ��Ӧ������Ϣ��"+entry.getValue());
            }
        }catch(Exception e){
            e.printStackTrace();
            area.append("***************************�쳣��"+e.getMessage()+"********************");
            log("�쳣��"+e.getMessage());
        } finally {

        }
    }

    /**
     * ��ʱִ�д���
     *
     */
    public  void getData(){
        InputStream is = getClass().getResourceAsStream("/db.properties");
        Properties dbProps = new Properties();
        try {
            dbProps.load(is);
        }catch (Exception e) {
            System.err.println("���ܶ�ȡ�����ļ�. " +
                    "��ȷ��db.properties��CLASSPATHָ����·����");
            return;
        }
        if(hour==0){
            hour= Integer.parseInt(dbProps.getProperty("hour","03"));
        }
        if(minute==0){
            minute= Integer.parseInt(dbProps.getProperty("minutes","30"));
        }
        //�õ�ʱ����
        Calendar date = Calendar.getInstance();
        //����ʱ��Ϊ xx-xx-xx 00:00:00
        date.set(date.get(Calendar.YEAR), date.get(Calendar.MONTH), date.get(Calendar.DATE), hour, minute, 0);
        //��־
        String logFile = dbProps.getProperty("logfile", "DBConnectionManager.log");
        logFile= date.get(Calendar.YEAR)+""+ (date.get(Calendar.MONTH)+1)+ date.get(Calendar.DATE)+logFile;
        try {
            log = new PrintWriter(new FileWriter(logFile, true), true);
             dataCount=  Integer.parseInt(dbProps.getProperty("data_count","1"));
        }
        catch (Exception e) {
            System.err.println("�޷�����־�ļ�: " + logFile);
            log = new PrintWriter(System.err);
            dataCount=10;
        }
        //һ��ĺ�����
        dataTrans(dataCount);
    }

    /**
     * ���ı���Ϣд����־�ļ�
     */
    private void log(String msg) {
        log.println(new Date() + ": " + msg);
    }

    /**
     * ���ı���Ϣ���쳣д����־�ļ�
     */
    private void log(Throwable e, String msg) {
        log.println(new Date() + ": " + msg);
        e.printStackTrace(log);
    }

    public static void main(String[] args) {
        try {
           TransDataTest test=new TransDataTest();
            test.getData();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
    }
}
