package com.cpinfo.his.web.etlMangager;
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
import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

import javax.swing.*;
import javax.swing.border.TitledBorder;

public class ETLTimerForEHR

{
    private static JTextArea area;  //��ʾ��
    private static JTextField hourText;//ʱ
    private static JTextField minuteText;//��
    private static JButton jb_input;//����
    //  public static JButton jb_test;//�������ݺϷ���
   private  static int hour=03;
   private static int minute=30;
    private PrintWriter log;
	 private String his_db="yhhis";    //his��ETL����������
//	 private DatabaseMeta yunhis  ;//his ���ݿ�
	 private JTextArea jTextArea;//��ʾ��

    public ETLTimerForEHR() {
       area= getTextArea();
        jTextArea=area;
    }

    public ETLTimerForEHR(JTextArea jTextArea) {
        this.jTextArea = jTextArea;
    }
    /**
     * �������ڿ��ӻ�
     * @return
     */
    public   JTextArea getTextArea(){
        JFrame frame = new JFrame("�����ϴ�");
        frame.setSize(800, 600); // ���ô�С
        frame.setAlwaysOnTop(true); // ��������������
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE); // Ĭ�Ϲرղ���
        frame.setIconImage(new ImageIcon("images/icon.jpg").getImage()); // ���ô����ͼ��
        frame.setLocationRelativeTo(null); // ���ô����ʼλ��
        frame.setLayout(new BorderLayout());
        JLabel jPanel=new JLabel();
        jPanel.setBackground(Color.pink);
        JLabel jp_top=new JLabel();
        jp_top.setLayout(new GridLayout(1,7));
        JLabel jLabel=new JLabel("   ",SwingUtilities.LEFT);
        jLabel.setBorder(BorderFactory.createEmptyBorder(0, 0, 0, 20));
        hourText=new JTextField("03");
        JLabel hourLabel=new JLabel("ʱ  ",SwingUtilities.LEFT);
        minuteText=new JTextField("30");
        JLabel minuteLabel=new JLabel("�֣�",SwingUtilities.LEFT);
        jb_input=new JButton("ȷ��");
//        jb_test=new JButton(" ����");
        jb_input.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                String hours=hourText.getText().trim();
                String minutes=minuteText.getText().trim();
                String reg="([01][0-9])|([2][0-3])";
                String minuteReg="[0-5][0-9]";
                if(hours.matches(reg)) {
                    hour = Integer.parseInt(hours);
                    if (minutes.matches(minuteReg)) {
                        minute = Integer.parseInt(minutes);
                        area.setText("��ʱ������"+hour+"��"+minute+"ִ��");
                        Date dateUp=new Date();
                        dateUp.setHours(hour);
                        dateUp.setMinutes(minute);
                        long upTimes=dateUp.getTime();
                        long nowTimes=new Date().getTime();
                        long dtime=(upTimes-nowTimes)/60000l;
                        area.append("���뿪ʼִ��ʱ��"+dtime+"��");
                        getData();
                    }else{
                        area.setText("ʱ��ָ�ʽ����ȷ");
                    }
                }else{
                    area.setText("ʱ���ʽ����ȷ");
                }
            }
        });
//        jb_test.addActionListener(new ActionListener() {
//            @Override
//            public void actionPerformed(ActionEvent e) {
//                TransDataTest test=new TransDataTest();
//                 test.getData();
//            }
//        });
        jp_top.add(jLabel);
        jp_top.add(hourText);
        jp_top.add(hourLabel);
        jp_top.add(minuteText);
        jp_top.add(minuteLabel);
        jp_top.add(jb_input);
//        jp_top.add(jb_test) ;
        jp_top.setSize(600,30);
        JTextArea jTextArea=new JTextArea();
        JScrollPane jScrollPane=new JScrollPane(jTextArea);
        jScrollPane.setBorder(new TitledBorder("�����ϴ���Ϣ"));
        jTextArea.setEditable(false);
        jPanel.setLayout(new BorderLayout(1,3));
        jPanel.add(jp_top);
        jPanel.add(jScrollPane);
//		frame.getContentPane().add(jp_top,BorderLayout.NORTH);
        frame.getContentPane().add(jPanel,BorderLayout.CENTER);
        frame.setVisible(true); // ��ʾ����
        return jTextArea;
    }
    public void start()
	  {
	    String jobName = "hisUpdateJob";
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	    try
	    {
	  	  //��ʼ������
    	      KettleEnvironment.init();
    	    //ѡ����ҵ
    	      //����DB��Դ��
    	      KettleDatabaseRepository repository = new KettleDatabaseRepository();
    	      //�����ݿ��������ƣ���Դ�����ͣ����ӷ�ʽ��IP�����ݿ������˿ڣ��û��������룩
//    	      	      DatabaseMeta databaseMeta = new DatabaseMeta("ehr", "Oracle", "Native", "115.28.172.54", "orcl", "1521", "his_bz", "123456");
    	      DatabaseMeta databaseMeta = new DatabaseMeta("upload", "Oracle", "Native", "127.0.0.1", "orcl", "1521", "hospitals", "123456");
    	      //ѡ����Դ��(ID,���ƣ�����)
    	      KettleDatabaseRepositoryMeta kettleDatabaseMeta = new KettleDatabaseRepositoryMeta("upload", "upload", "upload", databaseMeta);
    	   	  repository.init(kettleDatabaseMeta);
    	      //������Դ�⣨�û��������룩
    	      repository.connect("admin", "admin");
    	      //��Դ��Ŀ¼
    	      RepositoryDirectoryInterface directory = repository.loadRepositoryDirectoryTree();
//    	      JobMeta jobMeta = repository.loadJob(jobName, directory, null, null);
//	         Job job ;
	         List<Map<String,String>> hisDb= getHisDBProperties();
	        if(hisDb!=null&&hisDb.size()>0){
	            for(Map<String,String> hismap:hisDb){
	                //�����ӣ�ִ�������ط���
	                System.out.println("ETL ehryh will be start");
	                //��ȡ����
	                //ִ��Ĭ�ϵĵ���his
	                System.out.println("ETL "+hismap.get("user")+" will be start");
         if(jTextArea!=null)jTextArea.append(sdf.format(new Date())+"ETL "+hismap.get("user")+" will be start\n");
	                String trans="HIS_ClinicCharge,HIS_ClinicRecord,Ipt_Record,Opt_Other,Ipt_Advicedetail,HIS_Recipe,his_cliniclabdetail,Bas_Identifier,Pt_Information,Opt_OtherDetail,HIS_RecipeDetail_new,HIS_ClinicDiag,HIS_ClinicLab,Opt_Register,his_cliniccharperstat_zy,HIS_CLINICCHARPERSTAT,Ipt_Admissionnote,Ipt_MedicalRecordPage_New,Ipt_SignsRecord,Cu_Register,Bas_Location,Cu_Summary,PT_EXAMREPORT,HIS_RecipeDetail,Ipt_Leaverecord,Pt_Information227,cu_summary,Cu_Detail";
	                trans="Pt_Information,Bas_Identifier,HIS_ClinicRecord,HIS_ClinicCharge,HIS_RecipeDetail_new,HIS_Recipe,Opt_OtherDetail,Opt_Other,Opt_Register,HIS_ClinicDiag,his_cliniclabdetail,HIS_ClinicLab,Ipt_Record,Ipt_Advicedetail,HIS_CLINICCHARPERSTAT,his_cliniccharperstat_zy,Ipt_Admissionnote";
	                String [] transNames=trans.split(",");
//                    //�������߳�ģʽ
//                    ExecutorService cachedThreadPool  = Executors.newCachedThreadPool();//�����̳߳ػ�ȡ����;
	                 for(String transName:transNames){
           if(jTextArea!=null)jTextArea.append(sdf.format(new Date())+"ETL "+transName+" will be start\n");
	                	TransMeta transMeta=repository.loadTransformation(transName,directory,null,true,null);
	                	//��ȡ����
	                	List<DatabaseMeta> dmlist =transMeta.getDatabases();
	                	for(DatabaseMeta dm:dmlist){
	                		String connection_name=dm.getName();
	                		if(his_db.equals(connection_name)){	                			
	                			dm.setUsername(hismap.get("user")); //�û�
	                			System.out.println("�û���:"+dm.getUsername());
	                			dm.setPassword(hismap.get("password")); //����
            if(jTextArea!=null)jTextArea.append(sdf.format(new Date())+"�û��� ��"+dm.getUsername()+"\n");
	                		}
	                	}
                        final TransMeta finalTransMeta =transMeta;
//                         cachedThreadPool.submit(new Runnable() {
//                             @Override
//                             public void run() {
                                 Trans transs=new Trans(finalTransMeta);
//                                 try {
                                     transs.execute(null);
//                                 } catch (KettleException e) {
//                                     e.printStackTrace();
//                                 }
                                 transs.waitUntilFinished();
//                             }
//                         }) ;
             if(jTextArea!=null)jTextArea.append(sdf.format(new Date())+"ETL  "+transName+" will be end\n");
	                }
//                    cachedThreadPool.shutdown();
//                    boolean loop=true;
//                    do{
//                        loop=!cachedThreadPool.awaitTermination(2, TimeUnit.SECONDS);  //�ȴ��߳��������
//                    }while (loop);
	                System.out.println("ETL  "+hismap.get("user")+" will be end");
            if(jTextArea!=null)jTextArea.append(sdf.format(new Date())+"ETL  "+hismap.get("user")+" will be end\n");
	            }
	        }
	    }
	    catch (Exception e)
	    {
	      e.printStackTrace();
            if(jTextArea!=null)jTextArea.append(sdf.format(new Date())+"ETL�����쳣 "+e.getMessage()+"\n");
	    }
	  }

	    /**
	     * ��ȡ������ɳ�ʼ��
	     */
	    private List<Map<String,String>> getHisDBProperties() {
	        InputStream is = getClass().getResourceAsStream("/db.properties");
	        Properties dbProps = new Properties();
	        try {
	            dbProps.load(is);
	        }
	        catch (Exception e) {
	            System.err.println("���ܶ�ȡ�����ļ�. " +
	                    "��ȷ��db.properties��CLASSPATHָ����·����");
	            return null;
	        }
	        Enumeration propNames = dbProps.propertyNames();
	        List<Map<String,String>> hisDbLiist=new ArrayList<Map<String,String>>();
	        Set<String> poolSet=new HashSet<String>();
	        //�����������Ʒ���his���ݿ����Ϣ
	        while (propNames.hasMoreElements()) {
	            String name = (String) propNames.nextElement();
	              if(name.startsWith("his")){
	                  String poolName = name.substring(0, name.lastIndexOf("."));
	                  if(poolSet.contains(poolName))continue;
	                  String url = dbProps.getProperty(poolName + ".url");
	                  if (url == null) {
	                      System.out.println("û��Ϊ���ӳ�" + poolName + "ָ��URL");
	                      continue;
	                  }
	                  Map<String,String> hisMap=new HashMap<String, String>();
	                  String user = dbProps.getProperty(poolName + ".user");
	                  String password = dbProps.getProperty(poolName + ".password");
	                //  String maxconn = dbProps.getProperty(poolName + ".maxconn", "0");
	                  hisMap.put("url",url);
	                  hisMap.put("user",user);
	                  hisMap.put("password",password);
	                  hisDbLiist.add(hisMap);
	                  poolSet.add(poolName);
	              }
	        }

	       return hisDbLiist;
	    }

    /**
     * ��ʱִ�д���
     *
     */
    public  void getData(){
        if(hour==0){
            hour= 03;
        }
        if(minute==0){
            minute= 30;
        }
        //�õ�ʱ����
        Calendar date = Calendar.getInstance();
        //����ʱ��Ϊ xx-xx-xx 00:00:00
        date.set(date.get(Calendar.YEAR), date.get(Calendar.MONTH), date.get(Calendar.DATE), hour, minute, 0);
        //һ��ĺ�����
        long daySpan = 24*60* 60 * 1000;
        area.setText("��ʱ������"+hour+"��"+minute+"ִ��");
        //�õ���ʱ��ʵ��
        Timer t = new Timer();
        //ʹ�������ڷ�ʽ���з�������
        t.schedule(new TimerTask() {
            public void run() {
                //run����д��ʱ����Ҫִ�еĴ����
                System.out.println("��ʱ��ִ��..");
                    area.append("��ʼִ���ϴ�����");
                    start();
            }
        }, date.getTime(), daySpan); //daySpan��һ��ĺ�������Ҳ��ִ�м��
    }
    public static void main(String[] args) {
        new ETLTimerForEHR();

    }


}