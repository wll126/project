package com.cpinfo.his.web.etlMangager;
import com.cpinfo.his.web.etlMangager.db.DBOperator;
import com.cpinfo.his.web.etlMangager.utils.UUIDGenerator;
import javax.swing.*;
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
import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * �ɼ����ݵķ���
 */
public class TimmerTrans {
	public static final String sigleJGs="340000002148,340000002150,340000002176,485957459,340000002180,48599188-4,340000002184,340000002162,340000002158,340000002182";
	public static JTextArea area;  //��ʾ��
	public static JTextField hourText;//ʱ
	public static JTextField minuteText;//��
	public static JButton jb_input;//����
//  public static JButton jb_test;//�������ݺϷ���
	public  static int hour=03;
	public static int minute=30;
    private PrintWriter log;
    private static boolean upflag=true;    //�ϴ��жϱ�ʶ
	/**
	 * �������ڿ��ӻ�
	 * @return
     */
	public  JTextArea getTextArea(){
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
//      jb_test=new JButton(" ����");
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
						getData();
					}else{
						area.setText("ʱ��ָ�ʽ����ȷ");
					}
				}else{
						area.setText("ʱ���ʽ����ȷ");
				}
			}
		});
		jp_top.add(jLabel);
		jp_top.add(hourText);
		jp_top.add(hourLabel);
		jp_top.add(minuteText);
		jp_top.add(minuteLabel);
		jp_top.add(jb_input);
//      jp_top.add(jb_test) ;
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
	public void dataTrans(){
		area.setText("��ʼ�ϴ�����\n");
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println("��һ����ʼ"+sdf.format(new Date()));
        area.append("\n��ʼ��һ����"+sdf.format(new Date())+"\n");
        upflag=false;//�����ϴ�״̬
        TransData();        //  ��ȡ��������
		System.out.println("personDataBegin"+sdf.format(new Date()));
		area.append("�ڶ�����"+sdf.format(new Date())+"\n");
		personData();
		System.out.println("empiUpdateBegin"+sdf.format(new Date()));
		area.append("���Ӳ���"+sdf.format(new Date())+"\n");
		empiUpdate();
		System.out.println("MVIEWFreshBegin"+sdf.format(new Date()));
		area.append("������ͼ"+sdf.format(new Date())+"\n");
		MVFresh();
		area.append("�ϴ�����");
        upflag=true;//���ϴ���ʶ�û�
	}
    public void TransData(){
	 	DBOperator mdb= null;
		int rownum=200;
		try {
		    mdb = new DBOperator("middledb");
			String sql="select u.tableename,u.tablename,u.tableid from uploadtable u where u.tableename is not null    and u.isshow='Y'  order by u.no ";
		     List<Map> tableList=mdb.find(sql);
            //��ȡ����Ҫ�ɼ��ı���
            /**�������б���һ��ȡ��ṹ*/
            ExecutorService cachedThreadPool  = Executors.newCachedThreadPool();//�����̳߳ػ�ȡ����;
            for(int i=0;i<tableList.size();i++){
             try{
                final  Map tableListMap=tableList.get(i);
                cachedThreadPool.submit(new Runnable() {
                    public void run() {
                        area.append("-----------------------"+tableListMap.get("tablename").toString()+"------------------------\n");
                            checkData(tableListMap, 200);  //ȥִ�����ݼ��
                         }
                    } );
                }  catch(Exception e2){
                   log("�쳣��Ϣ:"+e2.getMessage());
                }

            }
              cachedThreadPool.shutdown();
            boolean loop=true;
            do{
                loop=!cachedThreadPool.awaitTermination(2, TimeUnit.SECONDS);  //�ȴ��߳��������
            }while (loop);
        } catch (Exception e) {
			e.printStackTrace();
			area.append("�쳣��Ϣ:"+e.getMessage());
		}finally{
			mdb.freeCon();
		}
       area.append("��һ����ִ������");

	}
    /**
     *    ������ݸ�ʽ��������ʽ��ķ���
     * @param tableListMap      *      *
     * @param rownum
     * @throws Exception
     */
    private  void checkData(Map tableListMap,int rownum)  {
        DBOperator db=null   ;
        DBOperator mdb=null ;
      try{
          db =new DBOperator() ;
          mdb= new DBOperator("middledb") ;
        String tablename=tableListMap.get("tablename").toString();
        String tableid=tableListMap.get("tableid").toString();
        String tableename=tableListMap.get("tableename").toString();
        String sql="select u.columname,u.columename,u.columlength,u.columtype,u.columisnull,u.dictnekey,u.columispk,u.rowdate from uploadtableinfo u where u.tableid=?";
        List<Map> tableInfoList=mdb.find(sql,new Object[]{tableid});    //��ȡ�ñ���ֶ���Ϣ
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
        addSql=addSql+"uploaddate";
        wenStr=wenStr+"sysdate";
        upStr=upStr+"uploaddate=sysdate";
        if("Pt_Information".equals(tableename)){
            upStr=upStr+",empiid=null";
        }
        sql="select * from "+tableename+" where uploadflag ='0'   and jgdm is not null   and rownum<='"+rownum+"'";        //and jgdm not in('340000002148','340000002150','340000002176','485957459','340000002180','48599188-4','340000002184','340000002162','340000002158','340000002182')
        String insertSql="insert into "+tableename+"("+addSql+") values("+wenStr+")";
        String errSql="insert into uploadErrorRecords(err_recordid,jgdm,yqjgdm,tableid,tablename,uploaddate,dataid,errorid,errorinfo,dodate)" +
                "values(?,?,?,?,?,?,?,?,?,?)";
        String medUpSql="update "+tableename+" set uploadflag=?,uploaddate=sysdate where dataid=?";
        String kematchSql="select e.centervalue,e.centername from dictcompare e where e.jgdm=? and e.centerkey=? and e.hosvalue=?";
        String nekySql="select s.contents from bas_dicts s where s.nekey=? and s.nevalue=?";
        String updateSql="update "+tableename+" set "+upStr+pkStr;
        String seleSql="select count(1) ct1 from "+tableename+pkStr;
        List<Map> list=mdb.find(sql);
     area.append(tablename+"  ����ȡ"+list.size()+"����¼\n");
     log("-------------------------"+tablename+"  ����ȡ"+list.size()+"����¼---------------------------");   //д����־
        while(list.size()>0){
            List<Object[]> errobjs=new ArrayList<Object[]>();
            List<Object[]> updateobjs=new ArrayList<Object[]>();
            Set<Map>  errorMap=new HashSet<Map>();  //��Ŵ������ݵļ���
            /**������ѯ���ļ�¼���������з���*/
            for(int j=0;j<list.size();j++){
                Map tmap=list.get(j);             //��ȡһ����¼
                area.append((j+1)+":"+tablename+"  �����ϴ����ݡ�����"+tmap+"\n");
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
                        Object pkvalue=tmap.get(columename);
                        if(null!=pkvalue)pkvalue=pkvalue.toString().trim();
                        pkobj[pkct]=pkvalue;
                        pkct++;
                    }
                    if(tmap.get(columename)==null||"".equals(tmap.get(columename).toString().trim())){
                        if("Y".equals(columisnull)){
                            if("sfzh".equals(columename)){
                                isfalse="Ϊ��";
                            }else{
                                String uuid=new UUIDGenerator().generate().toString();
                                errobjs.add(new Object[]{uuid,tmap.get("jgdm"),tmap.get("yqjgdm"),tableid,tablename,tmap.get("createdate"),tmap.get("dataid"),"002",columname+"("+columename+")Ϊ��",tmap.get(dodate)});
                                updateobjs.add(new Object[]{uuid,tmap.get("dataid")});
                                break;
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
                                    updateobjs.add(new Object[]{uuid,tmap.get("dataid")});
                                    break;
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
                                    break;
                                }
                            }

                            //��֤�ֵ�ƥ��
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
                                    tlist=mdb.find(kematchSql,new Object[]{jgdm,nekey,dateStr});
                                    if(tlist.size()>0&&tlist.get(0).get("centervalue")!=null){
                                        dateStr=tlist.get(0).get("centervalue").toString();
                                        break;
                                    }else{
                                        List<Map> tlist2=new ArrayList<Map>();
                                        tlist2=mdb.find(nekySql,new Object[]{nekey,dateStr});
                                        if(tlist2.size()>0){
                                            break;
                                        }
                                    }
                                }
                                if(n>=nekeyS.length){
                                    String uuid=new UUIDGenerator().generate().toString();
                                    errobjs.add(new Object[]{uuid,tmap.get("jgdm"),tmap.get("yqjgdm"),tableid,tablename,tmap.get("createdate"),tmap.get("dataid"),"005",columname+"("+columename+")δ���ֵ�ƥ��",tmap.get(dodate)});
                                    updateobjs.add(new Object[]{uuid,tmap.get("dataid")});
                                    break;
                                }

                            }
                            //��֤����
                            if(!"".equals(columlength)&&columlength!=null){
                                int length=dateStr.getBytes().length;
                                if(length> Double.parseDouble(columlength)){
                                    String uuid=new UUIDGenerator().generate().toString();
                                    errobjs.add(new Object[]{uuid,tmap.get("jgdm"),tmap.get("yqjgdm"),tableid,tablename,tmap.get("createdate"),tmap.get("dataid"),"003",columname+"("+columename+")���ݳ��ȹ���",tmap.get(dodate)});
                                    updateobjs.add(new Object[]{uuid,tmap.get("dataid")});
                                    break;
                                }
                            }
                            if("sfzh".equals(columename)){
                                Pattern idNumPattern = Pattern.compile("(\\d{14}[0-9a-zA-Z])|(\\d{17}[0-9a-zA-Z])");
                                Matcher idNumMatcher = idNumPattern.matcher(dateStr);
                                if(!idNumMatcher.matches()){
                                    isfalse="��֤����";
                                }
                            }
                            obj[k]=dateStr;
                            continue;
                        }
                    }
                }
                int errorSize= errobjs.size()   ;
                area.append(tablename+"  ������Ϣ��"+errorSize+"��\n");
                if(k>=tableInfoList.size()){
                    Map map=(Map)db.findOne(seleSql,pkobj);            //�����Ƿ���ڣ�����ִ�и��£�����ִ����������
                    if(!map.get("ct1").toString().equals("0")){
                        int colnum=tableInfoList.size();
                        Object[] upobj=new Object[colnum+pkobj.length];
                        for(int m=0;m<colnum+pkobj.length;m++){
                            if(m<colnum){
                                upobj[m]=obj[m];
                            }else{
                                upobj[m]=pkobj[m-colnum];
                            }
                        }
                        area.append(tablename+"  ����\n");
                        db.excute(updateSql, upobj);
                    }else{
                        area.append(tablename+"  ����\n");
                        db.excute(insertSql,obj);
                    }
                    if("".equals(isfalse)){
                        mdb.excute(medUpSql,new Object[]{"1",tmap.get("dataid")});
                        db.excute(errSql,new Object[]{new UUIDGenerator().generate().toString(),tmap.get("jgdm"),tmap.get("yqjgdm"),tableid,tablename,tmap.get("createdate"),tmap.get("dataid"),"000","�ɹ�",tmap.get(dodate)});
                    }else{
                        String uuid=new UUIDGenerator().generate().toString();
                        mdb.excute(medUpSql,new Object[]{uuid,tmap.get("dataid")});
                        String errid="001";
                        if("Ϊ��".equals(isfalse)){
                            errid="001";
                        }
                        db.excute(errSql,new Object[]{uuid,tmap.get("jgdm"),tmap.get("yqjgdm"),tableid,tablename,tmap.get("createdate"),tmap.get("dataid"),errid,"���֤��(sfzh)"+isfalse,tmap.get(dodate)});
                    }
                }
            }
            Object[][] piErr=new Object[errobjs.size()][9];
            for(int j=0;j<errobjs.size();j++){
                piErr[j]=errobjs.get(j);
                area.append(tablename+"  ������Ϣ��"+errobjs.get(j)[8]+"\n");
            }
            Object[][] piMidUp=new Object[updateobjs.size()][2];
            for(int j=0;j<updateobjs.size();j++){
                piMidUp[j]=updateobjs.get(j);
            }
            db.excuteBatch(errSql, piErr);
            mdb.excuteBatch(medUpSql, piMidUp);
            db.commit();
            mdb.commit();
            area.append(tablename+"  �ϴ��ύ*****************************************\n");
            list=mdb.find(sql);
        }
        }catch(Exception e){
             e.printStackTrace();
          area.append("***************************�쳣��"+e.getMessage()+"********************");
          log("�쳣��"+e.getMessage());
            db.rollback();
            mdb.rollback();
        } finally {
             db.freeCon();
            mdb.freeCon();
        }
    }

	public void empiUpdate(){
		DBOperator db= null;
		try {
			db=new DBOperator();
			String sql="select t.tableename from uploadtableinfo f,uploadtable t where t.tableid=f.tableid and f.columename='sfzh' order by t.no";
			List<Map> tableList=db.find(sql);
			for(int i=0;i<tableList.size();i++){
				String tableename=tableList.get(i).get("tableename").toString();
				String upSql="update "+tableename+" t set t.empiid=(select b.empi from bas_person b where b.idcard=t.sfzh) where t.empiid is null";
				db.excute(upSql);
				db.commit();
			}
		} catch (Exception e) {
			db.rollback();
			e.printStackTrace();
		}finally{
			db.freeCon();
		}
	}
	public void personData(){
		DBOperator db= null;
		int rownum=1000;
		try {
			db=new DBOperator();
			String sql="select h.jgdm,h.kh,h.klx,h.xm,h.sfzh,h.csrq,h.xbdm,h.mzdm,h.gjdm,h.hyzk,h.zydm,h.gzdwdz,h.gzddyb,h.hzdz,h.lxdhlb,h.lxdhhm,h.lxrxm,h.lxryhzgx,h.lxrdh,to_char(h.zcsj,'yyyymmddhh24miss') zcsj,h.zcsj zcsjdate,h.aboxx,h.rhxx,h.whcd from pt_information h where h.empiid is null and h.sfzh is not null and rownum<="+rownum;
			String shSql="select max(to_char(p.registdate,'yyyymmddhh24miss')) regdate from bas_person p where p.idcard=?";
			String upSql="update bas_person b ";
			String inSql="insert into bas_person(empi,patname,sex,idcard,bloodtype,dateofbirth,birthplace,maritalstatus,culturaldegree,national,nationality,profession,registdate,rhbloodtype,registorg)" +
					"values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			String empisql="select to_char(sysdate,'yyyymmdd')||seq_empi.nextval as nextval from dual";
			String phoneSql="insert into bas_phone(phoneid,empi,phonetype,contacter,phonecall,relation,registdate) values(sys_guid(),?,?,?,?,?,?)";
			String dictSql="select s.contents from bas_dicts s where s.nekey=? and s.nevalue=?";
			String empiSql="update pt_information t set t.empiid=(select b.empi from bas_person b where b.idcard=t.sfzh) where t.empiid is null";
			List<Map> list=db.find(sql);
			while(list.size()>0){
				List<Object[]> insertPeople=new ArrayList<Object[]>();
				List<Object[]> updatePeople=new ArrayList<Object[]>();
				List<Object[]> insertPhone=new ArrayList<Object[]>();
				for(int i=0;i<list.size();i++){
					Map people=list.get(i);
					String idcard=people.get("sfzh").toString();
					String zcsj=people.get("zcsj").toString();
					List<Map> tlist=db.find(shSql,new Object[]{idcard});
					if(tlist.size()>0&&tlist.get(0).get("regdate")!=null){
						String regdate=tlist.get(0).get("regdate").toString();
						if(zcsj.compareTo(regdate)>0){//����
							//�ݲ�����
						}else{
							continue;
						}
					}else{//����
						String empi=((Map)db.findOne(empisql)).get("nextval").toString();
						insertPeople.add(new Object[]{empi,people.get("xm"),people.get("xbdm"),people.get("sfzh"),people.get("aboxx"),
								people.get("csrq"),people.get("hzdz"),people.get("hyzk"),people.get("whcd"),people.get("mzdm"),
								people.get("gjdm"),people.get("zydm"),people.get("zcsjdate"),people.get("rhxx"),people.get("jgdm")});
						if(!(people.get("lxdhhm")==null||"".equals(people.get("lxdhhm")))){
							insertPhone.add(new Object[]{empi,"01",people.get("xm"),people.get("lxdhhm"),"����",people.get("zcsjdate")});
						}
						if(!(people.get("lxrdh")==null||"".equals(people.get("lxrdh"))||people.get("lxryhzgx")==null||"".equals(people.get("lxryhzgx")))){
							String lxrgx=((Map)db.findOne(dictSql,new Object[]{"1306",people.get("lxryhzgx")})).get("contents").toString();
							insertPhone.add(new Object[]{empi,people.get("lxryhzgx"),people.get("lxrxm"),people.get("lxrdh"),lxrgx,people.get("zcsjdate")});
						}
					}
				}
				Object[][] piInsPeople=new Object[insertPeople.size()][15];
				for(int j=0;j<insertPeople.size();j++){
					piInsPeople[j]=insertPeople.get(j);
				}
				Object[][] piUpPeople=new Object[updatePeople.size()][16];
				for(int j=0;j<updatePeople.size();j++){
					piUpPeople[j]=updatePeople.get(j);
				}
				Object[][] piInsPhone=new Object[insertPhone.size()][6];
				for(int j=0;j<insertPhone.size();j++){
					piInsPhone[j]=insertPhone.get(j);
				}
				db.excuteBatch(inSql, piInsPeople);
				db.excuteBatch(upSql, piUpPeople);
				db.excuteBatch(phoneSql, piInsPhone);
				db.excute(empiSql);
				db.commit();
				list=db.find(sql);
			}			
		} catch (Exception e) {
			db.rollback();
			e.printStackTrace();
		}finally{
			db.freeCon();
		}
	}
	public void MVFresh(){
		DBOperator db = null;
		try {
			db = new DBOperator();
			String avgSql = "call DBMS_MVIEW.REFRESH('OPT_AVG','C')";
			String uploadSql = "call DBMS_MVIEW.REFRESH('uploadstatics','C')";
			String statSql="call DBMS_MVIEW.REFRESH('HIS_CLINICCHARPERSTAT','C')";
			db.excute(avgSql);
			db.excute(uploadSql);
			db.excute(statSql);
			db.commit();
		} catch (Exception e) {
			e.printStackTrace();
			db.rollback();
		} finally {
			db.freeCon();
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
        int intervalMinute=Integer.parseInt(dbProps.getProperty("interval","60")) ;
		//�õ�ʱ����
		Calendar date = Calendar.getInstance();
		//����ʱ��Ϊ xx-xx-xx 00:00:00
		date.set(date.get(Calendar.YEAR), date.get(Calendar.MONTH), date.get(Calendar.DATE), hour, minute, 0);
         Calendar now=Calendar.getInstance();
        int compare=now.compareTo(date);       //�ж��趨ʱ���Ƿ���
        if(compare==1){
           date.add(Calendar.DATE,1);//��һ��
        }
         //��־
        String logFile = dbProps.getProperty("logfile", "DBConnectionManager.log");
        logFile= date.get(Calendar.YEAR)+""+ (date.get(Calendar.MONTH)+1)+ date.get(Calendar.DATE)+logFile;
        try {
            log = new PrintWriter(new FileWriter(logFile, true), true);
        }
        catch (IOException e) {
            System.err.println("�޷�����־�ļ�: " + logFile);
            log = new PrintWriter(System.err);
        }
		//һ��ĺ�����
		long daySpan = intervalMinute* 60 * 1000;
		area.setText("��ʱ������"+hour+"��"+minute+"ִ��\n");
		//�õ���ʱ��ʵ��
		Timer t = new Timer();
		//ʹ�������ڷ�ʽ���з�������
		t.schedule(new TimerTask() {
			public void run() {
				//run����д��ʱ����Ҫִ�еĴ����
				System.out.println("��ʱ��ִ��..");
				if(upflag){
                    area.append("��ʼִ���ϴ�����\n");
                    dataTrans();
                }else{
                    area.append("�ϴ���������ִ���С�����\n");
                }
			}
		}, date.getTime(), daySpan); //daySpan��һ��ĺ�������Ҳ��ִ�м��
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
			area=new TimmerTrans().getTextArea();
		} catch (Exception e) {
			e.printStackTrace();
//			new TimmerTrans().getData();
		} finally {

		}
	}

	}
	

