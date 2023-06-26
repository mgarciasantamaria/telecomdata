#!/usr/bin/env python
#_*_ codig: utf8 _*_
import psycopg2, time, datetime, json, os
from modules.constants import *
from modules.functions import *

if __name__ == '__main__':
    if Flag_Status('r'):
        try:
            date_log=str(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"))
            cdndb_connect=psycopg2.connect(data_base_connect_prod)
            cdndb_cur=cdndb_connect.cursor()
            dict_summary={}
            logs_List=Download_Logs(date_log)
            if type(logs_List) == dict:
                dict_summary['Log_Error']=logs_List
                dict_summary_str=json.dumps(dict_summary, sort_keys=True, indent=4)
                print(dict_summary_str)
                print_log('a', dict_summary_str, date_log) #Se registra en el log de eventos el resumen.
                mail_subject='WARNING etltelecomlogs PROD error Download Logs' #Se establece el asunto del correo.
                SendMail(dict_summary_str, mail_subject) #Se envia correo electronico.
            elif type(logs_List) == list:
                if logs_List ==[]:
                    text_print=f"Logs not found"
                    print_log(text_print, date_log)
                    dict_summary['logs_Sum']=text_print
                    cdndb_cur.close() #Se cierra la conexion con el cursor de la base de datos.
                    cdndb_connect.close() #Se cierra la conexion con la base de datos.
                else:
                    dict_summary['logs_Sum']=len(logs_List)
                    for csv_file_path in logs_List:
                        print(csv_file_path)
                        with open(csv_file_path, 'r', encoding="latin1") as csv_data:
                            consulta_copy = f"COPY telecomdata FROM STDIN WITH (FORMAT CSV, HEADER true, DELIMITER ';');"
                            cdndb_cur.copy_expert(consulta_copy, csv_data)
                            dict_summary[csv_file_path]={'Sum_csv_Data' : cdndb_cur.rowcount}
                            cdndb_connect.commit()

                        cdndb_cur.execute("UPDATE telecomdata SET datetime = REPLACE(REPLACE(datetime, 'T', ' '), 'Z', '') WHERE datetime LIKE '%T%';")
                        cdndb_connect.commit()
                        time.sleep(2)
                        cdndb_cur.execute("UPDATE telecomdata SET device = 'other'  WHERE device LIKE 'NO DEFINIDO';")
                        cdndb_connect.commit()
                        time.sleep(2)
                        cdndb_cur.execute("UPDATE telecomdata SET device = 'WEB' WHERE device LIKE 'STATIONARY' or device LIKE 'CLOUD_CLIENT';")
                        cdndb_connect.commit()
                        time.sleep(2)
                        cdndb_cur.execute("SELECT DISTINCT telecomdata.contentid FROM telecomdata LEFT JOIN xmldata ON telecomdata.contentid = xmldata.contentid where xmldata.contentid is NULL;")
                        contentid_list=cdndb_cur.fetchall()
                        if contentid_list != []:
                            xml_nofound, dict_xml_extract = extract_xml_data(contentid_list)
                            dict_summary[csv_file_path].update({'extract_xml_data': dict_xml_extract})
                        else:
                            pass

                        for contentid in xml_nofound:
                            cdndb_cur.execute(f"DELETE FROM telecomdata WHERE contentid LIKE '{contentid}';")
                            cdndb_connect.commit()

                        sql="""INSERT INTO playbacks
                        SELECT 
                        telecomdata.datetime,
                        telecomdata.country,
                        'flow',
                        telecomdata.device,
                        telecomdata.clientid,
                        telecomdata.contentid,
                        xmldata.contenttype,
                        xmldata.channel,
                        xmldata.title,
                        xmldata.serietitle,
                        xmldata.releaseyear,
                        xmldata.season,
                        xmldata.episode,
                        xmldata.genre,
                        xmldata.rating,
                        xmldata.duration,
                        telecomdata.segduration
                        FROM telecomdata
                        LEFT JOIN xmldata ON telecomdata.contentid = xmldata.contentid
                        GROUP BY telecomdata.datetime,
                        telecomdata.country,
                        telecomdata.device,
                        telecomdata.clientid,
                        telecomdata.contentid,
                        xmldata.contenttype,
                        xmldata.channel,
                        xmldata.title,
                        xmldata.serietitle,
                        xmldata.releaseyear,
                        xmldata.season,
                        xmldata.episode,
                        xmldata.genre,
                        xmldata.rating,
                        xmldata.duration,
                        telecomdata.segduration;
                        """
                        cdndb_cur.execute(sql)
                        dict_summary[csv_file_path].update({'sum_Insert_Playbacks': cdndb_cur.rowcount})
                        cdndb_connect.commit()
                        dict_str=json.dumps(dict_summary[csv_file_path], sort_keys=False, indent=4)
                        print(dict_str)
                        time.sleep(2)
                        cdndb_cur.execute('DELETE FROM telecomdata;')
                        cdndb_connect.commit()
                        os.remove(csv_file_path)
                    dict_summary_str=json.dumps(dict_summary, sort_keys=False, indent=4)
                    print(dict_summary_str)
                    print_log(dict_summary_str, date_log)
                    SendMail(dict_summary_str, 'Summary telecom Data Playbacks')
                    cdndb_cur.close()
                    cdndb_connect.close()
        except:
            Flag_Status("w")
            cdndb_cur.close()
            cdndb_connect.close()
            error=sys.exc_info()[2]
            errorinfo=traceback.format_tb(error)[0]
            dict_summary['Error']={
                'Error': str(sys.exc_info()[1]),
                'error_info': errorinfo
            }
            dict_summary_str=json.dumps(dict_summary, sort_keys=False, indent=4)
            print_log(dict_summary_str, date_log)
            mail_subject='FAIL etltelecom_PROD Execution Error'
            SendMail(dict_summary_str, mail_subject)
    else:
        SendMail("etltelecom_PROD application failure not recognized", "FAIL etltelecom_PROD Execution Error")