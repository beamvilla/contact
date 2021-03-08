import json 
import numpy as np
import pandas as pd
import queue
from threading import Thread
import time
import multiprocessing

contact_list_sum = list()
#json_file_path = your json file path
with open(json_file_path) as json_file:
    data = json.load(json_file)

df = pd.DataFrame(data = data)
df.set_index("Id",inplace=True)

df_ticket = pd.DataFrame(columns=["ticket_id","ticket_trace/contact"])
df_ticket["ticket_id"] = df.index.values[:100]

id = df.index.values
email = df.Email.values
phone =  df.Phone.values
contact =  df.Contacts.values
order_id =  df.OrderId.values

df_email = pd.DataFrame(columns=["Id","Email","Contacts"])
df_email["Id"] = id
df_email["Email"] = email
df_email["Contacts"] = contact
df_email = df_email[(df_email["Email"] != "") & (df_email["Email"] != " ")]
df_email.sort_values(by="Id",ascending=True,inplace=True)
df_email.set_index("Id",inplace=True)

df_phone = pd.DataFrame(columns=["Id","Phone","Contacts"])
df_phone["Id"] = id
df_phone["Phone"] = phone
df_phone["Contacts"] = contact
df_phone = df_phone[(df_phone["Phone"] != "") & (df_phone["Phone"] != " ")]
df_phone.sort_values(by="Id",ascending=True,inplace=True)
df_phone.set_index("Id",inplace=True)

df_order = pd.DataFrame(columns=["Id","OrderId","Contacts"])
df_order["Id"] = id
df_order["OrderId"] = order_id
df_order["Contacts"] = contact
df_order = df_order[(df_order["OrderId"] != "") & (df_order["OrderId"] != " ")]
df_order.sort_values(by="Id",ascending=True,inplace=True)
df_order.set_index("Id",inplace=True)

def filter_contact(id):
  a = list(df_email[df_email["Email"] == df.loc[id,"Email"] ].index.values)
  b = list(df_phone[df_phone["Phone"] == df.loc[id,"Phone"] ].index.values)
  c = list(df_order[df_order["OrderId"] == df.loc[id,"OrderId"] ].index.values)
  a.extend(b)
  a.extend(c)
  a = sorted(list(set(a)))
  #print(a)
  join_id = '-'.join(map(str, a)) 
  contact_cnt = df.loc[a,"Contacts"].sum()
  result_text = join_id +", "+str(contact_cnt)
  df_ticket.loc[id,"ticket_trace/contact"] = result_text
  print("row",id,"\n")
  #return result_text

def filter_thread(id_list):
  contact_list = [filter_contact(id) for id in id_list]
  return contact_list

#Define queue
n_Thread = 5
start_last_interval = int(100/n_Thread) ##500
que_list = list()
for que in range(n_Thread):
  que_list.append(queue.Queue())

#Thread list
threads_list = list()

#manager = multiprocessing.Manager()
jobs = []
for n in range(100):
  p = multiprocessing.Process(target=filter_contact, args=(n,))
  jobs.append(p)
  p.start()

for proc in jobs:
  proc.join()

####Define Thread####
#for n in range(n_Thread):
  #t = Thread(target=lambda q, arg: q.put(filter_thread(arg)), args=(que_list[n], df.index.values[n*start_last_interval:(n+1)*start_last_interval]))
  #t.start()
 # threads_list.append(t)
  #print("Thread",n,"appended!")

###Join thread
#for t in threads_list:
    #t.join()

#for que in que_list:
    #contact_list_sum.extend(que.get())



#print(len(contact_list_sum))
##Create output dataframe
#df_ticket = pd.DataFrame(columns=["ticket_id","ticket_trace/contact"])
#df_ticket["ticket_id"] = df.index.values
#df_ticket["ticket_trace/contact"] = contact_list_sum
df_ticket.to_csv("C:/Users/dell/Desktop/ticket.csv",index=False)