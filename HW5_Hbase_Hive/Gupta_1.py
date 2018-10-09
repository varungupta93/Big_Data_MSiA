import starbase
import os

c = starbase.Connection(host="wolf.iems.northwestern.edu", port= 20550)
directory = "/home/public/course/enron"


table = c.table("Emp_Table_Gupta")
table.create("EmployeeData", "EmailData")



empnames = os.listdir(directory)
i = 0
for empname in empnames:
    mailfiles = os.listdir(os.path.join(directory,empname))
    
    for filename in mailfiles:
        mailfile = os.path.join(directory,empname,filename)
        i = i + 1
        with open(mailfile) as f:
            email = f.read()
            mid = email.split("\n")[0][13:-1]
            sender_address = email[(email.find("\nFrom:") + 7):email.find("\nTo:")]
            mail_date = email.split("\n")[1][11:]
            month = mail_date.split(' ')[1]
            send_to = email[(email.find("To:")+4):email.find("Subject:")].replace("\n","")
            last_metadat_onwards = email[email.find("X-FileName:"):]
            body = last_metadat_onwards[last_metadat_onwards.find('\n'):]
            table.insert(str(i),{'EmployeeData':{'name':empname},'EmailData':{'mail_date':mail_date,'month':month, 'sender_address':sender_address,
                                                              'send_to':send_to,'body':body}})    
