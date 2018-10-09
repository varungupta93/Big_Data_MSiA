scan 'Emp_Table_Gupta', {COLUMNS=>['EmployeeData:name','EmailData:body'], FILTER=>"SingleColumnValueFilter('EmployeeData','name',=,'binary:mCarson')"}

scan 'Emp_Table_Gupta',{COLUMNS=>['EmailData:month','EmailData:body'], FILTER=>"SingleColumnValueFilter('EmailData','month',=,'binary:Feb')"}

scan 'Emp_Table_Gupta',{COLUMNS=>['EmployeeData:name', 'EmailData:month','EmailData:body'], FILTER=>"SingleColumnValueFilter('EmployeeData','name',=,'binary:mKelly') AND SingleColumnValueFilter('EmailData','month',=,'binary:Feb')"}

exit
