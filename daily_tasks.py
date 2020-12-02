import pandas as pd
from cdlx.cnxn_manager import cnxn_manager

class MC:

    def __init__(self, BusinessID, table=r'C:\Python37_64\Lib\site-packages\cleaning\seg_constructor.sql',mc_env='Cleaning Prod'): #be carefule of the path when you call
        self.conn_mgr = cnxn_manager()
        self.mc_conn = self.conn_mgr.openCon(mc_env)
        self.BusinessID = BusinessID
        self.df_info = pd.read_sql(self.conn_mgr.readSQL(table).format(self.BusinessID),self.mc_conn)
        self.BusinessName = self.df_info['BusinessName'].item()
        self.SQLRuleID = self.df_info['SQLRuleID'].item()
        self.RuleType = self.df_info['RuleType'].item()
        self.SQLRule = self.df_info['WhereClause'].item()


    def check_ruleupdate(self, table=r'C:\Python37_64\Lib\site-packages\cleaning\IN_table.sql', batch=20000, countGT20000="countGT20000.txt", newrule=""):
        self.df_table = pd.read_sql(self.conn_mgr.readSQL(table).format(self.BusinessID),self.mc_conn)
        self.count = len(self.df_table)
        self.batch = batch

        self.df_table["MID_single"] = "'" + self.df_table["MID"] + "'"
        self.df_table["MID_double"] = "''" + self.df_table["MID"] + "''"
        self.df_group = self.df_table.groupby(['BusinessID'], as_index=False)[["MID_single","MID_double"]].agg(lambda x: ', '.join(map(str, set(x))))  
        self.MID_single = self.df_group['MID_single'].item() 
        self.MID_double = self.df_group['MID_double'].item() 
        self.newrule = newrule 
        self.proposedrule ="'{} AND BUSINESSID IS NULL'".format(self.newrule.replace("'","''")) 

        if self.RuleType == 'P':
            self.df_data2add = pd.read_sql(self.conn_mgr.readSQL(r'C:\Python37_64\Lib\site-packages\cleaning\data2add_P.sql').format(self.newrule,self.BusinessID),self.mc_conn)
            self.df_data2remove = pd.read_sql(self.conn_mgr.readSQL(r'C:\Python37_64\Lib\site-packages\cleaning\data2remove_P.sql').format(self.BusinessID,self.newrule),self.mc_conn)
        else:
            self.oldrule = self.SQLRule.replace("AND BUSINESSID IS NULL","")
            self.df_data2add = pd.read_sql(self.conn_mgr.readSQL(r'C:\Python37_64\Lib\site-packages\cleaning\data2_R.sql').format(self.newrule,self.oldrule),self.mc_conn)
            self.df_data2remove = pd.read_sql(self.conn_mgr.readSQL(r'C:\Python37_64\Lib\site-packages\cleaning\data2_R.sql').format(self.oldrule,self.newrule),self.mc_conn)

        self.count = max(len(self.df_data2add), len(self.df_data2remove))
        
        self.df_data2add = self.df_data2add["MerchantID"].astype(str)
        self.data2add = self.df_data2add.str.cat(sep=",")
        self.df_data2remove = self.df_data2remove["MerchantID"].astype(str)       
        self.data2remove = self.df_data2remove.str.cat(sep=",")

        if self.count > self.batch:
            with open(countGT20000,"a") as file:
                file.write("BusinessID ({})/SQLRuleID ({}) has {} strings to clean.".format(self.BusinessID,self.SQLRuleID, self.count))
                file.write("\n")
        else:
            if self.data2add:
                if self.data2remove:
                    self.check = "EXEC usp_check_SQLRule @BusinessID = {}, @proposedWhereClause = {}, @data2add= '{}', @data2remove= '{}'".format(self.BusinessID, self.proposedrule, self.data2add, self.data2remove)
                else:
                    self.check = "EXEC usp_check_SQLRule @BusinessID = {}, @proposedWhereClause = {}, @data2add= '{}'".format(self.BusinessID, self.proposedrule, self.data2add)
            else:
                if self.data2remove:
                    self.check = "EXEC usp_check_SQLRule @BusinessID = {}, @proposedWhereClause = {}, @data2remove= '{}'".format(self.BusinessID, self.proposedrule, self.data2remove)
                else:
                    self.check = "EXEC usp_check_SQLRule @BusinessID = {}, @proposedWhereClause = {}".format(self.BusinessID, self.proposedrule)

            self.ruleupdate = "print 'Businessid = {}' EXEC SQLRule_Update  @businessid = {}, @whereclause = {}, @updatetype = 'A',  @ruledescription = 'change rule for DD segs to capture Ascot MIDs'".format(self.BusinessID, self.BusinessID, self.proposedrule)
            self.data = {
                'BusinessID':self.BusinessID,
                'BusinessName':self.BusinessName,
                'SQLRuleID':self.SQLRuleID,
                'RuleType':self.RuleType,
                'check':self.check,
                'ruleupdate':self.ruleupdate
                    }     
            self.df = pd.DataFrame(self.data,index=[0])  
            return self.df 


    def assignbuid(self, table=r'table.sql', batch=20000, countGT20000="countGT20000.txt"): #be carefule of the path when you call, and create the file first  
        self.df_table = pd.read_sql(self.conn_mgr.readSQL(table).format(self.BusinessID),self.mc_conn)
        self.count = len(self.df_table)
        self.batch = batch

        if self.count > self.batch:
            with open(countGT20000,"a") as file:
                file.write("BusinessID ({})/SQLRuleID ({}) has {} strings to clean.".format(self.BusinessID,self.SQLRuleID, self.count))
                file.write("\n")

            self.df = pd.DataFrame()
            for i in range(0, self.count, self.batch):
                self.df_batch = self.df_table[i:i+batch]
                self.df_group = self.df_batch.groupby(['BusinessID'], as_index=False)[['MerchantId']].agg(lambda x: ', '.join(map(str, set(x))))
                self.MerchantID = self.df_group['MerchantId'].item()
                self.AssignBUID = "EXEC  AssignBUID @SQLRuleIDParam = " + str(self.SQLRuleID) + ", @RuleTypeParam = '" + self.RuleType + "', @MerchantIDList = '" + str(self.MerchantID) + "', @Mode = 2, @Operation = 0 "
            
                self.data = {
                    'BusinessID':self.BusinessID,
                    'BusinessName':self.BusinessName,
                    'SQLRuleID':self.SQLRuleID,
                    'RuleType':self.RuleType,
                    'AssignBuid':self.AssignBUID
                        }     
                self.df_data = pd.DataFrame(self.data,index=[0]) 
                self.df = self.df.append(self.df_data)

        else:
            self.df_group = self.df_table.groupby(['BusinessID'], as_index=False)[['MerchantId']].agg(lambda x: ', '.join(map(str, set(x))))
            self.MerchantID = self.df_group['MerchantId'].item()           
            self.AssignBUID = "EXEC  AssignBUID @SQLRuleIDParam = " + str(self.SQLRuleID) + ", @RuleTypeParam = '" + self.RuleType + "', @MerchantIDList = '" + str(self.MerchantID) + "', @Mode = 2, @Operation = 0 "
            
            self.data = {
                'BusinessID':self.BusinessID,
                'BusinessName':self.BusinessName,
                'SQLRuleID':self.SQLRuleID,
                'RuleType':self.RuleType,
                'AssignBuid':self.AssignBUID
                    }     
            self.df = pd.DataFrame(self.data,index=[0]) 

        return self.df