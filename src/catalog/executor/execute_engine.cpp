#include "executor/execute_engine.h"
#include "glog/logging.h"
#include <vector>
#include <algorithm>
ExecuteEngine::ExecuteEngine() {

}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context);
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context);
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context);
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context);
    case kNodeShowTables:
      return ExecuteShowTables(ast, context);
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context);
    case kNodeDropTable:
      return ExecuteDropTable(ast, context);
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context);
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context);
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context);
    case kNodeSelect:
      return ExecuteSelect(ast, context);
    case kNodeInsert:
      return ExecuteInsert(ast, context);
    case kNodeDelete:
      return ExecuteDelete(ast, context);
    case kNodeUpdate:
      return ExecuteUpdate(ast, context);
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context);
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context);
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context);
    case kNodeExecFile:
      return ExecuteExecfile(ast, context);
    case kNodeQuit:
      return ExecuteQuit(ast, context);
    default:
      break;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  DBStorageEngine* db = new DBStorageEngine(ast->child_->val_);
  //ï¿½ï¿½ï¿½ï¿½mapÓ³ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½
  dbs_[ast->child_->val_]=db;
  //cout<<"ExecuteCreateDatabase SUCCESS!"<<endl;
  return DB_SUCCESS;
  //return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  delete dbs_[ast->child_->val_];
  dbs_.erase(ast->child_->val_);
  return DB_SUCCESS;
  //return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  cout<<"------DataBases------"<<endl;
  for (auto p=dbs_.begin();p!=dbs_.end();p++){
    std::cout<<p->first<<std::endl;
  }
  //cout<<"ExecuteShowDatabases SUCCESS!"<<endl;
  return DB_SUCCESS;
  //return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  
  if(dbs_.count(ast->child_->val_)){
    current_db_=ast->child_->val_;
    current_db=dbs_[current_db_];
  }else{
    cout<<"Invalid DataBase Name!"<<endl;
  }
  return DB_SUCCESS;
  //return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  cout<<"------Tables------"<<endl;
  vector<TableInfo* > tables;
  current_db->catalog_mgr_->GetTables(tables);
  for(auto p=tables.begin();p<tables.end();p++){
    cout<<(*p)->GetTableName()<<endl;
  }
  return DB_SUCCESS;
  //return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  string table_name = ast->child_->val_;
  //cout<<"table_name:"<<table_name<<endl;
  pSyntaxNode column_pointer= ast->child_->next_->child_;//µÚÒ»¸öÊôÐÔ¶ÔÓ¦µÄÖ¸Õë
  vector<Column*>vec_col;
  while(column_pointer!=nullptr&&column_pointer->type_==kNodeColumnDefinition){
    //ËüµÄº¢×Ó ÊÇ±íµÄÊôÐÔÐÅÏ¢
    //cout<<"---------------"<<endl;
    bool is_unique = false;
    if (column_pointer->val_!=nullptr){
      string s = column_pointer->val_;
      is_unique = (s=="unique");
    }
    string column_name = column_pointer->child_->val_;//ÊôÐÔÃû×Ö
    //cout<<"column_name:"<<column_name<<endl;
    string column_type = column_pointer->child_->next_->val_;//ÊôÐÔÀàÐÍ
    //cout<<"column_type:"<<column_type<<endl;
    int cnt = 0;
    Column *now;
    if(column_type=="int"){
      now = new Column(column_name,kTypeInt,cnt,true,is_unique);
    }
    else if(column_type=="char"){
      string len = column_pointer->child_->next_->child_->val_;
      //cout<<"len:"<<len<<endl;
      long unsigned int a = -1;
      if (len.find('.')!=a){
        cout<<"Semantic Error, String Length Can't be a Decimal!"<<endl;
        return DB_FAILED;
      }
      int length = atoi(column_pointer->child_->next_->child_->val_);
      if (length<0){
        cout<<"Semantic Error, String Length Can't be Negative!"<<endl;
        return DB_FAILED;
      }
      now = new Column(column_name,kTypeChar,length,cnt,true,is_unique);
    }
    else if(column_type=="float"){
      now = new Column(column_name,kTypeFloat,cnt,true,is_unique);
    }
    else{
      cout<<"Error Column Type!"<<endl;
      return DB_FAILED;
    }
    //cout<<"is_unique:"<<is_unique<<endl;
    vec_col.push_back(now);
    column_pointer = column_pointer->next_;
    cnt++;
  }
  //Îªprimary key½¨Á¢Ë÷Òý
  Schema *schema = new Schema(vec_col);
  TableInfo *table_info = nullptr;
  //cout<<"SUCCEED!"<<endl;
  dberr_t IsCreate=current_db->catalog_mgr_->CreateTable(table_name,schema,nullptr,table_info);
  if(IsCreate==DB_TABLE_ALREADY_EXIST){
    cout<<"Table Already Exist!"<<endl;
    return IsCreate;
  }
  if (column_pointer!=nullptr){
    cout<<"It has primary key!"<<endl;
    pSyntaxNode key_pointer = column_pointer->child_;
    vector <string>primary_keys;
    while(key_pointer!=nullptr){
      string key_name = key_pointer->val_ ;
      cout<<"key_name:"<<key_name<<endl;
      primary_keys.push_back(key_name);
      key_pointer = key_pointer->next_;
    }
    CatalogManager* current_catalog=current_db->catalog_mgr_;
    IndexInfo* indexinfo=nullptr;
    string index_name = table_name + "_pk";
    cout<<"index_name:"<<index_name<<endl;
    current_catalog->CreateIndex(table_name,index_name,primary_keys,nullptr,indexinfo);
  }
  //ÎªuniqueÊôÐÔ½¨Á¢Ë÷Òý
  for (auto r = vec_col.begin() ; r != vec_col.end(); r ++ ){
    if ((*r)->IsUnique()){
      string unique_index_name = table_name + "_"+(*r)->GetName()+"_unique";
      CatalogManager* current_catalog=current_db->catalog_mgr_;
      vector <string>unique_attribute_name = {(*r)->GetName()};
      IndexInfo* indexinfo=nullptr;
      current_catalog->CreateIndex(table_name,unique_index_name,unique_attribute_name,nullptr,indexinfo);
    }
  }
  return IsCreate;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  dberr_t IsDrop=current_db->catalog_mgr_->DropTable(ast->child_->val_);
  if(IsDrop==DB_TABLE_NOT_EXIST){
    cout<<"Table Not Exist!"<<endl;
  }
  return IsDrop;
  //return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  cout<<"------Indexes------"<<endl;
  vector<TableInfo* > tables;
  current_db->catalog_mgr_->GetTables(tables);
  //ï¿½È»ï¿½ï¿½ï¿½ï¿½ï¿½ÐµÄ±ï¿½ï¿½ï¿½È»ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½Ãµï¿½Ã¿ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½
  for(auto p=tables.begin();p<tables.end();p++){
    //ï¿½ï¿½ï¿½ï¿½Ã¿ï¿½ï¿½ï¿½ï¿½
    cout<<"Indexes of Table "<<(*p)->GetTableName()<<":"<<endl;
    vector<IndexInfo*> indexes;
    current_db->catalog_mgr_->GetTableIndexes((*p)->GetTableName(),indexes);
    for(auto q=indexes.begin();q<indexes.end();q++){
      cout<<(*q)->GetIndexName()<<endl;
    }
  }
  return DB_SUCCESS;
  //return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  string table_name = ast->child_->next_->val_;
  CatalogManager* current_catalog=current_db->catalog_mgr_;
  TableInfo *tableinfo = nullptr;
  current_catalog->GetTable(table_name, tableinfo);
  //¿ÉÒÔÖªµÀÐèÒª½¨Á¢Ë÷ÒýµÄÃ¿¸ökeyµÄÃû×Ö£¬Í¨¹ýÃû×ÖÀ´ÅÐ¶ÏÕâÐ©keyÊÇ·ñunique£¬ÓÐÒ»¸ö²»ÊÇ¾Í²»ÄÜ½¨Á¢Ë÷Òý
  pSyntaxNode key_name=ast->child_->next_->next_->child_;//ÕâÊÇµÚÒ»¸öÊôÐÔ
  for(;key_name!=nullptr;key_name=key_name->next_){
    uint32_t key_index;//´æÕâ¸öÊÇµÚ¼¸¸ö
    dberr_t IsIn = tableinfo->GetSchema()->GetColumnIndex(key_name->val_,key_index);
    if (IsIn==DB_COLUMN_NAME_NOT_EXIST){
      cout<<"Attribute "<<key_name->val_<<" Isn't in The Table!"<<endl;
      return DB_FAILED;
    }
    const Column* ky=tableinfo->GetSchema()->GetColumn(key_index);
    if(ky->IsUnique()==false){
      cout<<"Can't Create Index On Non-unique Key!"<<endl;
      return DB_FAILED;
    }
  }
  vector <string> index_keys;
  //ï¿½Ãµï¿½index_keyï¿½Äµï¿½Ò»ï¿½ï¿½ï¿½ï¿½ï¿?
  pSyntaxNode index_key=ast->child_->next_->next_->child_;
  for(;index_key!=nullptr;index_key=index_key->next_){
    index_keys.push_back(index_key->val_);
  }
  IndexInfo* indexinfo=nullptr;
  string index_name = ast->child_->val_;
  dberr_t IsCreate=current_catalog->CreateIndex(table_name,index_name,index_keys,nullptr,indexinfo);
  if(IsCreate==DB_TABLE_NOT_EXIST){
    cout<<"Table Not Exist!"<<endl;
  }
  if(IsCreate==DB_INDEX_ALREADY_EXIST){
    cout<<"Index Already Exist!"<<endl;
  }

  TableHeap* tableheap = tableinfo->GetTableHeap();
  vector<uint32_t>index_column_number;
  for (auto r = index_keys.begin(); r != index_keys.end() ; r++ ){//±éÀúÊôÐÔµÄÃû×Ö
    uint32_t index ;
    tableinfo->GetSchema()->GetColumnIndex(*r,index);
    index_column_number.push_back(index);
  }
  vector<Field>fields;
  for (auto iter=tableheap->Begin(nullptr) ; iter!= tableheap->End(); iter++) {
    Row &it_row = *iter;
    vector<Field> index_fields;
    for (auto m=index_column_number.begin();m!=index_column_number.end();m++){
      index_fields.push_back(*(it_row.GetField(*m)));//µÃµ½¸Ãrow¶ÔÓ¦Ë÷ÒýÊôÐÔµÄÖµ
    }
    Row index_row(index_fields);
    indexinfo->GetIndex()->InsertEntry(index_row,it_row.GetRowId(),nullptr);
  }
  return IsCreate;
  //return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  //ï¿½ï¿½ï¿½ï¿½index_nameï¿½Ç²ï¿½Ò»ï¿½ï¿½ï¿½Ä£ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½Òµï¿½Ò»ï¿½ï¿½ï¿½ï¿½ï¿½Ö±ï¿½Ó·ï¿½ï¿½ï¿?
  //ï¿½ï¿½ï¿½ï¿½Ð¶ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½Öµï¿½Þ·ï¿½ï¿½ï¿½ï¿½ï¿½
  vector<TableInfo* > tables;
  current_db->catalog_mgr_->GetTables(tables);
  //ï¿½È»ï¿½ï¿½ï¿½ï¿½ï¿½ÐµÄ±ï¿½ï¿½ï¿½È»ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½Ãµï¿½Ã¿ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½
  for(auto p=tables.begin();p<tables.end();p++){
    //ï¿½ï¿½ï¿½ï¿½Ã¿ï¿½ï¿½ï¿½ï¿½
    //cout<<"Indexes of Table "<<(*p)->GetTableName()<<":"<<endl;
    vector<IndexInfo*> indexes;
    current_db->catalog_mgr_->GetTableIndexes((*p)->GetTableName(),indexes);
    string index_name=ast->child_->val_;
    for(auto q=indexes.begin();q<indexes.end();q++){
      //ï¿½Ð¶ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½Í?ï¿½ï¿½É¾ï¿½ï¿½
      if((*q)->GetIndexName()==index_name){
        dberr_t IsDrop=current_db->catalog_mgr_->DropIndex((*p)->GetTableName(),index_name);
        if(IsDrop==DB_TABLE_NOT_EXIST){
          cout<<"Table Not Exist!"<<endl;
        }
        if(IsDrop==DB_INDEX_NOT_FOUND){
          cout<<"Index Not Found!"<<endl;
        }
        return IsDrop;
      }
    }
  }
  //ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï»¹Ã»ï¿½ï¿½ï¿½ï¿½Ëµï¿½ï¿½Ã»ï¿½Òµï¿½ï¿½ï¿½Ã»ï¿½ï¿½É¾ï¿½ï¿½ï¿½É¹ï¿?
  cout<<"Index Not Found!"<<endl;
  return DB_FAILED;
}

vector<Row*> rec_sel(pSyntaxNode sn, std::vector<Row*>& r, TableInfo* t){
  if(sn == nullptr) return r;
  if(sn->type_ == kNodeConnector){
    
    vector<Row*> ans;
    if(strcmp(sn->val_,"and") == 0){
      auto r1 = rec_sel(sn->child_,r,t);
      ans = rec_sel(sn->child_->next_,r1,t);
      return ans;
    }
    else if(strcmp(sn->val_,"or") == 0){
      auto r1 = rec_sel(sn->child_,r,t);
      auto r2 = rec_sel(sn->child_->next_,r,t);
      for(uint32_t i=0;i<r1.size();i++){
        ans.push_back(r1[i]);        
      }
      for(uint32_t i=0;i<r2.size();i++){
        int flag=1;//Ã»ï¿½ï¿½ï¿½Ø¸ï¿½
        for(uint32_t j=0;j<r1.size();j++){
          int f=1;
          for(uint32_t k=0;k<r1[i]->GetFieldCount();k++){
            if(!r1[i]->GetField(k)->CompareEquals(*r2[j]->GetField(k))){
              f=0;break;
            }
          }
          if(f==1){
            flag=0;//ï¿½ï¿½ï¿½Ø¸ï¿½
            break;}
        }
        if(flag==1) ans.push_back(r2[i]);        
      } 
      return ans;
    }
  }
  if(sn->type_ == kNodeCompareOperator){
    string op = sn->val_;
    string col_name = sn->child_->val_;
    string val = sn->child_->next_->val_;
    uint32_t keymap;
    vector<Row*> ans;
    if(t->GetSchema()->GetColumnIndex(col_name, keymap)!=DB_SUCCESS){
      cout<<"column not found"<<endl;
      return ans;
    }
    const Column* key_col = t->GetSchema()->GetColumn(keymap);
    TypeId type =  key_col->GetType();
    if(op == "="){
      if(type==kTypeInt)
      {  
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {  
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()+2];
        strcpy(ch,val.c_str());//input compare object
        // cout<<"ch "<<sizeof(ch)<<endl;
        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();
          // cout<<"tuple len "<<sizeof(test)<<" "<<test<<endl;
          int eq=1;
          for(uint32_t q = 0;q<sizeof(test)+2;q++){
            if(test[q]!=ch[q]) eq=0;
          }
          // string ts = test;
          if(eq==1){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == "<"){
      if(type==kTypeInt)
      {  
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareLessThan(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {  
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareLessThan(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()+2];
        strcpy(ch,val.c_str());//ï¿½È½ï¿½Ä¿ï¿½ï¿½ï¿½ï¿½ï¿½chï¿½ï¿½
        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();
          // cout<<"tuple len "<<sizeof(test)<<" "<<test<<endl;
          // int eq=1;
          // for(uint32_t q = 0;q<sizeof(test)+2;q++){
          //   if(test[q]>ch[q]) {eq=0;break;}
          // }
          // string ts = test;
          // if(eq==1){
          if(strcmp(test,ch)<0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == ">"){
      if(type==kTypeInt)
      {  
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareGreaterThan(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {  
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareGreaterThan(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()];
        strcpy(ch,val.c_str());//ï¿½È½ï¿½Ä¿ï¿½ï¿½ï¿½ï¿½ï¿½chï¿½ï¿½

        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();
          
          if(strcmp(test,ch)>0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == "<="){
      if(type==kTypeInt)
      {  
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareLessThanEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {  
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareLessThanEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()];
        strcpy(ch,val.c_str());//ï¿½È½ï¿½Ä¿ï¿½ï¿½ï¿½ï¿½ï¿½chï¿½ï¿½

        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();
          
          if(strcmp(test,ch)<=0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == ">="){
      if(type==kTypeInt)
      {  
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareGreaterThanEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {  
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareGreaterThanEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()];
        strcpy(ch,val.c_str());//ï¿½È½ï¿½Ä¿ï¿½ï¿½ï¿½ï¿½ï¿½chï¿½ï¿½

        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();
        
          if(strcmp(test,ch)>=0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == "<>"){
      if(type==kTypeInt)
      {  
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareNotEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {  
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareNotEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()];
        strcpy(ch,val.c_str());//ï¿½È½ï¿½Ä¿ï¿½ï¿½ï¿½ï¿½ï¿½chï¿½ï¿½

        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();
          
          if(strcmp(test,ch)!=0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    return ans;
  }
  return r; 
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  pSyntaxNode range = ast->child_;
  vector<uint32_t> columns;
  string table_name=range->next_->val_;
  TableInfo *tableinfo = nullptr;
  dberr_t GetRet = current_db->catalog_mgr_->GetTable(table_name, tableinfo);
  if (GetRet==DB_TABLE_NOT_EXIST){
    cout<<"Table Not Exist!"<<endl;
    return DB_FAILED;
  }
  if(range->type_ == kNodeAllColumns){
    // cout<<"select all"<<endl;
    for(uint32_t i=0;i<tableinfo->GetSchema()->GetColumnCount();i++)
      columns.push_back(i);
  }
  else if(range->type_ == kNodeColumnList){
    // vector<Column*> all_columns = tableinfo->GetSchema()->GetColumns();
    pSyntaxNode col = range->child_;
    while(col!=nullptr){
      uint32_t pos;
      if(tableinfo->GetSchema()->GetColumnIndex(col->val_,pos)==DB_SUCCESS){
        columns.push_back(pos);
      }
      else{
        cout<<"column not found"<<endl;
        return DB_FAILED;
      }
      col = col->next_;
    }
  }
  for(auto i:columns){
    cout<<tableinfo->GetSchema()->GetColumn(i)->GetName()<<"   ";
  }
  cout<<endl;
  if(range->next_->next_==nullptr)//Ã»ï¿½ï¿½Ñ¡ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½
  {
    int cnt=0;
    for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
      for(uint32_t j=0;j<columns.size();j++){
        if(it->GetField(columns[j])->IsNull()){
          cout<<"null";
        }
        else
          it->GetField(columns[j])->fprint();
        cout<<"  ";
        
      }
      cout<<endl;
      cnt++;
    }
    cout<<"Select Success, Affects "<<cnt<<" Record!"<<endl;
    return DB_SUCCESS;
  }
  else if(range->next_->next_->type_ == kNodeConditions){
    pSyntaxNode cond = range->next_->next_->child_;
    vector<Row*> origin_rows;
    for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
      Row* tp = new Row(*it);
      origin_rows.push_back(tp);
    }    
    auto ptr_rows  = rec_sel(cond, *&origin_rows,tableinfo);
    
    for(auto it=ptr_rows.begin();it!=ptr_rows.end();it++){
      for(uint32_t j=0;j<columns.size();j++){
        (*it)->GetField(columns[j])->fprint();
        cout<<"  ";
      }
      cout<<endl;
    }
    cout<<"Select Success, Affects "<<ptr_rows.size()<<" Record!"<<endl;
  }
  
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  //ï¿½ï¿½ï¿½ÈµÃµï¿½ï¿½ï¿½ï¿½ï¿½
  string table_name=ast->child_->val_;
  //ï¿½Ãµï¿½tableinfo*
  //ï¿½ï¿½ï¿½ï¿½Ò»ï¿½ï¿½Row
  TableInfo *tableinfo = nullptr;
  dberr_t GetRet = current_db->catalog_mgr_->GetTable(table_name, tableinfo);
  if (GetRet==DB_TABLE_NOT_EXIST){
    cout<<"Table Not Exist!"<<endl;
    return DB_FAILED;
  }
  vector<Field> fields;
  pSyntaxNode column_pointer= ast->child_->next_->child_;//Í·Ö¸ï¿½ï¿½
  int cnt = tableinfo->GetSchema()->GetColumnCount();
  // cout<<"cnt:"<<cnt<<endl;
  for ( int i = 0 ; i < cnt ; i ++ ){
    TypeId now_type_id = tableinfo->GetSchema()->GetColumn(i)->GetType();
    if (column_pointer==nullptr){
      for ( int j = i ; j < cnt ; j ++ ){
        //cout<<"has null!"<<endl;
        Field new_field(tableinfo->GetSchema()->GetColumn(j)->GetType());
        fields.push_back(new_field);
      }
      break;
    }
    if(column_pointer->val_==nullptr ){
      //cout<<"has null!"<<endl;
      Field new_field(now_type_id);
      fields.push_back(new_field);
    }
    else{
      //cout<<"a number"<<endl;
      if (now_type_id==kTypeInt){//ï¿½ï¿½ï¿½ï¿½
        int x = atoi(column_pointer->val_);
        Field new_field (now_type_id,x);
        fields.push_back(new_field);
      }
      else if(now_type_id==kTypeFloat){//ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½
        float f = atof(column_pointer->val_);
        Field new_field (now_type_id,f);
        fields.push_back(new_field);
      }
      else {//ï¿½Ö·ï¿½ï¿½ï¿½
        string s = column_pointer->val_;
        Field new_field (now_type_id,column_pointer->val_,s.length(),true); 
        fields.push_back(new_field);
      }
    }
    column_pointer = column_pointer->next_;
  }
  if (column_pointer!=nullptr){
    cout<<"Column Count doesn't match!"<<endl;
    return DB_FAILED;
  }
  Row row(fields);//ï¿½ï¿½ï¿½ï¿½Ò»ï¿½ï¿½row
  //ï¿½Â¹ï¿½ï¿½ï¿½ï¿½ï¿½Rowï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½Îªrowï¿½ï¿½RowIDÎªrowid
  //ï¿½Ãµï¿½ï¿½ï¿½ï¿½Ðµï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½
  ASSERT(tableinfo!=nullptr,"TableInfo is Null!");
  TableHeap* tableheap=tableinfo->GetTableHeap();//ï¿½Ãµï¿½ï¿½ï¿½ï¿½ï¿½Ó¦ï¿½ï¿½ï¿½Ä¼ï¿½ï¿½ï¿½
  bool Is_Insert=tableheap->InsertTuple(row,nullptr);
  if(Is_Insert==false){
    cout<<"Insert Failed, Affects 0 Record!"<<endl;
    return DB_FAILED;
  }else{
    vector <IndexInfo*> indexes;//ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½indexinfo
    current_db->catalog_mgr_->GetTableIndexes(table_name,indexes);

    for(auto p=indexes.begin();p<indexes.end();p++){
      IndexSchema* index_schema = (*p)->GetIndexKeySchema();
      vector<Field> index_fields;
      for(auto it:index_schema->GetColumns()){
        index_id_t tmp;
        if(tableinfo->GetSchema()->GetColumnIndex(it->GetName(),tmp)==DB_SUCCESS){
          index_fields.push_back(fields[tmp]);
        }
      }
      Row index_row(index_fields);
      dberr_t IsInsert=(*p)->GetIndex()->InsertEntry(index_row,row.GetRowId(),nullptr);
      //cout<<"RowID: "<<row.GetRowId().Get()<<endl;
      if(IsInsert==DB_FAILED){
        //ï¿½ï¿½ï¿½ï¿½Ê§ï¿½ï¿½
        cout<<"Insert Into Index Failed, Affects 0 Record!"<<endl;
        //ï¿½ï¿½ï¿½ï¿½Ê§ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½Òªï¿½ï¿½ï¿½ï¿½Ö®Ç°ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½Ð²ï¿½ï¿½ï¿½Ä¼ï¿½Â¼ï¿½ï¿½ï¿½ï¿½ï¿½Ò´Ó±ï¿½ï¿½ï¿½É¾ï¿½ï¿?
        for(auto q=indexes.begin();q!=p;q++){
          (*q)->GetIndex()->RemoveEntry(row,row.GetRowId(),nullptr);
        }
        tableheap->MarkDelete(row.GetRowId(),nullptr);
        return IsInsert;
      }else{
        cout<<"Insert Into Index Sccess"<<endl;
      }
    }
    //È«ï¿½ï¿½ï¿½ï¿½ï¿½Ü²ï¿½ï¿½È¥ï¿½ï¿½ï¿½Ü³É¹ï¿½ï¿½ï¿½ï¿½ï¿?
    cout<<"Insert Success, Affects 1 Record!"<<endl;
    return DB_SUCCESS;
  }
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  string table_name=ast->child_->val_;
  TableInfo *tableinfo = nullptr;
  dberr_t GetRet = current_db->catalog_mgr_->GetTable(table_name, tableinfo);
  if (GetRet==DB_TABLE_NOT_EXIST){
    cout<<"Table Not Exist!"<<endl;
    return DB_FAILED;
  }
  TableHeap* tableheap=tableinfo->GetTableHeap();//ï¿½Ãµï¿½ï¿½ï¿½ï¿½ï¿½Ó¦ï¿½ï¿½ï¿½Ä¼ï¿½ï¿½ï¿½
  auto del = ast->child_;
  vector<Row*> tar;

  if(del->next_==nullptr){//ï¿½ï¿½È¡ï¿½ï¿½ï¿½ï¿½Ñ¡ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½rowï¿½ï¿½ï¿½ï¿½ï¿½ï¿½vector<Row*> tarï¿½ï¿½
    for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
      Row* tp = new Row(*it);
      tar.push_back(tp);
    }  
  }
  else{
    vector<Row*> origin_rows;
    for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
      Row* tp = new Row(*it);
      origin_rows.push_back(tp);
    }
    tar  = rec_sel(del->next_->child_, *&origin_rows,tableinfo); 
  }
  for(auto it:tar){
    tableheap->ApplyDelete(it->GetRowId(),nullptr);
  }
  cout<<"Delete Success, Affects "<<tar.size()<<" Record!"<<endl;
  vector <IndexInfo*> indexes;//ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½indexinfo
  current_db->catalog_mgr_->GetTableIndexes(table_name,indexes);
  for(auto p=indexes.begin();p<indexes.end();p++){
    for(auto j:tar){
      vector<Field> index_fields;
      for(auto it:(*p)->GetIndexKeySchema()->GetColumns()){
        index_id_t tmp;
        if(tableinfo->GetSchema()->GetColumnIndex(it->GetName(),tmp)==DB_SUCCESS){
          index_fields.push_back(*j->GetField(tmp));
        }
      }
      Row index_row(index_fields);
      (*p)->GetIndex()->RemoveEntry(index_row,j->GetRowId(),nullptr);
    }
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  string table_name=ast->child_->val_;
  TableInfo *tableinfo = nullptr;
  dberr_t GetRet = current_db->catalog_mgr_->GetTable(table_name, tableinfo);
  if (GetRet==DB_TABLE_NOT_EXIST){
    cout<<"Table Not Exist!"<<endl;
    return DB_FAILED;
  }
  TableHeap* tableheap=tableinfo->GetTableHeap();//ï¿½Ãµï¿½ï¿½ï¿½ï¿½ï¿½Ó¦ï¿½ï¿½ï¿½Ä¼ï¿½ï¿½ï¿½
  auto updates = ast->child_->next_;
  vector<Row*> tar;

  if(updates->next_==nullptr){//ï¿½ï¿½È¡ï¿½ï¿½ï¿½ï¿½Ñ¡ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½rowï¿½ï¿½ï¿½ï¿½ï¿½ï¿½vector<Row*> tarï¿½ï¿½
    for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
      Row* tp = new Row(*it);
      tar.push_back(tp);
    }
    // cout<<"---- all "<<tar.size()<<" ----"<<endl;    
  }
  else{
    vector<Row*> origin_rows;
    for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
      Row* tp = new Row(*it);
      origin_rows.push_back(tp);
    }
    tar  = rec_sel(updates->next_->child_, *&origin_rows,tableinfo);
    // cout<<"---- part "<<tar.size()<<" ----"<<endl;   
  }
  updates = updates->child_;
  while(updates && updates->type_ == kNodeUpdateValue){//Ö±ï¿½ï¿½ï¿½Õ½ï¿½ï¿?
    string col = updates->child_->val_;
    string upval = updates->child_->next_->val_;
    uint32_t index;//ï¿½Òµï¿½colï¿½ï¿½Ó¦ï¿½ï¿½index
    tableinfo->GetSchema()->GetColumnIndex(col,index);
    TypeId tid = tableinfo->GetSchema()->GetColumn(index)->GetType();
    if(tid == kTypeInt){
      Field* newval = new Field(kTypeInt,stoi(upval));
      for(auto it:tar){
        it->GetFields()[index] = newval;
      }
    }
    else if(tid == kTypeFloat){
      Field* newval = new Field(kTypeFloat,stof(upval));
      for(auto it:tar){
        it->GetFields()[index] = newval;
      }
    }
    else if(tid == kTypeChar){
      uint32_t len = tableinfo->GetSchema()->GetColumn(index)->GetLength();
      char* tc = new char[len];
      strcpy(tc,upval.c_str());
      Field* newval = new Field(kTypeChar,tc,len,true);
      for(auto it:tar){
        it->GetFields()[index] = newval;
      }
    }
    updates = updates->next_;
  }
  for(auto it:tar){
    tableheap->UpdateTuple(*it,it->GetRowId(),nullptr);
  }
  cout<<"Update Success, Affects "<<tar.size()<<" Record!"<<endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}
#include<fstream>
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  string name = ast->child_->val_;
  string file_name = "/mnt/e/---xixi---/sql_gen/"+name;
  //cout<<file_name;
  ifstream infile;
  infile.open(file_name.data());//Connect a file stream object to a file
  if (infile.is_open()){ //if open fails,return false
    string s;
    while(getline(infile,s)){//read line by line
      YY_BUFFER_STATE bp = yy_scan_string(s.c_str());
      if (bp == nullptr) {
        LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
        exit(1);
      }
      yy_switch_to_buffer(bp);
      // init parser module
      MinisqlParserInit();
      // parse
      yyparse();
      ExecuteContext context;
      Execute(MinisqlGetParserRootNode(), &context);
    }
    return DB_SUCCESS;
  }
  else{
    cout<<"Failed In Opening File!"<<endl;
    return DB_FAILED;
  }
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}
