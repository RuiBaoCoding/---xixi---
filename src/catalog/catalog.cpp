#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  char* buffer = buf;
  MACH_WRITE_TO(uint32_t, buffer, CATALOG_METADATA_MAGIC_NUM);
  buffer+=sizeof(uint32_t);
  uint32_t len = table_meta_pages_.size()-1;
  // cout<<len<<endl;
  MACH_WRITE_TO(uint32_t, buffer, len);
  buffer+=sizeof(uint32_t);
  auto it1 = table_meta_pages_.begin();
  for(int i=0;i<int(len);i++){
    uint32_t tid = it1->first;
    int32_t pid = it1->second;
    MACH_WRITE_TO(uint32_t, buffer, tid);
    buffer+=sizeof(uint32_t);
    MACH_WRITE_TO(int32_t, buffer, pid);
    buffer+=sizeof(int32_t);
    it1++;
  }
  len = index_meta_pages_.size()-1;
  // cout<<len<<endl;
  MACH_WRITE_TO(uint32_t, buffer, len);
  buffer+=sizeof(uint32_t);
  auto it2 = index_meta_pages_.begin();
  for(int i=0;i<int(len);i++){
    uint32_t iid = it2->first;
    int32_t pid = it2->second;
    MACH_WRITE_TO(uint32_t, buffer, iid);
    buffer+=sizeof(uint32_t);
    MACH_WRITE_TO(int32_t, buffer, pid);
    buffer+=sizeof(int32_t);
    it2++;
  }
  // ASSERT(false, "Not Implemented yet");
  // cout<<buffer-buf<<endl;
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  // ASSERT(false, "Not Implemented yet");
  char* buffer = buf;
  CatalogMeta* ret = new(heap->Allocate(sizeof(CatalogMeta)))CatalogMeta;
  [[maybe_unused]]uint32_t val = MACH_READ_FROM(uint32_t, buffer); buffer+=4;
  // cout<<val<<endl;//输出魔数
  uint32_t len = MACH_READ_FROM(uint32_t, buffer); buffer+=4;
  for(int i=0;i<int(len);i++){
    uint32_t tid = MACH_READ_FROM(uint32_t, buffer); buffer+=4;
    int32_t pid = MACH_READ_FROM(int32_t, buffer); buffer+=4;
    ret->table_meta_pages_[tid] = pid;
  }
  ret->GetTableMetaPages()->emplace(len, INVALID_PAGE_ID);
  len = MACH_READ_FROM(uint32_t, buffer); buffer+=4;
  for(int i=0;i<int(len);i++){
    uint32_t iid = MACH_READ_FROM(uint32_t, buffer); buffer+=4;
    int32_t pid = MACH_READ_FROM(int32_t, buffer); buffer+=4;
    ret->index_meta_pages_[iid] = pid;
  }
  ret->GetIndexMetaPages()->emplace(len, INVALID_PAGE_ID);
  return ret;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  // ASSERT(false, "Not Implemented yet");
  uint32_t s = table_meta_pages_.size()+index_meta_pages_.size()-2;
  s = s*8+4+8;
  return s;
}

CatalogMeta::CatalogMeta() {}


CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
        : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
          log_manager_(log_manager), heap_(new SimpleMemHeap()) {
  // ASSERT(false, "Not Implemented yet");
  if(init == false) 
  {
    Page* p = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    char* t = p->GetData();
    while(*t!='\0'){
      TableMetadata* meta;
      t+=meta->DeserializeFrom(t, meta, heap_);
      TableInfo* tinfo;

      if(table_names_.count(meta->GetTableName()) != 0) continue;
      table_names_[meta->GetTableName()] = meta->GetTableId();

      tinfo = TableInfo::Create(heap_);
      TableMetadata *table_meta = TableMetadata::Create(meta->GetTableId(), meta->GetTableName(), 0, meta->GetSchema(), heap_);
      TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_,meta->GetSchema(), nullptr, log_manager_, lock_manager_, heap_);
      tinfo->Init(table_meta, table_heap);
      tables_[meta->GetTableId()] = tinfo;
      index_names_.insert({meta->GetTableName(), std::unordered_map<std::string, index_id_t>()});

      // CreateTable(meta->GetTableName(),meta->GetSchema(),nullptr, tinfo);
    }
    // cout<<"catalog load done"<<endl;
    Page* p2 = buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID);
    char* t2 = p2->GetData();
    // cout<<"fetch page done"<<endl;
    while(*t2!='\0'){
      IndexMetadata* meta;
      t2+=IndexMetadata::DeserializeFrom(t2, meta, heap_);
      TableInfo* tinfo = tables_[meta->GetTableId()];
      // cout<<meta->GetTableId()<<" "<<meta->GetIndexName()<<endl;
      // cout<<tinfo->GetTableName()<<" "<<tinfo->GetTableId()<<endl;
      IndexInfo* indinfo = IndexInfo::Create(heap_);
      indinfo->Init(meta,tinfo,buffer_pool_manager_);//segmentation
      // cout<<"p1"<<endl;
      index_names_[tinfo->GetTableName()][meta->GetIndexName()] = meta->GetIndexId();
      indexes_[meta->GetIndexId()] = indinfo;
    }
    buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID, false);
    buffer_pool_manager->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
  }
}

CatalogManager::~CatalogManager() {
  delete heap_;
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
    if(table_names_.count(table_name) != 0)
      return DB_TABLE_ALREADY_EXIST;
    table_id_t table_id = next_table_id_++;
    table_names_[table_name] = table_id;

    table_info = TableInfo::Create(heap_);
    TableMetadata *table_meta = TableMetadata::Create(table_id, table_name, 0, schema, heap_);
    TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_,schema, txn, log_manager_, lock_manager_, heap_);
    table_info->Init(table_meta, table_heap);
    tables_[table_id] = table_info;
    index_names_.insert({table_name, std::unordered_map<std::string, index_id_t>()});
    ///将table的信息写入metapage
    auto len = table_meta->GetSerializedSize();
    char meta[len+1];
    table_meta->SerializeTo(meta);
    char* p = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData();
    memcpy(p+len,p,PAGE_SIZE-len);
    memcpy(p,meta,len);

    if(table_meta!=nullptr && table_heap!=nullptr)
      return DB_SUCCESS;
    return DB_FAILED;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  if(table_names_.count(table_name) <= 0)
    return DB_TABLE_NOT_EXIST;
  table_id_t table_id = table_names_[table_name];
  table_info = tables_[table_id];
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  for(auto it=tables_.begin();it!=tables_.end();it++){
    tables.push_back(it->second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  // ASSERT(false, "Not Implemented yet");
    if(index_names_.count(table_name)<=0) return DB_TABLE_NOT_EXIST;
    auto it = index_names_[table_name];
    if(it.count(index_name)>0) return DB_INDEX_ALREADY_EXIST;

    index_id_t index_id = next_index_id_++;
    std::vector<uint32_t> key_map;
    TableInfo* tinfo;
    dberr_t error = GetTable(table_name,tinfo);
    if(error) return error;
    for(int j=0;j<int(index_keys.size());j++){
      auto i = index_keys[j];
      uint32_t tkey;
      dberr_t err = tinfo->GetSchema()->GetColumnIndex(i, tkey);
      if(err) return err;
      key_map.push_back(tkey);
    }
    IndexMetadata *index_meta_data_ptr = IndexMetadata::Create(index_id, index_name, table_names_[table_name],key_map,heap_ );
    // cout<<"p0"<<endl;
    index_info = IndexInfo::Create(heap_);
    index_info->Init(index_meta_data_ptr,tinfo,buffer_pool_manager_);
    // cout<<"p1"<<endl;
    index_names_[table_name][index_name] = index_id;
    indexes_[index_id] = index_info;
    // cout<<"p2"<<endl;
    ///将index的信息写入index_roots_page
    auto len = index_meta_data_ptr->GetSerializedSize();
    char meta[len+1];
    index_meta_data_ptr->SerializeTo(meta);
    char* p = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData();
    memcpy(p+len,p,PAGE_SIZE-len);
    memcpy(p,meta,len);
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  auto search_table = index_names_.find(table_name);
  if (search_table == index_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  auto index_table = search_table->second;
  auto search_index_id = index_table.find(index_name);
  if (search_index_id == index_table.end()){
    return DB_INDEX_NOT_FOUND;
  }
  index_id_t index_id = search_index_id->second;
  auto it = indexes_.find(index_id);
  if(it == indexes_.end()) return DB_INDEX_NOT_FOUND;
  index_info = it->second;
  return DB_SUCCESS;
                                 }
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  auto table_indexes = index_names_.find(table_name);
  if(table_indexes == index_names_.end())
    return DB_TABLE_NOT_EXIST;
  auto indexes_map = table_indexes->second;
  for(auto it:indexes_map){
    indexes.push_back(indexes_.find(it.second)->second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  auto it = table_names_.find(table_name);
  if(it == table_names_.end()) return DB_TABLE_NOT_EXIST;
  table_id_t tid = it->second;
  // delete tables_[tid];
  auto it2 = index_names_.find(table_name);
  for(auto it3 : it2->second){
    index_id_t tmp = it3.second;
    indexes_.erase(tmp);
  }
  index_names_.erase(table_name);
  tables_.erase(tid);
  table_names_.erase(table_name);
  // ASSERT(false, "Not Implemented yet");
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  auto it = index_names_.find(table_name);
  if(it == index_names_.end()) return DB_TABLE_NOT_EXIST;
  auto it2 = it->second.find(index_name);
  if(it2 == it->second.end()) return DB_INDEX_NOT_FOUND;
  
  index_id_t index_id = it2->second;
  // IndexInfo* iinfo = indexes_[index_id];
  // delete iinfo;//因为iinfo中所有成员变量空间都是有heap_分配的，所以删除heap_即可
  it->second.erase(index_name);
  indexes_.erase(index_id);
  return DB_SUCCESS;
}


dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  auto it = tables_.find(table_id);
  if(it==tables_.end()) return DB_TABLE_NOT_EXIST;
  table_info = it->second;
  return DB_SUCCESS;
}