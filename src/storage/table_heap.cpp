#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  int record_len = row.GetSerializedSize(schema_); //�õ�row���л��ĳ���
  if(record_len>SIZE_MAX_ROW){
    //��������£�ֻ��һ����¼���ļ�ͷ+�ü�¼ƫ����+��¼����+��¼����Ҳ�Ų���
    return false;
  }
  if (first_page_id_==INVALID_PAGE_ID){
    TablePage *First_Page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(first_page_id_));
    if (First_Page==nullptr) return false;
    buffer_pool_manager_->UnpinPage(first_page_id_,true);//����Ϊ��ҳ
    First_Page->SetPrevPageId(INVALID_PAGE_ID);
    First_Page->SetNextPageId(INVALID_PAGE_ID);
    First_Page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);
  }
  else{
    //�ѵ�һ��page������
    TablePage *NowPage = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
    bool flag = false;//flag=true˵����Ҫ�½�ҳ
    while(1){//ѭ���ҵ�������ܷŵ�page
      if(NowPage->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)){
        buffer_pool_manager_->UnpinPage(NowPage->GetPageId(),true);//����Ϊ��ҳ
        return true;//�ɹ�����
      }
      page_id_t NextPageId = NowPage->GetNextPageId();
      if (NextPageId == INVALID_PAGE_ID){
        flag=true;//˵���ҵ����Ҳû����ɹ�
        break;
      }
      NowPage = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(NextPageId));
    }
    if (flag){
      int new_page_id = INVALID_PAGE_ID;
      buffer_pool_manager_->UnpinPage(NowPage->GetPageId(),true);//����Ϊ��ҳ
      TablePage *New_Page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
      if (New_Page==nullptr) return false;
      //���˫������
      buffer_pool_manager_->UnpinPage(New_Page->GetPageId(),true);//����Ϊ��ҳ
      New_Page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);
      NowPage->SetNextPageId(new_page_id);
      New_Page->SetPrevPageId(NowPage->GetPageId());
      New_Page->SetNextPageId(INVALID_PAGE_ID);
    }        
  }
  return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
  //���ҵ��ɵļ�¼����ҳ
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if(page==nullptr){//�Ҳ��������¼
    return false;
  }
  //�ҵĵ�
  Row old_row(rid);  //��ԭ����row������,����rollback
  bool Isupdate;//�����Ӻ������
  Isupdate=page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  if(Isupdate){//�ɹ�����
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;
  }else{//����ʧ��
    uint32_t serialized_size = row.GetSerializedSize(schema_);
    ASSERT(serialized_size > 0, "Can not have empty row.");
    uint32_t slot_num = old_row.GetRowId().GetSlotNum();
    uint32_t tuple_size = page->GetTupleSize(slot_num);
    if(slot_num>=page->GetTupleCount() || TablePage::IsDeleted(tuple_size)){
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
      return false;
    }else{//��Ϊ�ռ䲻�㵼�²���ʧ��
      //page_id_t next_page_id=page->GetNextPageId();
      //auto next_page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
      bool isInsert;
      isInsert=InsertTuple(row,txn);//�ж���û�гɹ�����
      //��ǰ�޷��������ֱ�ӵ��ò��뺯���嵽��ĵط�����ͬʱrid�ᱻ�޸ģ����Ը����˲�������
      if(isInsert)
        MarkDelete(rid,txn);//���ɼ�¼�����Ϊɾ��
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), isInsert);
      return isInsert;
    }
  }

  //return false;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page!=nullptr);
  page->ApplyDelete(rid,txn,log_manager_);
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(),true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {
  if (first_page_id_==INVALID_PAGE_ID){
    return;
  }
  else{
    //�ѵ�һ��page������
    TablePage *NowPage = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
    while(1){//ѭ���ҵ�������ܷŵ�page
      buffer_pool_manager_->DeletePage(NowPage->GetPageId());
      page_id_t NextPageId = NowPage->GetNextPageId();
      if (NextPageId == INVALID_PAGE_ID){
        break;
      }
      NowPage = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(NextPageId));
    }
  }
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if(page==nullptr){
    return false;
  }else{
    bool isGet;
    isGet=page->GetTuple(row,schema_,txn,lock_manager_);
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return isGet;
  }
  //return false;
}

TableIterator TableHeap::Begin(Transaction *txn) {
  //�ҵ���һ����¼������һ������������row����
  return TableIterator();
}

TableIterator TableHeap::End() {
  return TableIterator();
}
