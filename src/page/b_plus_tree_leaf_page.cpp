#include <algorithm>
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_leaf_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetNextPageId(INVALID_PAGE_ID);
  //SetNextPageId(-1);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  int s = GetSize();
  //std::cout<<"s: "<<s<<std::endl;
  //if(s==1) return 0;
  //比第一个小就在第0个结点
  int i = 0;
  for ( ; i < s ; i ++ ){
    if (comparator(array_[i].first,key)>=0){
      break;
    }
  }
  return i;
  /*int left = 0, right = s;//二分法搜索key,实际范围为0到(s-1)
  while(left<right){
    int mid = (left+right)/2;
    int t = comparator(key, array_[mid].first);
    if(t<=0){
      right = mid;
    }else{
      left=mid+1;
    }
    //cout<<"left: "<<left<<endl;
    //cout<<"right: "<<right<<endl;
  }
  return right;*/
  //在叶结点中查找key，返回index（如果找不着就返回GetSize()）
  //return array_[left].second;
  
  /*for(int i=1;i<GetSize();i++){
    if(comparator(array_[i].first,key)>=0) return i;
  }
  return GetSize();*///若不存在比key大的值，返回size_
}
/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  KeyType key = array_[index].first;
  return key;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  return array_[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  if(GetSize()==0){
    cout<<"size=0"<<endl;
    array_[0].first = key;
    array_[0].second = value;
    IncreaseSize(1);
    return 1;
  }
  //查找第一个大于等于key的值
  int pos = KeyIndex(key, comparator);
  std::cout<<"pos: "<<pos<<std::endl;
  if(pos!=GetSize() && comparator(key, array_[pos].first)==0) return GetSize();
  for(int i=GetSize();i>pos;i--){
    array_[i] = array_[i-1];
  }
  array_[pos].first = key;
  array_[pos].second = value;
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  int s = GetSize();
  recipient->CopyNFrom(array_+s-int(s/2),int(s/2));
  IncreaseSize(-int(s/2));
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  for(int i=0;i<size;i++){
    CopyLastFrom(items[i]);
  }
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value, const KeyComparator &comparator) const {
  int pos = KeyIndex(key, comparator);
  if(pos!=GetSize() && comparator(array_[pos].first, key)==0){
    value = array_[pos].second;
    return true;
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  int pos = KeyIndex(key, comparator);
  if(pos==GetSize() || comparator(key, array_[pos].first)!=0) return GetSize();
  for(int i=pos;i<GetSize()-1;i++) array_[i] = array_[i+1];
  IncreaseSize(-1);//Remove之后size--
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  recipient->SetNextPageId(GetNextPageId());
  recipient->CopyNFrom(array_,GetSize());
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  recipient->CopyLastFrom(array_[0]);
  for(int i=0;i<GetSize()-1;i++){
    array_[i] = array_[i+1];
  }
  IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  array_[GetSize()].first = item.first;
  array_[GetSize()].second = item.second;
  IncreaseSize(1);

}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  ASSERT(recipient!=nullptr,"recipient is null!");
  cout<<"In MoveLtoFront."<<endl;
  recipient->CopyFirstFrom(array_[GetSize()-1]);
  IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  cout<<"In CopyFirstFrom."<<endl;
  for(int i = GetSize();i>0;i--){array_[i] = array_[i-1];}
  array_[0].first = item.first;
  array_[0].second = item.second;
  IncreaseSize(1);
}

template
class BPlusTreeLeafPage<int, int, BasicComparator<int>>;

template
class BPlusTreeLeafPage<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTreeLeafPage<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTreeLeafPage<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTreeLeafPage<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTreeLeafPage<GenericKey<64>, RowId, GenericComparator<64>>;