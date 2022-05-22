#ifndef MINISQL_B_PLUS_TREE_LEAF_PAGE_H
#define MINISQL_B_PLUS_TREE_LEAF_PAGE_H

/**
 * b_plus_tree_leaf_page.h
 *
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.

 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 24 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) | ParentPageId (4) |
 *  ---------------------------------------------------------------------
 *  ------------------------------
 * | PageId (4) | NextPageId (4)
 *  ------------------------------
 */
#include <utility>
#include <vector>

#include "page/b_plus_tree_page.h"

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define LEAF_PAGE_HEADER_SIZE 28
#define LEAF_PAGE_SIZE (((PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType)) - 1)

INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
public:
  // After creating a new leaf page from buffer pool, must call initialize
  // method to set default values
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = LEAF_PAGE_SIZE);

  // helper methods
  //���ظ�Ҷ����page_id_
  page_id_t GetNextPageId() const;
  
  //set��Ҷ����next_page_id_
  void SetNextPageId(page_id_t next_page_id);

  //��index����key
  KeyType KeyAt(int index) const;

  //�ҵ���һ��>=key��λ�ã�index��
  //ֻ��generating index iteratorʱ��
  int KeyIndex(const KeyType &key, const KeyComparator &comparator) const;

  //��index�����أ�key��value��
  const MappingType &GetItem(int index);

  // insert and delete methods
  //��Ҷ����в��루key,value)������size
  int Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator);

  //��Ҷ����в��ң�key��value�����ҵ�����true���Ҳ�������false
  bool Lookup(const KeyType &key, ValueType &value, const KeyComparator &comparator) const;

  //ɾ��һ��key������size������Ҳ����Ͳ������������ص�ǰsize��
  int RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator);

  // Split and Merge utility methods
  void MoveHalfTo(BPlusTreeLeafPage *recipient);

  void MoveAllTo(BPlusTreeLeafPage *recipient);

  void MoveFirstToEndOf(BPlusTreeLeafPage *recipient);

  void MoveLastToFrontOf(BPlusTreeLeafPage *recipient);

  void MoveAllToFrontOf(BPlusTreeLeafPage *recipient);

  page_id_t next_page_id_;
  MappingType array_[0];
private:
  void CopyNFrom(MappingType *items, int size);

  void CopyLastFrom(const MappingType &item);

  void CopyFirstFrom(const MappingType &item);

  
  
};

#endif  // MINISQL_B_PLUS_TREE_LEAF_PAGE_H
